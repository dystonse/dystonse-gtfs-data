#![recursion_limit="1024"]

mod importer;
mod analyser;
mod predictor;
mod types;

#[cfg(feature = "monitor")]
mod monitor;

use std::error::Error;
#[macro_use]
extern crate lazy_static;

use clap::{App, Arg, ArgMatches};
use mysql::*;
use retry::delay::Fibonacci;
use retry::retry;
use simple_error::{SimpleError, bail};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Duration};
use regex::Regex;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use importer::Importer;
use analyser::Analyser;
use predictor::Predictor;

#[cfg(feature = "monitor")]
use monitor::Monitor;

use gtfs_structures::Gtfs;
use types::DelayStatistics;

use std::fmt::Debug;

// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

pub struct Main {
    verbose: bool,
    pool: Arc<Pool>,
    args: ArgMatches,
    source: String,
    dir: String,
    //file caches using Mutexes so main doesn't have to be mutable:
    gtfs_cache: Mutex<FileCache<Gtfs>>,
    statistics_cache: Mutex<FileCache<DelayStatistics>>,
}

fn main() -> FnResult<()> {
    let mut instance = Main::new()?;
    instance.run()?;
    Ok(())
}


trait OrError<T> {
    fn or_error(self, message: &str) -> FnResult<T>;
}

impl<T> OrError<T> for Option<T> {
    fn or_error(self, message: &str) -> FnResult<T> {
        if self.is_none() {
            bail!(message);
        }
        Ok(self.unwrap())
    }
}

impl<T, E> OrError<T> for std::result::Result<T, E>
where E: Debug
{
    fn or_error(self, message: &str) -> FnResult<T> {
        match self {
            Err(e) => bail!(format!("{}\nInner error message: {:?}", message, e)),
            Ok(t) => Ok(t)
        }
    }
}

/// Reads contents of the given directory and returns an alphabetically sorted list of included files / subdirectories as Vector of Strings.
pub fn read_dir_simple(path: &str) -> FnResult<Vec<String>> {
    let mut path_list: Vec<String> = fs::read_dir(path)?
        .filter_map(|r| r.ok()) // unwraps Options and ignores any None values
        .map(|d| {
            String::from(d.path().to_str().expect(&format!(
                "Found file with invalid UTF8 in file name in directory {}.",
                &path
            )))
        })
        .collect();
    path_list.sort();
    Ok(path_list)
}

pub fn date_from_filename(filename: &str) -> FnResult<NaiveDate> {
    lazy_static! {
        static ref FIND_DATE: Regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap(); // can't fail because our hard-coded regex is known to be ok
    }
    let date_element_captures =
        FIND_DATE
            .captures(&filename)
            .or_error(&format!(
            "File name does not contain a valid date (does not match format YYYY-MM-DD): {}",
            filename
        ))?;
    let date_option = NaiveDate::from_ymd_opt(
        date_element_captures[1].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
        date_element_captures[2].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
        date_element_captures[3].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
    );
    Ok (date_option.ok_or(SimpleError::new(format!("File name does not contain a valid date (format looks ok, but values are out of bounds): {}", filename)))?)
}

fn parse_args() -> ArgMatches {
    #[allow(unused_mut)]
    let mut app = App::new("dystonse-gtfs-data")
        .subcommand(Importer::get_subcommand())
        .subcommand(Analyser::get_subcommand())
        .subcommand(Predictor::get_subcommand())            
        .arg(Arg::new("verbose")
            .short('v')
            .long("verbose")
            .about("Output status messages during run.")
        ).arg(Arg::new("password")
            .short('p')
            .long("password")
            .env("DB_PASSWORD")
            .takes_value(true)
            .about("Password used to connect to the database.")
            .required_unless("help")
        ).arg(Arg::new("user")
            .short('u')
            .long("user")
            .env("DB_USER")
            .takes_value(true)
            .about("User on the database.")
            .default_value("dystonse")
        ).arg(Arg::new("host")
            .long("host")
            .env("DB_HOST")
            .takes_value(true)
            .about("Host on which the database can be connected.")
            .default_value("localhost")   
        ).arg(Arg::new("port")
            .long("port")
            .env("DB_PORT")
            .takes_value(true)
            .about("Port on which the database can be connected.")
            .default_value("3306")
        ).arg(Arg::new("database")
            .short('d')
            .long("database")
            .env("DB_DATABASE")
            .takes_value(true)
            .about("Database name which will be selected.")
            .default_value("dystonse")
        ).arg(Arg::new("source")
            .short('s')
            .long("source")
            .env("GTFS_DATA_SOURCE_ID")
            .takes_value(true)
            .about("Source identifier for the data sets. Used to distinguish data sets with non-unique ids.")
            .required_unless("help")
        ).arg(Arg::new("dir")
            .long("dir")
            .value_name("DIRECTORY")
            .required_unless("help")
            .about("The directory which contains schedules, realtime files, and precomputed curves")
            .long_about(
                "The directory that contains the schedules, realtime files, (located in a subdirectory named 'schedules' or 'rt') \
                and precomputed curve data."
            )
        ).arg(Arg::new("schedule")
            .long("schedule")
            .about("The path of the GTFS schedule that is used to look up any static GTFS data.")
            .takes_value(true)
            .value_name("GTFS_SCHEDULE")
        );

        #[cfg(feature = "monitor")]
        {
            app = app.subcommand(Monitor::get_subcommand());
        } 

        let matches = app.get_matches();
    return matches;
}

impl Main {
    /// Constructs a new instance of Main, with parsed arguments and a ready-to-use pool of database connections.
    fn new() -> FnResult<Main> {
        let args = parse_args();
        let verbose = args.is_present("verbose");
        let source = String::from(args.value_of("source").unwrap()); // already validated by clap
        let dir = String::from(args.value_of("dir").unwrap()); // already validated by clap

        if verbose {
            println!("Connecting to database…");
        }
        let pool = retry(Fibonacci::from_millis(1000), || {
            Main::open_db(&args, verbose)
        })
        .expect("DB connections should succeed eventually.");
        Ok(Main {
            args,
            verbose,
            pool: Arc::new(pool),
            source,
            dir,
            gtfs_cache: Mutex::new(FileCache::<Gtfs>::new()),
            statistics_cache: Mutex::new(FileCache::<DelayStatistics>::new()),
        })
    }

    /// Runs the actions that are selected via the command line args
    fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("import", Some(sub_args)) => {
                let mut importer = Importer::new(&self, sub_args);
                importer.run()
            },
            ("analyse", Some(sub_args)) => {
                let mut analyser = Analyser::new(&self, sub_args);
                analyser.run()
            },
            ("predict", Some(sub_args)) => {
                let mut predictor = Predictor::new(&self, sub_args)?;
                predictor.run()
            },
            #[cfg(feature = "monitor")]
            ("monitor", Some(sub_args)) => {
                Monitor::run(&self, sub_args)
            },
            _ => panic!("Invalid arguments."),
        }
    }

    /// Opens a connection to a database and returns the resulting connection pool.
    /// Takes configuration values from DB_PASSWORD, DB_USER, DB_HOST, DB_PORT and DB_DATABASE
    /// environment variables. For all values except DB_PASSWORD a default is provided.
    fn open_db(args: &ArgMatches, verbose: bool) -> FnResult<Pool> {
        if verbose {
            println!("Trying to connect to the database.");
        }
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            args.value_of("user").unwrap(), // already validated by clap
            args.value_of("password").unwrap(), // already validated by clap
            args.value_of("host").unwrap(), // already validated by clap
            args.value_of("port").unwrap(), // already validated by clap
            args.value_of("database").unwrap()  // already validated by clap
        );
        let pool = Pool::new(url)?;
        Ok(pool)
    }

    // returns the schedule (from args or auto-lookup)
    pub fn get_schedule(&self) -> FnResult<Arc<Gtfs>> {
        let filename = Self::get_schedule_filename(&self.args)?;
        FileCache::get_cached_simple(&self.gtfs_cache, &filename)
    }

    fn get_schedule_filename(args: &ArgMatches) -> FnResult<String> {
        // find out if schedule arg is given:
        let schedule_filename : String = 
        if let Some(filename) = args.value_of("schedule") {
            filename.to_string()
        } else {
            // if the arg is not given, look up the newest schedule file:
            println!("No schedule file name given, looking up the most recent schedule file…");
            let dir = args.value_of("dir").unwrap(); // already validated by clap
            let schedule_dir = format!("{}/schedule", dir);
            let schedule_filenames = read_dir_simple(&schedule_dir)?; //list of all schedule files
            schedule_filenames.last().or_error("No schedule found when trying to find the newest schedule file.")?.clone() //return the newest file (last filename)
        };
        println!("Using schedule '{}'", schedule_filename);
        Ok(schedule_filename)
    }

}

pub struct FileCache<T> {
    object: Option<Arc<T>>,
    filename: Option<String>,
    modification_time: Option<std::time::SystemTime>,
}

impl<T> FileCache<T> where T: Loadable<T> {

    //creates a new, empty file cache
    pub fn new() -> FileCache<T> {
        return FileCache::<T> {
            object: None,
            filename: None,
            modification_time: None
        }
    }

    // wrapper around get_cached so the mutex stuff does not have to be repeated
    pub fn get_cached_simple(cache: &Mutex<Self>, filename: &str) -> FnResult<Arc<T>> {
        let mut cache_lock = cache.lock().unwrap();
        cache_lock.get_cached(filename)
    }

    // Returns the cached object. 
    // If possible, use get_cached_simple instead to avoid dealing with mutex stuff directly.
    pub fn get_cached(&mut self, filename: &str) -> FnResult<Arc<T>> {

        let mut filename_changed = true;
        let mut modtime_changed = true;

        let metadata = fs::metadata(filename)?;
        let mod_time = metadata.modified()?;

        //compare filenames:
        if let Some(f) = &self.filename {
            if &f == &filename {
                filename_changed = false;

                //compare modification times:
                if let Some(mt) = self.modification_time {
                    if mt == mod_time {
                        modtime_changed = false;
                    } else {
                        self.modification_time = Some(mod_time);
                    }
                } else {
                    self.modification_time = Some(mod_time);
                }
            } else {
                self.filename = Some(filename.to_string());
                self.modification_time = Some(mod_time);
            }
        } else {
            self.filename = Some(filename.to_string());
            self.modification_time = Some(mod_time);
        }

        //reload file if anything changed:
        if filename_changed || modtime_changed {
            self.object = None;
            let obj = <T>::load(filename)?;
            self.object = Some(Arc::new(obj));
        }

        match &self.object {
            Some(o) => Ok(o.clone()),
            None => bail!("Object {} could not be returned from cache. Loading probably failed in a previous iteration.", filename)
        }
    }
} 

pub trait Loadable<T> {
    fn load(filename: &str) -> FnResult<T>;
}

impl Loadable<Gtfs> for Gtfs {
    fn load(filename: &str) -> FnResult<Gtfs> {
        let gtfs = Gtfs::new(filename)?;
        return Ok(gtfs);
    }
}

impl Loadable<DelayStatistics> for DelayStatistics {
    fn load(filename: &str) -> FnResult<DelayStatistics> {

        let mut f = File::open(filename).expect(&format!("Could not open {}", filename));
        let mut buffer = Vec::<u8>::new();
        f.read_to_end(&mut buffer)?;
        let parsed = rmp_serde::from_read_ref::<_, Self>(&buffer)?;

        return Ok(parsed);
    }
}

/// Adds a time (as seconds since/before midnight) to a NaiveDateTime.
/// This is nessecary because NaiveTime can't handle negative times
/// or times larger than 24 hours.
pub fn date_and_time(date: &NaiveDate, time: i32) -> NaiveDateTime {
    const SECONDS_PER_DAY: i32 = 24 * 60 * 60;
    let extra_days = (time as f32 / SECONDS_PER_DAY as f32).floor() as i32;
    let actual_time = time - extra_days * SECONDS_PER_DAY;
    assert!(actual_time >= 0);
    assert!(actual_time <= SECONDS_PER_DAY);
    let actual_date = *date + Duration::days(extra_days as i64);
    return actual_date.and_time(NaiveTime::from_num_seconds_from_midnight(actual_time as u32, 0));
}