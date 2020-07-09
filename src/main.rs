mod importer;
mod analyser;
mod predictor;
mod types;

use std::error::Error;
#[macro_use]
extern crate lazy_static;

use clap::{App, Arg, ArgMatches};
use mysql::*;
use retry::delay::Fibonacci;
use retry::retry;
use simple_error::{SimpleError, bail};
use chrono::NaiveDate;
use regex::Regex;
use std::fs;

use importer::Importer;
use analyser::Analyser;
use predictor::Predictor;

// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

pub struct Main {
    verbose: bool,
    pool: Pool,
    args: ArgMatches,
    source: String,
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

impl<T, E> OrError<T> for std::result::Result<T, E> {
    fn or_error(self, message: &str) -> FnResult<T> {
        match self {
            Err(_) => bail!(message),
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
    let matches = App::new("dystonse-gtfs-data")
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
        )
        .get_matches();
    return matches;
}

impl Main {
    /// Constructs a new instance of Main, with parsed arguments and a ready-to-use pool of database connections.
    fn new() -> FnResult<Main> {
        let args = parse_args();
        let verbose = args.is_present("verbose");
        let source = String::from(args.value_of("source").unwrap()); // already validated by clap

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
            pool,
            source,
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
                let mut predictor = Predictor::new(&self, sub_args);
                predictor.run()
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

}