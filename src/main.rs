use std::error::Error;
use std::fs;
use std::fs::DirBuilder;
use std::path::{Path, PathBuf};
use std::{thread, time};
#[macro_use]
extern crate lazy_static;

use chrono::NaiveDate;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use mysql::*;
use rayon::prelude::*;
use regex::Regex;
use retry::retry;
use retry::delay::Fibonacci;

mod importer;
use importer::Importer;

// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

const TIME_BETWEEN_DIR_SCANS: time::Duration = time::Duration::from_secs(60);

struct Main {
    verbose: bool,
    pool: Pool,
    args: ArgMatches,
    source: String
}

fn main() -> FnResult<()> {
    let instance = Main::new()?;
    instance.run()?;
    Ok(())
}

fn parse_args() -> ArgMatches {
    let matches = App::new("Dystonse GTFS Importer")
        .subcommand(App::new("automatic")
            .about("Runs forever, importing all files which are present or become present during the run.")
            .arg(Arg::with_name("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .help("The directory which contains schedules and realtime data")
                .long_help(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and realtime data (located in a subdirectory named 'rt'). \
                    Successfully processed files are moved to a subdirectory named 'imported'. \
                    The 'imported' subdirectory will be created automatically if it doesn't already exist."
                )
            )
        )
        .subcommand(App::new("batch")
            .about("Imports all files which are present at the time it is started.")
            .arg(Arg::with_name("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .help("The directory which contains schedules and realtime data")
                .long_help(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and realtime data (located in a subdirectory named 'rt'). \
                    Successfully processed files are moved to a subdirectory named 'imported'. \
                    The 'imported' subdirectory will be created automatically if it doesn't already exist."
                )
            )
        )
        .subcommand(App::new("manual")
            .about("Imports all specified realtime files using one specified schedule. Paths to schedule and realtime files have to be given as arguments.")
            .arg(Arg::with_name("schedule")
                .index(1)
                .value_name("SCHEDULE")
                .help("The static GTFS schedule, as directory or .zip")
                .required_unless("help")
            ).arg(Arg::with_name("rt")
                .index(2)
                .multiple(true)
                .value_name("PBs")
                .help("One or more files with real time data, as .pb or .zip")
                .required_unless("help")
            )
        ).arg(Arg::with_name("verbose")
            .short('v')
            .long("verbose")
            .help("Output status messages during run.")
        ).arg(Arg::with_name("password")
            .short('p')
            .long("password")
            .env("DB_PASSWORD")
            .takes_value(true)
            .help("Password used to connect to the database.")
            .required_unless("help")
        ).arg(Arg::with_name("user")
            .short('u')
            .long("user")
            .env("DB_USER")
            .takes_value(true)
            .help("User on the database.")
            .default_value("dystonse")
        ).arg(Arg::with_name("host")
            .long("host")
            .env("DB_HOST")
            .takes_value(true)
            .help("Host on which the database can be connected.")
            .default_value("localhost")   
        ).arg(Arg::with_name("port")
            .long("port")
            .env("DB_PORT")
            .takes_value(true)
            .help("Port on which the database can be connected.")
            .default_value("3306")
        ).arg(Arg::with_name("database")
            .short('d')
            .long("database")
            .env("DB_DATABASE")
            .takes_value(true)
            .help("Database name which will be selected.")
            .default_value("dystonse")
        ).arg(Arg::with_name("source")
            .short('s')
            .long("source")
            .env("GTFS_DATA_SOURCE_ID")
            .takes_value(true)
            .help("Source identifier for the data sets. Used to distinguish data sets with non-unique ids.")
            .required_unless("help")
            .takes_value(true)
        )
        .get_matches();
    return matches;
}

impl Main {
    /// Constructs a new instance of Main, with parsed arguments and a ready-to-use pool of database connections.
    fn new() -> FnResult<Main> {
        let args = parse_args();
        let verbose = args.is_present("verbose");
        let source = String::from(args.value_of("source").unwrap());

        if verbose {
            println!("Connecting to database…");
        }
        let pool = retry(Fibonacci::from_millis(1000), || {
            Main::open_db(&args)
        }).expect("DB connections should succeed eventually.");
        
        Ok(Main {
            args,
            verbose,
            pool,
            source,
        })
    }

    /// Opens a connection to a database and returns the resulting connection pool.
    /// Takes configuration values from DB_PASSWORD, DB_USER, DB_HOST, DB_PORT and DB_DATABASE
    /// environment variables. For all values except DB_PASSWORD a default is provided.
    fn open_db(args: &ArgMatches) -> FnResult<Pool> {
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            args.value_of("user").unwrap(),
            args.value_of("password").unwrap(),
            args.value_of("host").unwrap(),
            args.value_of("port").unwrap(),
            args.value_of("database").unwrap()
        );
        let pool = Pool::new(url)?;
        Ok(pool)
    }

    /// Runs the actions that are selected via the command line args
    fn run(&self) -> FnResult<()> {
        match self.args.subcommand() {
            ("automatic", Some(sub_args)) => self.run_as_non_manual(sub_args, true),
            ("batch", Some(sub_args)) => self.run_as_non_manual(sub_args, false),
            ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }

    /// Handle manual mode
    fn run_as_manual(&self, args: &ArgMatches) -> FnResult<()> {
        let gtfs_schedule_filename = args.value_of("schedule").unwrap();
        let gtfs_realtime_filenames: Vec<String> = args
            .values_of("rt")
            .unwrap()
            .map(|s| String::from(s))
            .collect();
        self.process_schedule_and_realtimes(
            &gtfs_schedule_filename,
            &gtfs_realtime_filenames,
            None,
        )?;

        Ok(())
    }

    fn read_dir_simple(path: &str) -> FnResult<Vec<String>> {
        let mut path_list: Vec<String> = fs::read_dir(path)?
            .filter_map(|r| r.ok())
            .map(|d| String::from(d.path().to_str().unwrap()))
            .collect();
        path_list.sort();
        Ok(path_list)
    }

    fn date_from_filename(filename: &str) -> NaiveDate {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();
        }
        let cap = FIND_DATE.captures(&filename).unwrap();
        NaiveDate::from_ymd(
            cap[1].parse().expect(""),
            cap[2].parse().expect(""),
            cap[3].parse().expect(""),
        )
    }

    /// Handle automatic mode and batch mode, which are very similar to each other
    fn run_as_non_manual(&self, args: &ArgMatches, is_automatic: bool) -> FnResult<()> {
        // construct paths of directories
        let dir = args.value_of("dir").unwrap();
        let schedule_dir = format!("{}/schedule", dir);
        let rt_dir = format!("{}/rt", dir);
        let target_dir = format!("{}/imported", dir);

        // ensure that the directory exists
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        builder.create(&target_dir)?;
        loop {
            if self.verbose { println!("Scan directory"); }
            // list files in both directories
            let mut schedule_filenames = Main::read_dir_simple(&schedule_dir)?;
            let rt_filenames = Main::read_dir_simple(&rt_dir)?;

            if rt_filenames.is_empty() {
                if is_automatic {
                    if self.verbose { println!("No realtime data. Going to sleep and checking again later.");}
                    thread::sleep(TIME_BETWEEN_DIR_SCANS);
                    continue;
                } else {
                    println!("No realtime data, exiting.");
                    return Ok(());
                }
            }

            if schedule_filenames.is_empty() {
                if is_automatic {
                    if self.verbose { println!("No schedule data. Going to sleep and checking again later.");}
                    thread::sleep(TIME_BETWEEN_DIR_SCANS);
                    continue;
                } else {
                    println!("No schedule data, but real time data is present. Exiting.");
                    return Ok(());
                }
            }

            // get the date of the earliest schedule, then reverse the list to start searching with the latest schedule
            let oldest_schedule_date = Main::date_from_filename(&schedule_filenames[0]);
            schedule_filenames.reverse();

            // data structures to collect the files to work on in the current iteration (one schedule and all its corresponding rt files)
            let mut current_schedule_file = String::new();
            let mut realtime_files_for_current_schedule: Vec<String> = Vec::new();

            // Iterate over all rt files (oldest first), collecting all rt files that belong to the same schedule to process them in batch.
            for rt_filename in rt_filenames {
                let rt_date = Main::date_from_filename(&rt_filename);
                if rt_date <= oldest_schedule_date {
                    eprintln!(
                        "Realtime data {} is older than any schedule, skipping,",
                        rt_filename
                    );
                    continue;
                }

                // Look at all schedules (newest first)
                for schedule_filename in &schedule_filenames {
                    let schedule_date = Main::date_from_filename(&schedule_filename);
                    // Assume we found the right schedule if this is the newest schedule that is older than the realtime file:
                    if rt_date > schedule_date { 
                        // process the current schedule's collection before going to next schedule
                        if *schedule_filename != current_schedule_file {
                            if !realtime_files_for_current_schedule.is_empty() {
                                self.process_schedule_and_realtimes(
                                    &current_schedule_file,
                                    &realtime_files_for_current_schedule,
                                    Some(&target_dir),
                                )?;
                            }
                            // go on with the next schedule
                            current_schedule_file = schedule_filename.clone();
                            realtime_files_for_current_schedule.clear();
                        }
                        realtime_files_for_current_schedule.push(rt_filename.clone());
                        // Correct schedule found for this one, so continue with next realtime file
                        break;
                    }
                }
            }

            // process last schedule's collection
            if !realtime_files_for_current_schedule.is_empty() {
                self.process_schedule_and_realtimes(
                    &current_schedule_file,
                    &realtime_files_for_current_schedule,
                    Some(&target_dir),
                )?;
            }

            if !is_automatic {
                println!("Finished.");
                return Ok(());
            }

            if self.verbose { println!("Finished one iteration. Sleeping until next directory scan."); }
            thread::sleep(TIME_BETWEEN_DIR_SCANS);
        }
    }

    /// Perform the import of one or more realtime data sets relating to a single schedule
    fn process_schedule_and_realtimes(
        &self,
        gtfs_schedule_filename: &str,
        gtfs_realtime_filenames: &Vec<String>,
        target_dir: Option<&String>,
    ) -> FnResult<()> {
        if self.verbose {
            println!("Parsing schedule…");
        }
        let gtfs = Gtfs::new(gtfs_schedule_filename).expect("Gtfs deserialisation");

        if self.verbose {
            println!("Importing realtime data…");
        }
        // create importer for this schedule and iterate over all given realtime files
        let imp =
            Importer::new(&gtfs, &self.pool, self.verbose, &self.source).expect("Could not create importer");

        gtfs_realtime_filenames
            .par_iter()
            .for_each(|gtfs_realtime_filename| {
                match self.process_realtime(&gtfs_realtime_filename, &imp, target_dir) {
                    Ok(_) => (),
                    Err(e) => eprintln!("Error while reading {}: {}", &gtfs_realtime_filename, e),
                }
            });
        if self.verbose {
            println!("Done!");
        }
        Ok(())
    }

    /// Process a single realtime file on the given Importer
    fn process_realtime(
        &self,
        gtfs_realtime_filename: &str,
        imp: &Importer,
        target_dir: Option<&String>,
    ) -> FnResult<()> {
        imp.import_realtime_into_database(&gtfs_realtime_filename)?;
        if self.verbose {
            println!("Finished importing file: {}", &gtfs_realtime_filename);
        } else {
            println!("{}", &gtfs_realtime_filename);
        }
        if let Some(dir) = target_dir {
            let mut target_path = PathBuf::from(dir);
            target_path.push(Path::new(&gtfs_realtime_filename).file_name().unwrap());
            std::fs::rename(gtfs_realtime_filename, target_path)?;
        }
        Ok(())
    }
}
