use std::error::Error;
use std::fs;
use std::fs::DirBuilder;
use std::path::{Path, PathBuf};
#[macro_use] extern crate lazy_static;

use gtfs_structures::Gtfs;
use mysql::*;
use rayon::prelude::*;
use regex::Regex;
use clap::{App, Arg, ArgMatches};
use chrono::{NaiveDate};

mod importer;
use importer::Importer;


// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

struct Main {
    verbose: bool,
    pool: Pool,
    args: ArgMatches,
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
            )
        )
        .subcommand(App::new("batch")
            .about("Imports all files which are present at the time it is started.")
            .arg(Arg::with_name("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .help("The directory which contains schedules and realtime data")
            )
        )
        .subcommand(App::new("manual")
            .about("Imports all files which are present at the time it is started.")
                .arg(Arg::with_name("schedule")
                .index(1)
                .value_name("SCHEDULE")
                .help("The  the static GTFS schedule, as directory or .zip")
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
            .global(true)
            .help("Output status messages during run.")
        ).arg(Arg::with_name("password")
            .short('p')
            .long("password")
            .global(true)
            .env("DB_PASSWORD")
            .help("Password used to connect to the database.")
            .required_unless("help")
        ).arg(Arg::with_name("user")
            .short('u')
            .long("user")
            .global(true)
            .env("DB_USER")
            .help("User on the database.")
            .default_value("dystonse")
        ).arg(Arg::with_name("host")
            .long("host")
            .global(true)
            .env("DB_HOST")
            .help("Host on which the database can be connected.")
            .default_value("localhost")   
        ).arg(Arg::with_name("port")
            .long("port")
            .global(true)
            .env("DB_PORT")
            .help("Port on which the database can be connected.")
            .default_value("3306")
        ).arg(Arg::with_name("database")
            .short('d')
            .long("database")
            .global(true)
            .env("DB_DATABASE")
            .help("Database name which will be selected.")
            .default_value("dystonse")
        )
    .get_matches();
    return matches;
}

impl Main {
    /// Constructs a new instance of Main, with parsed arguments and a ready-to-use pool of database connections.
    fn new() -> FnResult<Main> {
        let args = parse_args();
        let verbose = args.is_present("verbose");

        if verbose {
            println!("Connecting to database…");
        }
        let pool = Main::open_db(&args)?;
        Ok(Main {
            args,
            verbose,
            pool,
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
        let gtfs_realtime_filenames: Vec<String> = args.values_of("rt").unwrap().map(|s| String::from(s)).collect();
        self.process_schedule_and_realtimes(&gtfs_schedule_filename, &gtfs_realtime_filenames, None)?;

        Ok(())
    }

    fn read_dir_simple(path: &str) -> FnResult<Vec<String>> {
        let mut path_list: Vec<String> = fs::read_dir(path)?.filter_map(|r| r.ok()).map(|d| String::from(d.path().to_str().unwrap())).collect();
        path_list.sort();
        Ok(path_list)
    }

    fn date_from_filename(filename: &str) -> NaiveDate {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();
        }
        let cap = FIND_DATE.captures(&filename).unwrap();
        NaiveDate::from_ymd(cap[1].parse().expect(""), cap[2].parse().expect(""), cap[3].parse().expect(""))
    }

    /// Handle automatic mode and batch mode, which are very similar to each other
    fn run_as_non_manual(&self, args: &ArgMatches, is_automatic: bool) -> FnResult<()> {
        if is_automatic {
            panic!("Sorry, automatic mode is not implemented yet. Try the batch mode instead :)");
        }

        // construct paths of directories
        let dir = args.value_of("dir").unwrap();
        let schedule_dir = format!("{}/schedule", dir);
        let rt_dir = format!("{}/rt", dir);
        let target_dir = format!("{}/imported", dir);

        // ensure that the directory exists
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        builder.create(&target_dir)?;
        
        // list files in both directories
        let mut schedule_filenames = Main::read_dir_simple(&schedule_dir)?;
        let rt_filenames = Main::read_dir_simple(&rt_dir)?;

        if rt_filenames.is_empty() {
            println!("No realtime data, exiting.");
            return Ok(());
        }

        if schedule_filenames.is_empty() {
            println!("No scheulde data, but real time data is present. Exiting.");
            return Ok(());
        }

        // get the date of the earliest schedule, then reverse the list to start searching with the latest schedule
        let first_schedule_date = Main::date_from_filename(&schedule_filenames[0]);
        schedule_filenames.reverse();

        let mut current_schedule_file = String::new();
        let mut realtime_files_for_schedule:Vec<String> = Vec::new();

        // Itereate over all rt files, collecting all rt files that belong to the same schedule to process them in batch.
        for rt_filename in rt_filenames {
            let rt_date = Main::date_from_filename(&rt_filename);
            if rt_date <= first_schedule_date {
                println!("Realtime data {} is older than any schedule, skipping,", rt_filename);
                continue;
            }

            for schedule_filename in &schedule_filenames {
                let schedule_date = Main::date_from_filename(&schedule_filename);
                if rt_date > schedule_date {
                    if current_schedule_file != *schedule_filename {
                        if !realtime_files_for_schedule.is_empty() {
                            self.process_schedule_and_realtimes(&current_schedule_file, &realtime_files_for_schedule, Some(&target_dir))?;
                        }

                        current_schedule_file = schedule_filename.clone();
                        realtime_files_for_schedule = Vec::new();
                    }
                    realtime_files_for_schedule.push(rt_filename.clone());
                    break;
                }
            }
        }

        println!("Finished.");
        Ok(())
    }

    /// Perform the import of one or more realtime data sets relating to a single schedule
    fn process_schedule_and_realtimes(
        &self,
        gtfs_schedule_filename: &str,
        gtfs_realtime_filenames: &Vec<String>,
        target_dir: Option<&String>
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
            Importer::new(&gtfs, &self.pool, self.verbose).expect("Could not create importer");

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
    fn process_realtime(&self, gtfs_realtime_filename: &str, imp: &Importer, target_dir: Option<&String>) -> FnResult<()> {
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
