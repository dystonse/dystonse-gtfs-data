use gtfs_structures::Gtfs;
use std::error::Error;

use mysql::*;
use rayon::prelude::*;

mod importer;
use importer::Importer;

use clap::{App, Arg, ArgMatches};

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
            .arg(Arg::with_name("schedule")
                .index(1)
                .value_name("DIRECTORY")
                .help("The directory which contains schedules and realtime data")
            )
        )
        .subcommand(App::new("batch")
            .about("Imports all files which are present at the time it is started.")
            .arg(Arg::with_name("schedule")
                .index(1)
                .value_name("DIRECTORY")
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
        let gtfs_realtime_filenames: Vec<&str> = args.values_of("rt").unwrap().collect();
        self.process_schedule_and_realtimes(&gtfs_schedule_filename, &gtfs_realtime_filenames)?;

        Ok(())
    }

    /// Handle automatic mode and batch mode, which are very similar to each other
    fn run_as_non_manual(&self, _args: &ArgMatches, _is_automatic: bool) -> FnResult<()> {
        // TODO implement
        panic!("Non-manual modes are not yet implemented.");
    }

    /// Perform the import of one or more realtime data sets relating to a single schedule
    fn process_schedule_and_realtimes(
        &self,
        gtfs_schedule_filename: &str,
        gtfs_realtime_filenames: &Vec<&str>,
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
                match self.process_realtime(&gtfs_realtime_filename, &imp) {
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
    fn process_realtime(&self, gtfs_realtime_filename: &str, imp: &Importer) -> FnResult<()> {
        imp.import_realtime_into_database(&gtfs_realtime_filename)?;
        if self.verbose {
            println!("Finished importing file: {}", &gtfs_realtime_filename);
        } else {
            println!("{}", &gtfs_realtime_filename);
        }
        Ok(())
    }
}
