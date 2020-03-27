use gtfs_structures::Gtfs;
use std::error::Error;

use mysql::*;
use rayon::prelude::*;

mod importer;
use importer::Importer;

use clap::{Arg, App, ArgMatches};

// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

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

fn parse_args() -> ArgMatches {
    let matches = App::new("Dystonse GTFS Importer")

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

/// Opens the database, reads schedule and transfers realtime data from
/// protobuffer files into the database.
/// gtfs is read from the first command line parameter (url or path to zip or directory)
/// gtfsrt is read from all other command line parameters (path to pb file)
fn main() -> FnResult<()> {
    let matches = parse_args();

    match matches.subcommand() {
        ("automatic", Some(sub_args)) => run_as_non_manual(sub_args, true),
        _ => panic!("Invalid arguments.")
    }
}

fn run_as_manual(args: &ArgMatches) -> FnResult<()> {
    let gtfs_schedule_filename = args.value_of("schedule").unwrap();
    let verbose: bool = args.is_present("verbose");
    let gtfs_realtime_filenames: Vec<&str> = args.values_of("rt").unwrap().collect();

    if verbose { println!("Connecting to database…"); }
    let pool = open_db(args)?;

    if verbose { println!("Parsing schedule…"); }
    let gtfs = Gtfs::new(gtfs_schedule_filename).expect("Gtfs deserialisation");

    if verbose { println!("Importing realtime data…"); }
    // create importer and iterate over all realtime files
    let imp = Importer::new(&gtfs, &pool, verbose).expect("Could not create importer");

    gtfs_realtime_filenames
        .par_iter()
        .for_each(|gtfs_realtime_filename| {
            match process_gtfs_realtime(&gtfs_realtime_filename, &imp, verbose) {
                Ok(_) => (),
                Err(e) => eprintln!("Error while reading {}: {}", &gtfs_realtime_filename, e)
            }
        });
    
    if verbose {
        println!("Done!");
    }
    Ok(())
}

fn process_gtfs_realtime(
    gtfs_realtime_filename: &str,
    imp: &Importer,
    verbose: bool,
) -> FnResult<()> {
    imp.import_realtime_into_database(&gtfs_realtime_filename)?;
    if verbose {
        println!("Finished importing file: {}", &gtfs_realtime_filename);
    } else {
        println!("{}", &gtfs_realtime_filename);
    }
    Ok(())
}
