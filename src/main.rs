use std::env;
use std::error::Error;
use gtfs_structures::Gtfs;

use mysql::*;

mod importer;

// This is handy, because mysql defines its own Result type and we don't 
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

/// Opens a connection to a database and returns the resulting connection pool. 
/// Takes configuration values from DB_PASSWORD, DB_USER, DB_HOST, DB_PORT and DB_DATABASE
/// environment variables. For all values except DB_PASSWORD a default is provided.
fn open_db() -> FnResult<Pool>  {
    let password = env::var("DB_PASSWORD")?;
    let user = env::var("DB_USER").unwrap_or(String::from("dystonse"));
    let host = env::var("DB_HOST").unwrap_or(String::from("localhost"));
    let port = env::var("DB_PORT").unwrap_or(String::from("3306"));
    let database = env::var("DB_DATABASE").unwrap_or(String::from("dystonse"));
    let url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database);
    let pool = Pool::new(url)?;
    
    Ok(pool)
}

/// Opens the database, reads schedule and transfers realtime data from 
/// protobuffer files into the database.
/// gtfs is read from the first command line parameter (url or path to zip or directory)
/// gtfsrt is read from all other command line parameters (path to pb file)
fn main() -> FnResult<()> {

    let mut args = env::args_os();
    // skip element 0 because it's the executable's name
    args.next(); 

    // use element 1 as schedule file
    let mut gtfs_schedule_filename_oss = args.next().unwrap();
    let mut gtfs_schedule_filename = gtfs_schedule_filename_oss.to_str().expect("invalid OsString");

    let mut verbose = false;

    // if the first arg is "-v", enable verbose mode and use next argument as "first" to define the schedule file
    if gtfs_schedule_filename == "-v" {
        verbose = true;
        gtfs_schedule_filename_oss = args.next().unwrap();
        gtfs_schedule_filename = gtfs_schedule_filename_oss.to_str().expect("invalid OsString");
    }

    // use all other elements as realtime file
    let gtfs_realtime_filenames = args.map(|arg| String::from(arg.to_str().expect("invalid OsString")));

    // connect to the database
    if verbose {
        println!("Connecting to database…");
    }
    let pool = open_db()?;

    // parse schedule
    if verbose {
        println!("Parsing schedule…");
    }
    let gtfs = Gtfs::new(gtfs_schedule_filename).expect("Gtfs deserialisation");

    if verbose {
        println!("Importing realtime data…");
    }
    // create importer and iterate over all realtime files
    let mut imp = importer::Importer::new(&gtfs, &pool, verbose).expect("Could not create importer");
    for gtfs_realtime_filename in gtfs_realtime_filenames {
        imp.import_realtime_into_database(&gtfs_realtime_filename)?;
        if verbose {
            println!("Finished importing file: {}", &gtfs_realtime_filename);
        } else {
            println!("{}", &gtfs_realtime_filename);
        }
    }

    if verbose {
        println!("Done!");
    }
    Ok(())
}