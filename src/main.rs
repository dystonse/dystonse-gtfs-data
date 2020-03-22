use std::env;
use std::error::Error;
use std::ffi::OsString;
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

/// Returns the nth positional argument sent to this process. If there are not enough
/// positional arguments, then this returns an error.
fn get_nth_arg(n: usize) -> FnResult<OsString> {
    match env::args_os().nth(n) {
        None => Err(From::from("Expected at least n argument(s), but got less.")),
        Some(file_path) => Ok(file_path),
    }
}

/// Opens the database, reads schedule and transfers realtime data from 
/// protobuffer file into the database.
/// gtfs is read from the first command line parameter (url or path to zip or directory)
/// gtfsrt is read from the second command line parameter (path to pb file)
fn main() -> FnResult<()> {
    
    let gtfs_schedule_filename = get_nth_arg(1)?;
    let gtfs_schedule_filename = gtfs_schedule_filename.to_str().expect("invalid OsString");
    let gtfs_realtime_filename = get_nth_arg(2)?;
    let gtfs_realtime_filename = gtfs_realtime_filename.to_str().expect("invalid OsString");
    let pool = open_db()?;
    let gtfs = Gtfs::new(gtfs_schedule_filename).expect("Gtfs deserialisierung");
    let mut imp = importer::Importer::new(&gtfs, &pool).expect("Could not create importer");
    imp.import_realtime_into_database(gtfs_realtime_filename)?;
    Ok(())
}