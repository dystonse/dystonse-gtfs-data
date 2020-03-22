use std::env;
use std::error::Error;
use std::ffi::OsString;
use gtfs_structures::Gtfs;

use mysql::*;

// This is handy, because mysql defintes its own Result type and we don't want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

mod importer;

fn open_db() -> FnResult<Pool>  {
    let password = env::var("DB_PASSWORD").unwrap();
    let user = env::var("DB_USER").unwrap_or(String::from("dystonse"));
    let host = env::var("DB_HOST").unwrap_or(String::from("localhost"));
    let port = env::var("DB_PORT").unwrap_or(String::from("3306"));
    let database = env::var("DB_DATABSE").unwrap_or(String::from("dystonse"));
    let url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database);
    let pool = Pool::new(url)?;
    
    Ok(pool)
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(n: usize) -> FnResult<OsString> {
    match env::args_os().nth(n) {
        None => Err(From::from("Expected at least n argument(s), but got less.")),
        Some(file_path) => Ok(file_path),
    }
}

fn main() -> FnResult<()> {
    let pool = open_db()?;
    let gtfs = Gtfs::new(get_nth_arg(1)?.to_str().expect("gtfs filename")).expect("Gtfs deserialisierung");
    let mut imp = importer::Importer::new(&gtfs, &pool).expect("Could not create importer");
    imp.read_proto_buffer(&get_nth_arg(2)?.to_str().expect("Could not convert string"))?;
    Ok(())
}