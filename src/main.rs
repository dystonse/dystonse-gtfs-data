use prost::Message; // need to use this, otherwise FeedMessage won't have a `decode` method
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::process;
use gtfs_structures::Gtfs;

use gtfs_rt::FeedMessage;

use mysql::*;
use mysql::prelude::*;

// This is handy, because mysql defintes its own Result type and we don't want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;


fn open_db() -> FnResult<PooledConn>  {
    let password = env::var("DB_PASSWORD").unwrap();
    let user = env::var("DB_USER").unwrap_or(String::from("dystonse"));
    let host = env::var("DB_HOST").unwrap_or(String::from("localhost"));
    let port = env::var("DB_PORT").unwrap_or(String::from("3306"));
    let database = env::var("DB_DATABSE").unwrap_or(String::from("dystonse"));
    let url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database);
    let pool = Pool::new(url)?;
    let conn = pool.get_conn()?;
    
    Ok(conn)
}

/// Reads the pb file at the given path and prints each trip update, usind some data from the routes
fn read_pb(file_path: OsString, gtfs: & Gtfs, conn: &mut PooledConn) -> FnResult<()> {
    let mut file = File::open(file_path)?;
    let mut vec = Vec::<u8>::new();

    // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
    file.read_to_end(&mut vec)?;
    let message = FeedMessage::decode(&vec)?;

    // println!("Full message: {:?}", message);

    let time_record = message.header.timestamp.expect("No global timestamp");

    
       // `message.entity` is actually a collection of entities
    for entity in message.entity {
        if let Some(trip_update) = entity.trip_update {
            // the fields of trip_update are Options, so we need to handle the case that they are missing.
            let trip = trip_update.trip;

            let route_id = trip.route_id.expect("Trip needs route_id");
            // et start_time = trip.start_time.expect("Trip needs start_time");
            let trip_id = trip.trip_id.expect("Trip needs id");

            for stop_time_update in trip_update.stop_time_update {
                println!("Stop time update: {:?}", stop_time_update);

                let s = conn.prep(r"INSERT INTO `realtime` 
                (`id`, `trip_id`, `stop_id`, `route_id`, `stop_sequence`, `time_record`, `time_schedule`, `time_estimate`, `mode`, `delay`) 
                VALUES 
                (NULL, :trip_id, :stop_id, :route_id, :stop_sequence, FROM_UNIXTIME(:time_record), FROM_UNIXTIME(:time_schedule), FROM_UNIXTIME(:time_estimate), :mode, :delay) ").expect("Could not prepare statement");

                let stop_id = stop_time_update.stop_id.expect("no stop_time");
                let stop_sequence = stop_time_update.stop_sequence.expect("no stop_sequence") as usize;

                // There's an enum but conversion to u32 is not supported: gtfs.get_route(&route_id).expect("I've got not route!!!").route_type as u32;
                let mode = 0;
                let delay = if let Some(arrival) = stop_time_update.arrival {
                    arrival.delay.expect("no delay")
                } else {
                    continue;
                };

                // TODO time_record includes a date, time_schedule and time_estimate currently are on 1970-01-01.
                let time_schedule = gtfs.get_trip(&trip_id).expect("no trip in schedule").stop_times[stop_sequence].arrival_time.expect("no arrival time") as i32;
                let time_estimate = time_schedule + delay;

                
                conn.exec_drop(s, params! { 
                    "trip_id" => trip_id.clone(), 
                    stop_id,
                    "route_id" => route_id.clone(),
                    stop_sequence, 
                    time_record, 
                    time_schedule, 
                    time_estimate, 
                    mode, 
                    delay 
                })?;
                
                // println!("Trip update for Route {:?} at {}", gtfs.get_route(&route_id).expect("Route nicht gefunden").short_name, start_time);
                
            }
        }
    }
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(n: usize) -> FnResult<OsString> {
    match env::args_os().nth(n) {
        None => Err(From::from("Expected at least n argument(s), but got less.")),
        Some(file_path) => Ok(file_path),
    }
}

fn real_main() -> FnResult<()> {
    let mut conn = open_db()?;

    let gtfs = Gtfs::new(get_nth_arg(1)?.to_str().expect("gtfs filename")).expect("Gtfs deserialisierung");
    read_pb(get_nth_arg(2)?, &gtfs, &mut conn)?;
    Ok(())
}

fn main() {
    if let Err(err) = real_main() {
        println!("{}", err);
        process::exit(1);
    }
}
