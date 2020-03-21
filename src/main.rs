use prost::Message; // need to use this, otherwise FeedMessage won't have a `decode` method
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::process;
use gtfs_structures::Gtfs;

use gtfs_rt::FeedMessage;

/// Reads the pb file at the given path and prints each trip update, usind some data from the routes
fn read_pb(file_path: OsString, gtfs: & Gtfs) -> Result<(), Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut vec = Vec::<u8>::new();

    // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
    file.read_to_end(&mut vec)?;
    let message = FeedMessage::decode(&vec)?;

    // `message.entity` is actually a collection of entities
    for entity in message.entity {
        if let Some(trip_update) = entity.trip_update {
            // the fields of trip_update are Options, so we need to handle the case that they are missing.
            let route_id = trip_update.trip.route_id.expect("Trip needs route_id");
            let start_time = trip_update.trip.start_time.expect("Trip needs start_time");
            println!("Trip update for Route {:?} at {}", gtfs.get_route(&route_id).expect("Route nicht gefunden").short_name, start_time);
        }
    }
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_nth_arg(n: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(n) {
        None => Err(From::from("Expected at least n argument(s), but got less.")),
        Some(file_path) => Ok(file_path),
    }
}

fn real_main() -> Result<(), Box<dyn Error>> {
    let gtfs = Gtfs::new(get_nth_arg(1)?.to_str().expect("gtfs filename")).expect("Gtfs deserialisierung");
    //let gtfs = Gtfs::new(get_nth_arg(1)?.as_os_str().to_str().expect("Blöder String"));
    read_pb(get_nth_arg(2)?, &gtfs)?;
    Ok(())
}

fn main() {
    if let Err(err) = real_main() {
        println!("{}", err);
        process::exit(1);
    }
}
