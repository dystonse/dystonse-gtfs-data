use prost::Message; // need to use this, otherwise FeedMessage won't have a `decode` method
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::process;

use gtfs_rt::FeedMessage;

#[derive(Debug, Deserialize)]
struct Route {
    route_id: String,
    agency_id: u32,
    route_short_name: String,
    route_type: u32,
    // Don't read fields that we won't need anyway
    // route_long_name: String,
    // route_color: String,
    // route_text_color: String,
    // route_desc: String
}

/// Reads the csv file at the given path and puts each Route it reads into routes, using the route_id as the key.
fn read_csv(file_path: OsString, routes: &mut HashMap<String, Route>) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.deserialize() {
        // we need to put this into a var because the compiler needs a type annotation for Route
        let route: Route = result?;

        // we clone route.route_id because we can't pass its ownership to the map, 
        // and references don't work either for some reson 
        routes.insert(route.route_id.clone(), route);
    }

    Ok(())
}

/// Reads the pb file at the given path and prints each trip update, usind some data from the routes
fn read_pb(file_path: OsString, routes: &mut HashMap<String, Route>) -> Result<(), Box<dyn Error>> {
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
            println!("Trip update for Route {:?} at {}", routes.get(&route_id).expect("Route nicht gefunden").route_short_name, start_time);
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
    let mut routes: HashMap<String, Route> = HashMap::new();

    read_csv(get_nth_arg(1)?, &mut routes)?;
    read_pb(get_nth_arg(2)?, &mut routes)?;
    Ok(())
}

fn main() {
    if let Err(err) = real_main() {
        println!("{}", err);
        process::exit(1);
    }
}
