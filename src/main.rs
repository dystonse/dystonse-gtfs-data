use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;
use prost::Message; // need to use this, otherwise FeedMessage won't have a `decode` method
use std::io::prelude::*;

use gtfs_rt::FeedMessage;

fn run() -> Result<(), Box<dyn Error>> {
    println!("Verusche, Protobuf zu lesen…");
    let file_path = get_first_arg()?;
    let mut file = File::open(file_path)?;
    let mut vec = Vec::<u8>::new();

    // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
    file.read_to_end(&mut vec)?; 
    let message = FeedMessage::decode(&vec)?;

    let mut count = 0;

    // `message.entity` is actually a collection of entities
    for entity in message.entity {
        if let Some(trip_update) = entity.trip_update {
            count+=1;
            // println!("Trip update for trip: {:?}", trip_update.trip);
        }
    }
    println!("Counted {} trip updates.", count);
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("Expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}


fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}