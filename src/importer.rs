use gtfs_structures::Gtfs;
use mysql::*;
use prost::Message; // need to use this, otherwise FeedMessage won't have a `decode` method
use mysql::prelude::*;
use gtfs_rt::FeedMessage;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;

use crate::FnResult;


pub struct Importer<'a> {
    conn: PooledConn,
    gtfs: &'a Gtfs
}

impl<'a> Importer<'a> {
    pub fn new(gtfs: &'a Gtfs, pool: &Pool) -> std::result::Result<Importer<'a>, Box<dyn Error>> {
        Ok(Importer { gtfs, conn: pool.get_conn()?})
    }

    pub fn read_proto_buffer(&mut self, path: &str) -> FnResult<()> {
        let mut file = File::open(path)?;
        let mut vec = Vec::<u8>::new();
    
        // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
        file.read_to_end(&mut vec)?;
        let message = FeedMessage::decode(&vec)?;
    
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
                    let s = self.conn.prep(r"INSERT INTO `realtime` 
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
                    let time_schedule = self.gtfs.get_trip(&trip_id).expect("no trip in schedule").stop_times[stop_sequence].arrival_time.expect("no arrival time") as i32;
                    let time_estimate = time_schedule + delay;
    
                    
                    self.conn.exec_drop(s, params! { 
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
                }
            }
        }
        Ok(())
    }
}