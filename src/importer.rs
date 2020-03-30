use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use gtfs_rt::FeedMessage as GtfsRealtimeMessage;
use gtfs_structures::Gtfs;
use gtfs_structures::Trip as ScheduleTrip;
use mysql::prelude::*;
use mysql::*;
use prost::Message; // need to use this, otherwise GtfsRealtimeMessage won't have a `decode` method
use std::fs::File;
use std::io::prelude::*;

use crate::FnResult;

const MAX_BATCH_SIZE: usize = 1000;

pub struct Importer<'a> {
    pool: &'a Pool,
    gtfs_schedule: &'a Gtfs,
    verbose: bool,
    source: &'a str
}

enum EventType {
    Arrival,
    Departure,
}

struct EventTimes {
    schedule: Option<i64>,
    estimate: Option<i64>,
    delay: Option<i64>,
}

impl EventTimes {
    fn empty() -> EventTimes {
        EventTimes {
            schedule: None,
            estimate: None,
            delay: None,
        }
    }
}

struct BatchedInsertions<'a> {
    params_vec: Vec<Params>,
    conn: &'a mut PooledConn,
    statement: Statement,
}

impl<'a> Importer<'a> {
    pub fn new(gtfs_schedule: &'a Gtfs, pool: &'a Pool, verbose: bool, source: &'a str) -> Importer<'a> {
        Importer {
            gtfs_schedule,
            pool,
            verbose,
            source,
        }
    }

    pub fn import_realtime_into_database(&self, path: &str) -> FnResult<()> {
        let mut file = File::open(path)?;
        let mut vec = Vec::<u8>::new();
        if path.ends_with(".zip") {
            let mut archive = zip::ZipArchive::new(file).unwrap();
            let mut zipped_file = archive.by_index(0).unwrap();
            if self.verbose {
                println!("Reading {} from zip…", zipped_file.name());
            }
            zipped_file.read_to_end(&mut vec)?;
        } else {
            file.read_to_end(&mut vec)?;
        }
        // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
        let message = GtfsRealtimeMessage::decode(&vec)?;
        let time_of_recording = message.header.timestamp.expect("No global timestamp");

        // TODO: Remove those statistics, they aren't accurate anyway
        let mut count_all_trip_updates = 0;
        let mut count_all_stop_time_updates = 0;
        let mut count_success = 0;
        let count_no_arrival = 0;
        let count_no_delay = 0;

        let mut conn = self.pool.get_conn()?;
        let mut batched = BatchedInsertions::new(&mut conn);
        // `message.entity` is actually a collection of entities
        for entity in message.entity {
            if let Some(trip_update) = entity.trip_update {
                count_all_trip_updates += 1;

                let realtime_trip = trip_update.trip;
                let route_id = realtime_trip.route_id.expect("Trip needs route_id");
                let trip_id = realtime_trip.trip_id.expect("Trip needs id");

                let start_date = if let Some(datestring) = realtime_trip.start_date {
                    NaiveDate::parse_from_str(&datestring, "%Y%m%d")
                        .expect(&datestring)
                        .and_hms(0, 0, 0)
                } else {
                    eprintln!("Trip without start date. Skipping.");
                    continue;
                };

                // TODO check if we actually need this
                let realtime_schedule_start_time =
                    if let Some(timestring) = realtime_trip.start_time {
                        NaiveTime::parse_from_str(&timestring, "%H:%M:%S").expect(&timestring)
                    } else {
                        eprintln!("Trip without start time. Skipping.");
                        continue;
                    };
                let schedule_trip = if let Ok(trip) = self.gtfs_schedule.get_trip(&trip_id) {
                    trip
                } else {
                    eprintln!("Did not find trip {} in schedule. Skipping.", trip_id);
                    continue;
                };

                let schedule_start_time = schedule_trip.stop_times[0].departure_time;
                let time_difference = realtime_schedule_start_time.num_seconds_from_midnight()
                    - schedule_start_time.unwrap();
                if time_difference != 0 {
                    eprintln!("Trip {} has a difference of {} seconds between scheduled start times in schedule data and realtime data.", trip_id, time_difference);
                }

                for stop_time_update in trip_update.stop_time_update {
                    count_all_stop_time_updates += 1;

                    let stop_id = stop_time_update.stop_id.expect("no stop_id");
                    let stop_sequence =
                        stop_time_update.stop_sequence.expect("no stop_sequence") as usize;
                    // There's an enum but conversion to u32 is not supported: gtfs.get_route(&route_id).expect("I've got no route!!!").route_type as u32;
                    // TODO add a method impl to RouteType to convert it back to u32
                    let mode = 99;

                    let arrival = Importer::handle_stop_time_update(
                        stop_time_update.arrival,
                        start_date,
                        EventType::Arrival,
                        &schedule_trip,
                        stop_sequence,
                    );
                    let departure = Importer::handle_stop_time_update(
                        stop_time_update.departure,
                        start_date,
                        EventType::Departure,
                        &schedule_trip,
                        stop_sequence,
                    );

                    batched.add_insertion(Params::from(params! {
                        "trip_id" => &trip_id,
                        stop_id,
                        "route_id" => &route_id,
                        stop_sequence,
                        time_of_recording,
                        mode,
                        "time_arrival_schedule" => arrival.schedule,
                        "time_arrival_estimate" => arrival.estimate,
                        "delay_arrival" => arrival.delay,
                        "time_departure_schedule" => departure.schedule,
                        "time_departure_estimate" => departure.estimate,
                        "delay_departure" => departure.delay,
                        "source" => &self.source,
                    }))?;

                    count_success += 1;
                }
            }
        }

        batched.write_to_database()?;

        // TODO: Remove those statistics, they aren't accurate anyway
        if self.verbose {
            println!("Finished processing {} trip updates with {} stop time updates. Success: {}, No arrival: {}, No delay: {}", 
                count_all_trip_updates, count_all_stop_time_updates, count_success, count_no_arrival, count_no_delay);
        }
        Ok(())
    }

    fn handle_stop_time_update(
        event: Option<gtfs_rt::trip_update::StopTimeEvent>,
        start_date: NaiveDateTime,
        event_type: EventType,
        schedule_trip: &ScheduleTrip,
        stop_sequence: usize,
    ) -> EventTimes {
        let delay = if let Some(event) = event {
            if let Some(delay) = event.delay {
                delay as i64
            } else {
                eprintln!(
                    "Stop time update {} without delay. Skipping.",
                    match event_type {
                        EventType::Arrival => "arrival",
                        EventType::Departure => "departure",
                    }
                );
                return EventTimes::empty();
            }
        } else {
            return EventTimes::empty();
        };

        let event_time = match event_type {
            EventType::Arrival => schedule_trip.stop_times[stop_sequence].arrival_time,
            EventType::Departure => schedule_trip.stop_times[stop_sequence].departure_time,
        };
        let schedule = start_date.timestamp() + event_time.expect("no arrival time") as i64;
        let estimate = schedule + delay;

        EventTimes {
            delay: Some(delay),
            schedule: Some(schedule),
            estimate: Some(estimate),
        }
    }
}

impl<'a> BatchedInsertions<'a> {
    fn new(conn: &mut PooledConn) -> BatchedInsertions {
        let statement = conn.prep(r"INSERT INTO `realtime` 
                    (`id`, `trip_id`, `stop_id`, `route_id`, `stop_sequence`, `mode`, `delay_arrival`, `delay_departure`,
                    `time_of_recording`, `time_arrival_schedule`, `time_arrival_estimate`, `time_departure_schedule`, `time_departure_estimate`, `source`) 
                    VALUES 
                    (NULL, :trip_id, :stop_id, :route_id, :stop_sequence, :mode, :delay_arrival, :delay_departure, 
                    FROM_UNIXTIME(:time_of_recording), FROM_UNIXTIME(:time_arrival_schedule), FROM_UNIXTIME(:time_arrival_estimate), FROM_UNIXTIME(:time_departure_schedule), FROM_UNIXTIME(:time_departure_estimate), :source)")
                    .expect("Could not prepare statement");

        BatchedInsertions {
            params_vec: Vec::with_capacity(MAX_BATCH_SIZE),
            conn,
            statement,
        }
    }

    fn add_insertion(&mut self, insertion: Params) -> FnResult<()> {
        self.params_vec.push(insertion);
        if self.params_vec.len() > MAX_BATCH_SIZE {
            self.write_to_database()?;
        }
        Ok(())
    }

    fn write_to_database(&mut self) -> FnResult<()> {
        let mut tx = self.conn.start_transaction(TxOpts::default())?;
        tx.exec_batch(&self.statement, &self.params_vec)?;
        self.params_vec.clear();
        tx.commit()?;
        Ok(())
    }
}
