use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use gtfs_rt::FeedMessage as GtfsRealtimeMessage;
use gtfs_structures::Gtfs;
use gtfs_structures::Trip as ScheduleTrip;
use mysql::*;
use prost::Message; // need to use this, otherwise GtfsRealtimeMessage won't have a `decode` method
use simple_error::bail;
use std::fs::File;
use std::io::prelude::*;
use mysql::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use super::batched_statements::BatchedStatements;
use super::Importer;

use crate::{FnResult, OrError};
use crate::types::{EventType, GetByEventType, DelayStatistics};
use crate::predictor::Predictor;
use dystonse_curves::tree::{NodeData, SerdeFormat};

#[derive(PartialEq, Eq, Clone)]
struct PredictionBasis {
    stop_id: String,
    delay_departure: i32
}

#[derive(Hash, PartialEq, Eq)]
struct VehicleIdentifier {
    trip_id: String,
    start_time: NaiveTime,
    start_date: NaiveDate
}

pub struct PerScheduleImporter<'a> {
    importer: &'a Importer<'a>,
    gtfs_schedule: Arc<Gtfs>,
    verbose: bool,
    filename: &'a str,
    record_statements: Option<BatchedStatements>,
    arrival_statements: Option<BatchedStatements>,
    departure_statements: Option<BatchedStatements>,
    perform_record: bool,
    perform_predict: bool,
    predictor: Option<Predictor<'a>>,
    current_prediction_basis: Mutex<HashMap<VehicleIdentifier, PredictionBasis>>
}

/// For an event (which may be an arrival or a departure), this struct
/// contains the three possible times, where (logically) estimate = schedule + delay.
/// No checkts are performed though.
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

    fn is_empty(&self) -> bool {
        return self.schedule.is_none() && self.estimate.is_none() && self.delay.is_none();
    }
}

impl<'a> PerScheduleImporter<'a> {
    pub fn new(
        gtfs_schedule: Arc<Gtfs>,
        importer: &'a Importer,
        verbose: bool,
        filename: &'a str,
    ) -> FnResult<PerScheduleImporter<'a>> {
        let mut instance = PerScheduleImporter {
            gtfs_schedule: Arc::clone(&gtfs_schedule),
            importer,
            verbose,
            filename,
            record_statements: None,
            arrival_statements: None,
            departure_statements: None,
            perform_record: importer.args.is_present("record"),
            perform_predict: importer.args.is_present("predict"),
            predictor: None,
            current_prediction_basis: Mutex::new(HashMap::new())
        };

        if instance.perform_record {
            instance.init_record_statements()?;
        }
        if instance.perform_predict {
            let dir_name = String::from(importer.args.subcommand_matches("automatic").unwrap().value_of("dir").unwrap());
            println!("Reading delay statistics from dir: {}", dir_name);
            let delay_stats = (DelayStatistics::load_from_file(&dir_name, "all_curves", &SerdeFormat::MessagePack))?;    

            instance.predictor = Some(Predictor {
                main: importer.main,
                args: &importer.main.args,
                _data_dir: None,
                schedule: Arc::clone(&gtfs_schedule),
                delay_statistics: delay_stats
            });
            //instance.init_arrival_statements()?;
            //instance.init_departure_statements()?;
        }

        Ok(instance)
    }

    pub fn handle_realtime_file(&self, path: &str) -> FnResult<()> {
        let mut file = File::open(path)?;
        let mut vec = Vec::<u8>::new();
        if path.ends_with(".zip") {
            let mut archive = zip::ZipArchive::new(file).or_error("Zip file not found.")?;
            let mut zipped_file = archive.by_index(0).or_error("Zip file was empty")?;
            if self.verbose {
                println!("Reading {} from zip…", zipped_file.name());
            }
            zipped_file.read_to_end(&mut vec)?;
        } else {
            file.read_to_end(&mut vec)?;
        }
        // suboptimal, I'd rather not read the whole file into memory, but maybe Prost just works like this
        let message = GtfsRealtimeMessage::decode(&vec)?;
        let time_of_recording = message.header.timestamp.or_error(
            "No global timestamp in realtime data, skipping."
        )?;

        self.process_message(&message, time_of_recording)?;
        Ok(())
    }

    fn process_message(&self, message: &GtfsRealtimeMessage, time_of_recording: u64) -> FnResult<()> { 
        // `message.entity` is actually a collection of entities
        for entity in &message.entity {
            if let Some(trip_update) = &entity.trip_update {
                if let Err(e) = self.process_trip_update(trip_update, time_of_recording) {
                    eprintln!("Error while processing trip update: {}.", e);
                }
            }
            if self.perform_record {
                self.record_statements.as_ref().unwrap().write_to_database()?;
            }
            if self.perform_predict {
                // Will panic with "not yet implemented":
                // self.arrival_statements.as_ref().unwrap().write_to_database()?;
                // self.departure_statements.as_ref().unwrap().write_to_database()?;
            }
        }
        Ok(())
    }

    fn process_trip_update(
        &self,
        trip_update: &gtfs_rt::TripUpdate,
        time_of_recording: u64,
    ) -> FnResult<()> {
        let realtime_trip = &trip_update.trip;
        let route_id = &realtime_trip.route_id.as_ref().or_error("Trip needs route_id")?;
        let trip_id = &realtime_trip.trip_id.as_ref().or_error("Trip needs id")?;

        let start_date = NaiveDate::parse_from_str(&realtime_trip.start_date.as_ref()
            .or_error("Trip without start date. Skipping.")?, "%Y%m%d")?;

        let realtime_schedule_start_time = NaiveTime::parse_from_str(&realtime_trip.start_time.as_ref()
            .or_error("Trip without start time. Skipping.")?, "%H:%M:%S")?;

        let schedule_trip = self.gtfs_schedule.get_trip(&trip_id)
            .or_error(&format!("Did not find trip {} in schedule. Skipping.", trip_id))?;

        let stu_count = trip_update.stop_time_update.len();
        if let Ok(route) = self.gtfs_schedule.get_route(&schedule_trip.route_id) {
            println!("Trip update with {} s.t.u. for route {} to {:?}:", stu_count, route.short_name, schedule_trip.trip_headsign);
        } else {
            println!("Trip update with {} s.t.u. for unknown route to {:?}:", stu_count, schedule_trip.trip_headsign);
        }

        let schedule_start_time = schedule_trip.stop_times[0].departure_time.unwrap();
        let time_difference =
            realtime_schedule_start_time.num_seconds_from_midnight() as i32 - schedule_start_time as i32;
        if time_difference != 0 {
            eprintln!("Trip {} has a difference of {} seconds between scheduled start times in schedule data and realtime data.", trip_id, time_difference);
        }

        let mut prediction_done = false;
        for stop_time_update in &trip_update.stop_time_update {
            
            let res = self.process_stop_time_update(
                stop_time_update,
                start_date,
                realtime_schedule_start_time,
                schedule_trip,
                &trip_id,
                &route_id,
                time_of_recording,
                &mut prediction_done
            );
            if let Err(e) = res {
                println!("Error with stop_time_update: {}", e);
            }
            if prediction_done {
                break;
            }
        }
        if !prediction_done {
            println!("At the end, still no prediction.");
        }

        Ok(())
    }

    fn process_stop_time_update(
        &self,
        stop_time_update: &gtfs_rt::trip_update::StopTimeUpdate,
        start_date: NaiveDate,
        start_time: NaiveTime,
        schedule_trip: &gtfs_structures::Trip,
        trip_id: &String,
        route_id: &String,
        time_of_recording: u64,
        prediction_done: &mut bool
    ) -> FnResult<()> {
        let stop_id : String = stop_time_update.stop_id.as_ref().or_error("no stop_id")?.clone();
        let stop_sequence = stop_time_update.stop_sequence.or_error("no stop_sequence")?;
        let start_date_time = start_date.and_time(start_time);
        let arrival = PerScheduleImporter::get_event_times(
            stop_time_update.arrival.as_ref(),
            start_date_time,
            EventType::Arrival,
            &schedule_trip,
            stop_sequence,
        );
        let departure = PerScheduleImporter::get_event_times(
            stop_time_update.departure.as_ref(),
            start_date_time,
            EventType::Departure,
            &schedule_trip,
            stop_sequence,
        );

        if arrival.is_empty() && departure.is_empty() {
            return Ok(());
        }

        if self.perform_record {
            self.record_statements.as_ref().unwrap().add_paramter_set(Params::from(params! {
                "source" => &self.importer.main.source,
                "route_id" => &route_id,
                "route_variant" => &schedule_trip.route_variant.as_ref().or_error("no route variant")?,
                "trip_id" => &trip_id,
                "date" => start_date,
                stop_sequence,
                "stop_id" => &stop_id,
                time_of_recording,
                "delay_arrival" => arrival.delay,
                "delay_departure" => departure.delay,
                "schedule_file_name" => self.filename
            }))?;
        }

        if departure.is_empty() {
            println!("Skip stop_sequence {} for predictions, because departure is empty.", stop_sequence);
        }

        if self.perform_predict && !*prediction_done && !departure.is_empty() {
            // we will try to do a prediction. We set this flag so that we 
            // don't do it again for the following stop_time_updates
            *prediction_done = true;
            let basis = PredictionBasis { 
                stop_id: stop_id.clone(),
                delay_departure: departure.delay.unwrap() as i32
            };
            let vehicle_id = VehicleIdentifier {
                trip_id: trip_id.clone(),
                start_date: start_date,
                start_time: start_time
            };

            {
                let mut cpr = self.current_prediction_basis.lock().unwrap();

                // check if we already made a prediction for this vehicle, and if, what was the basis
                if let Some(previous_basis) = cpr.get(&vehicle_id) {
                    // if we used the same basis, no need to do the same prediction again
                    if *previous_basis == basis {
                        println!("Didn't make new prediction, because we already have one with the same basis.");
                        return Ok(());
                    }
                }
            }
            let stop_count_estimate = schedule_trip.stop_times.iter().last().unwrap().stop_sequence as u32 - stop_sequence;
            println!("Trying to make predictions based on {} delay at stop sequence {} (about {} stops ahead).", 
                basis.delay_departure, stop_sequence, stop_count_estimate);

            let mut actual_success = false;

            for stop_time in &schedule_trip.stop_times {
                if stop_time.stop_sequence as u32 > stop_sequence {

                    let arrival_prediction = self.predictor.as_ref().unwrap().predict(
                        &route_id,
                        &trip_id, 
                        &Some((basis.stop_id.clone(), Some(basis.delay_departure as f32))),
                        &stop_time.stop.id, 
                        EventType::Arrival, 
                        NaiveDateTime::from_timestamp(departure.schedule.unwrap(), 0));
                    
                        
                    if arrival_prediction.is_ok() {
                        println!("Made a prediction for stop_sequence {}: {}", stop_time.stop_sequence, arrival_prediction.unwrap());
                        actual_success = true;
                        // TODO write to DB
                    } else {
                        println!("Prediction error for stop_sequence {}: {}", stop_time.stop_sequence, arrival_prediction.err().unwrap())
                    }

                    // TODO do the same for departute_prediction as soon as the
                    // predictor can do departure-to-departure-predictions
                }
            }

            if actual_success {
                let mut cpr = self.current_prediction_basis.lock().unwrap();
                cpr.insert(vehicle_id, basis.clone());
            }
        }

        Ok(())
    }

    fn get_event_times(
        event: Option<&gtfs_rt::trip_update::StopTimeEvent>,
        start_date_time: NaiveDateTime,
        event_type: EventType,
        schedule_trip: &ScheduleTrip,
        stop_sequence: u32,
    ) -> EventTimes {
        let delay = if let Some(event) = event {
            if let Some(delay) = event.delay {
                delay as i64
            } else {
                eprintln!("Stop time update {:?} without delay. Skipping.", event_type);
                return EventTimes::empty();
            }
        } else {
            return EventTimes::empty();
        };

        let potential_stop_time = schedule_trip.stop_times.iter().filter(|st| st.stop_sequence == stop_sequence as u16).nth(0);
        let event_time = if let Some(stop_time) = potential_stop_time {
            stop_time.get_time(event_type)
        } else {
            eprintln!("Realtime data references stop_sequence {}, which does not exist in trip {}.", stop_sequence, schedule_trip.id);
            // TODO return Error or something
            return EventTimes::empty();
        };
        let schedule = start_date_time.timestamp() + event_time.expect("no arrival/departure time") as i64;
        let estimate = schedule + delay;

        EventTimes {
            delay: Some(delay),
            schedule: Some(schedule),
            estimate: Some(estimate),
        }
    }

    fn init_record_statements(&mut self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        let update_statement = conn.prep(r"UPDATE `records`
        SET 
            `stop_id` = :stop_id,
            `time_of_recording` = FROM_UNIXTIME(:time_of_recording),
            `delay_arrival` = :delay_arrival,
            `delay_departure` = :delay_departure,
            `schedule_file_name` = :schedule_file_name
        WHERE 
            `source` = :source AND
            `route_id` = :route_id AND
            `route_variant` = :route_variant AND
            `trip_id` = :trip_id AND
            `date` = :date AND
            `stop_sequence` = :stop_sequence AND
            `time_of_recording` < FROM_UNIXTIME(:time_of_recording);").expect("Could not prepare update statement"); // Should never happen because of hard-coded statement string

        
        let insert_statement = conn.prep(r"INSERT IGNORE INTO `records` (
            `source`, 
            `route_id`,
            `route_variant`,
            `trip_id`,
            `date`,
            `stop_sequence`,
            `stop_id`,
            `time_of_recording`,
            `delay_arrival`,
            `delay_departure`,
            `schedule_file_name`
        ) VALUES ( 
            :source,
            :route_id,
            :route_variant,
            :trip_id,
            :date,
            :stop_sequence,
            :stop_id,
            FROM_UNIXTIME(:time_of_recording),
            :delay_arrival,
            :delay_departure, 
            :schedule_file_name
        );")
        .expect("Could not prepare insert statement"); // Should never happen because of hard-coded statement string

        // TODO: update where old.time_of_recording < new.time_of_recording...; INSERT IGNORE...;
        self.record_statements = Some(BatchedStatements::new(conn, vec![update_statement, insert_statement]));
        Ok(())
    }

    fn init_arrival_statements(&self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        bail!("Not yet implemented.");
    }

    fn init_departure_statements(&self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        bail!("Not yet implemented.");
    }
}