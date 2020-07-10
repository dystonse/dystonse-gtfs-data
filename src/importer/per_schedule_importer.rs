use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike, Duration};
use gtfs_rt::FeedMessage as GtfsRealtimeMessage;
use gtfs_structures::{Gtfs, StopTime};
use gtfs_structures::Trip as ScheduleTrip;
use mysql::*;
use prost::Message; // need to use this, otherwise GtfsRealtimeMessage won't have a `decode` method
use simple_error::bail;
use std::fs::File;
use std::io::prelude::*;
use mysql::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use rayon::prelude::*;

use super::batched_statements::BatchedStatements;
use super::Importer;
use crate::types::PredictionResult;

use crate::{FnResult, OrError};
use crate::types::{EventType, GetByEventType, PredictionBasis};
use crate::predictor::Predictor;
use dystonse_curves::Curve;

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
    predictions_statements: Option<BatchedStatements>,
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
            predictions_statements: None,
            perform_record: importer.args.is_present("record"),
            perform_predict: importer.args.is_present("predict"),
            predictor: None,
            current_prediction_basis: Mutex::new(HashMap::new())
        };

        if instance.perform_record {
            instance.init_record_statements()?;
        }
        if instance.perform_predict {
            instance.predictor = Some(Predictor::new(importer.main, &importer.main.args));
            instance.init_predictions_statements()?;
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
        println!("Processing {} entitites in prallel.", message.entity.len());
        let (success, total) = message.entity.par_iter().map(
            |entity| {
                if let Some(trip_update) = &entity.trip_update {
                    match self.process_trip_update(trip_update, time_of_recording) {
                        Ok(()) => (1, 1),
                        Err(e) => {
                            println!("Error in process_trip_update: {}", e);
                            (0, 1)
                        }
                    }
                } else {
                    (0, 0)
                }
            }
        ).reduce(
            || (0, 0),
            |(a_s, a_t), (b_s, b_t)| (a_s + b_s, a_t + b_t),
        );
        println!("Finished message, {} of {} successful.", success, total);

        if self.perform_record {
            self.record_statements.as_ref().unwrap().write_to_database()?;
        }
        if self.perform_predict {
            self.predictions_statements.as_ref().unwrap().write_to_database()?;
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
        if self.perform_predict && !prediction_done {
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
                delay_departure: departure.delay
            };
            let vehicle_id = VehicleIdentifier {
                trip_id: trip_id.clone(),
                start_date: start_date,
                start_time: start_time
            };

            {
                let cpr = self.current_prediction_basis.lock().unwrap();

                // check if we already made a prediction for this vehicle, and if, what was the basis
                if let Some(previous_basis) = cpr.get(&vehicle_id) {
                    // if we used the same basis, no need to do the same prediction again
                    if *previous_basis == basis {
                        return Ok(());
                    }
                }
            }
            let mut actual_success = false;

            for stop_time in &schedule_trip.stop_times {
                if stop_time.stop_sequence as u32 > stop_sequence {
                    for event_type in &EventType::TYPES {
                        match self.make_prediction(
                            route_id,
                            &vehicle_id,
                            basis.clone(),
                            stop_time,
                            **event_type
                        ) {
                            Ok(()) => actual_success = true,
                            Err(e) => println!("Prediction error: {}", e)
                        }
                    }
                }
            }
            if actual_success {
                let mut cpr = self.current_prediction_basis.lock().unwrap();
                cpr.insert(vehicle_id, basis.clone());
            }
        }

        Ok(())
    }

    fn make_prediction(
        &self,
        route_id: &String,
        vehicle_id: &VehicleIdentifier,
        actual_begin: PredictionBasis,
        scheduled_end: &StopTime,
        event_type: EventType,
    ) -> FnResult<()> {
        let arrival_prediction = self.predictor.as_ref().unwrap().predict(
            &route_id,
            &vehicle_id.trip_id, 
            &Some(actual_begin),
            &scheduled_end.stop.id, 
            scheduled_end.stop_sequence,
            event_type, 
            NaiveDateTime::from_timestamp(scheduled_end.departure_time.unwrap() as i64, 0))?;
            
        let prediction_type = arrival_prediction.to_type_int();
        let curve : Box<dyn Curve> = match arrival_prediction {
            PredictionResult::General(curve) => curve,
            PredictionResult::SpecificCurve(curve) => curve,
            _ => bail!("Result of unexpected type, can't write to DB!")
        };

        let schedules_event_time = match event_type {
            EventType::Arrival => scheduled_end.arrival_time,
            EventType::Departure => scheduled_end.departure_time
        }.unwrap() as i32;

        let prediction_min = Self::date_and_time(&vehicle_id.start_date, schedules_event_time + curve.min_x() as i32);
        let prediction_max = Self::date_and_time(&vehicle_id.start_date, schedules_event_time + curve.max_x() as i32);
        
        self.predictions_statements.as_ref().unwrap().add_paramter_set(Params::from(params! {
            "event_type" => event_type.to_int(),
            "stop_id" => scheduled_end.stop.id.clone(),
            prediction_min,
            prediction_max,
            route_id,
            "trip_id" => vehicle_id.trip_id.clone(),
            "trip_start_date" => vehicle_id.start_date,
            "trip_start_time" => vehicle_id.start_time,
            "stop_sequence" => scheduled_end.stop_sequence,
            "prediction_type" => prediction_type,
            "prediction_curve" => curve.serialize_compact_limited(120)
        }))?;
        Ok(())
    }

    fn date_and_time(date: &NaiveDate, time: i32) -> NaiveDateTime {
        const SECONDS_PER_DAY: i32 = 24 * 60 * 60;
        let extra_days = time / SECONDS_PER_DAY;
        let actual_time = time - extra_days * SECONDS_PER_DAY;
        assert!(actual_time >= 0);
        assert!(actual_time <= SECONDS_PER_DAY);
        let actual_date = *date + Duration::days(extra_days as i64);
        return actual_date.and_time(NaiveTime::from_num_seconds_from_midnight(actual_time as u32, 0));
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

    fn init_predictions_statements(&mut self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        let update_statement = conn.prep(r"UPDATE `predictions`
        SET 
            `stop_id` = :stop_id,
            `prediction_min` = :prediction_min,
            `prediction_max` = :prediction_max,
            `prediction_type` = :prediction_type,
            `prediction_curve` = :prediction_curve
            WHERE
            `event_type` = :event_type AND
            `stop_sequence` = :stop_sequence AND
            `route_id` = :route_id AND
            `trip_id` = :trip_id AND
            `trip_start_date` = :trip_start_date AND
            `trip_start_time` = :trip_start_time;").expect("Could not prepare update statement"); // Should never happen because of hard-coded statement string

        let insert_statement = conn.prep(r"INSERT IGNORE INTO `predictions` (
            `event_type`,
            `stop_id`,
            `prediction_min`,
            `prediction_max`,
            `route_id`,
            `trip_id`,
            `trip_start_date`,
            `trip_start_time`,
            `stop_sequence`,
            `prediction_type`,
            `prediction_curve`
        ) VALUES ( 
            :event_type,
            :stop_id,
            :prediction_min,
            :prediction_max,
            :route_id,
            :trip_id,
            :trip_start_date,
            :trip_start_time,
            :stop_sequence,
            :prediction_type,
            :prediction_curve
        );")
        .expect("Could not prepare insert statement"); // Should never happen because of hard-coded statement string

        // TODO: update where old.time_of_recording < new.time_of_recording...; INSERT IGNORE...;
        self.predictions_statements = Some(BatchedStatements::new(conn, vec![update_statement, insert_statement]));
        Ok(())
    }
}