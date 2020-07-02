use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use gtfs_rt::FeedMessage as GtfsRealtimeMessage;
use gtfs_structures::Gtfs;
use gtfs_structures::Trip as ScheduleTrip;
use mysql::*;
use prost::Message; // need to use this, otherwise GtfsRealtimeMessage won't have a `decode` method
use simple_error::{SimpleError, bail};
use std::fs::File;
use std::io::prelude::*;
use mysql::prelude::*;

use super::batched_statements::BatchedStatements;
use super::Importer;

use crate::{FnResult, OrError};
use crate::types::{EventType, GetByEventType};

pub struct PerScheduleImporter<'a> {
    importer: &'a Importer<'a>,
    gtfs_schedule: &'a Gtfs,
    verbose: bool,
    filename: &'a str,
    record_statements: Option<BatchedStatements>,
    arrival_statements: Option<BatchedStatements>,
    departure_statements: Option<BatchedStatements>,
    perform_record: bool,
    perform_predict: bool
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
        gtfs_schedule: &'a Gtfs,
        importer: &'a Importer,
        verbose: bool,
        filename: &'a str,
    ) -> FnResult<PerScheduleImporter<'a>> {
        let mut instance = PerScheduleImporter {
            gtfs_schedule,
            importer,
            verbose,
            filename,
            record_statements: None,
            arrival_statements: None,
            departure_statements: None,
            perform_record: importer.args.is_present("record"),
            perform_predict: importer.args.is_present("predict"),
        };

        if instance.perform_record {
            instance.init_record_statements()?;
        }
        if instance.perform_predict {
            instance.init_arrival_statements()?;
            instance.init_departure_statements()?;
        }

        Ok(instance)
    }

    pub fn handle_realtime_file(&self, path: &str) -> FnResult<()> {
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
        let time_of_recording = message.header.timestamp.ok_or(SimpleError::new(
            "No global timestamp in realtime data, skipping.",
        ))?;

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
            self.record_statements.as_ref().unwrap().write_to_database()?;
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

        let start_date = if let Some(datestring) = &realtime_trip.start_date {
            NaiveDate::parse_from_str(&datestring, "%Y%m%d")?.and_hms(0, 0, 0)
        } else {
            bail!("Trip without start date. Skipping.");
        };

        let realtime_schedule_start_time = NaiveTime::parse_from_str(&realtime_trip.start_time.as_ref().or_error("Trip without start time. Skipping.")?, "%H:%M:%S")?;

        let schedule_trip = self.gtfs_schedule.get_trip(&trip_id).or_error(&format!("Did not find trip {} in schedule. Skipping.", trip_id))?;

        let schedule_start_time = schedule_trip.stop_times[0].departure_time;
        let time_difference =
            realtime_schedule_start_time.num_seconds_from_midnight() as i32 - schedule_start_time.unwrap() as i32;
        if time_difference != 0 {
            eprintln!("Trip {} has a difference of {} seconds between scheduled start times in schedule data and realtime data.", trip_id, time_difference);
        }

        for stop_time_update in &trip_update.stop_time_update {
            self.process_stop_time_update(
                stop_time_update,
                start_date,
                schedule_trip,
                &trip_id,
                &route_id,
                time_of_recording,
            )?;
        }

        Ok(())
    }

    fn process_stop_time_update(
        &self,
        stop_time_update: &gtfs_rt::trip_update::StopTimeUpdate,
        start_date: NaiveDateTime,
        schedule_trip: &gtfs_structures::Trip,
        trip_id: &String,
        route_id: &String,
        time_of_recording: u64,
    ) -> FnResult<u32> {
        let stop_id = &stop_time_update.stop_id.as_ref().or_error("no stop_id")?;
        let stop_sequence = stop_time_update.stop_sequence.or_error("no stop_sequence")?;

        let arrival = PerScheduleImporter::get_event_times(
            stop_time_update.arrival.as_ref(),
            start_date,
            EventType::Arrival,
            &schedule_trip,
            stop_sequence,
        );
        let departure = PerScheduleImporter::get_event_times(
            stop_time_update.departure.as_ref(),
            start_date,
            EventType::Departure,
            &schedule_trip,
            stop_sequence,
        );

        if arrival.is_empty() && departure.is_empty() {
            return Ok(0);
        }

        if self.perform_record {
            self.record_statements.as_ref().unwrap().add_paramter_set(Params::from(params! {
                "source" => &self.importer.main.source,
                "route_id" => &route_id,
                "route_variant" => &schedule_trip.route_variant.as_ref().ok_or(SimpleError::new("no route variant"))?,
                "trip_id" => &trip_id,
                "date" => start_date,
                stop_sequence,
                stop_id,
                time_of_recording,
                "delay_arrival" => arrival.delay,
                "delay_departure" => departure.delay,
                "schedule_file_name" => self.filename
            }))?;
        }

        Ok(1)
    }

    fn get_event_times(
        event: Option<&gtfs_rt::trip_update::StopTimeEvent>,
        start_date: NaiveDateTime,
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
        let schedule = start_date.timestamp() + event_time.expect("no arrival time") as i64;
        let estimate = schedule + delay;

        EventTimes {
            delay: Some(delay),
            schedule: Some(schedule),
            estimate: Some(estimate),
        }
    }

    fn init_record_statements(&mut self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        let update_statement = conn.prep(r"UPDATE `realtime`
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

        
        let insert_statement = conn.prep(r"INSERT IGNORE INTO `realtime` (
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