use chrono::{NaiveDate, Duration, Local, DateTime};
use chrono::offset::TimeZone;
use gtfs_structures::{Gtfs, Trip};
use std::sync::Arc;
use mysql::*;
use mysql::prelude::*;

use super::{Importer, VehicleIdentifier};
use super::MAX_ESTIMATED_TRIP_DURATION;
use super::batched_statements::BatchedStatements;
use crate::{FnResult, date_and_time_local};
use crate::types::{OriginType, EventType, PredictionResult, GtfsDateTime};
use crate::types::CurveData;
use crate::predictor::Predictor;
use dystonse_curves::Curve;

/// This imports predictions to the database that are based on schedule data
/// and can be generated for any trip, regardless of realtime data availability
pub struct ScheduledPredictionsImporter<'a> {
    importer: &'a Importer<'a>,
    gtfs_schedule: Arc<Gtfs>,
    verbose: bool,
    predictor: Predictor<'a>,
    predictions_statements: Option<BatchedStatements>,
}

lazy_static!{
    // For how many days in the future we want to prepare predictions:
    static ref PREDICTION_BUFFER_SIZE : Duration = Duration::days(7) + Duration::hours(12);

    // How many minutes of scheduled predictions we want to compute in one iteration,
    // before we try to process the next batch of realtime updates:
    static ref PREDICTION_MIN_BATCH_DURATION : Duration = Duration::minutes(6);

    // Minimum number of trips for which predictions will be made during one batch.
    // The time range will be extended until this number of trips is found.
    // Don't set this const below 1 or predictions may stall forever.
    static ref PREDICTION_MIN_BATCH_COUNT : usize = 1000;

    // How long we pause scheduled scheduled predictions when we reached
    // the end of the PREDICTION_BUFFER_SIZE
    static ref PREDICTION_FULL_TIMEOUT : Duration = Duration::minutes(20);
}

impl<'a> ScheduledPredictionsImporter<'a> {
    
    pub fn new(
        importer: &'a Importer,
        verbose: bool
    ) -> FnResult<ScheduledPredictionsImporter<'a>> {
        let mut instance = ScheduledPredictionsImporter {
            importer,
            gtfs_schedule: importer.main.get_schedule()?,
            verbose,
            predictor: Predictor::new(importer.main, &importer.main.args)?,
            predictions_statements: None,
        };
        instance.init_predictions_statements()?;
        Ok(instance)
    }

    pub fn make_scheduled_predictions(&mut self) -> FnResult<()> {
        { //block for mutex
            let mut until_option = self.importer.timeout_until.lock().unwrap();
            if let Some(until) = *until_option {
                if Local::now() < until {
                    println!("Skipping scheduled prediction because of timeout until {}.", until);
                    return Ok(());
                } else {
                    println!("Reached end of timeout.");
                    *until_option = None;
                }
            }
        }

        // we use absolute timestamps of scheduled trip start times to track
        // which is the latest trip for which we already have schedule-based
        // predictions
        let initial_begin = self.get_latest_prediction_time_from_database()?;

        // compute the time span for which predictions shall be made in this iteration:
        let mut begin = initial_begin; 

        // this is the absolute time limit. Predictions shall never be made for
        // trips which start after this time.
        let time_limit = Local::now() + *PREDICTION_BUFFER_SIZE;

        let mut end = if begin >= (time_limit - *PREDICTION_MIN_BATCH_DURATION) {
            { //block for mutex
                let mut until_option = self.importer.timeout_until.lock().unwrap();
                *until_option = Some(Local::now() + *PREDICTION_FULL_TIMEOUT);
            }
            println!("Prediction buffer will be full after this iteration, setting timeout.");
            time_limit
        } else {
            begin + *PREDICTION_MIN_BATCH_DURATION
        };

        // Now things get complicated. Trip start times may be larger than 23:59:59,
        // in fact there are good reasons to use times up to 27:00:00, see
        // https://gist.github.com/derhuerst/574edc94981a21ef0ce90713f1cff7f6
        // So we have to assume that for any given absolute datetime, trips
        // may start at (date + time) but also on ((date - 1 day) + (time + 24:00:00)).
        // We must use schedule for both dates to find the relevant trips.


        // Get all trips that are scheduled for the selected dates.
        // We use end.date() instead of begin.date() because it ensures
        // proper handling of time spans across midnight.
        let mut current_day = end.date();
        let mut previous_day = end.date() - Duration::days(1);

        let mut current_day_trips : Vec<&Trip> = self.gtfs_schedule.trips_for_date(current_day.naive_local())?;
        let mut previous_day_trips : Vec<&Trip> = self.gtfs_schedule.trips_for_date(previous_day.naive_local())?;

        // collect trips for which we want to make predictions during this batach in this vec:
        let mut trip_selection : Vec<(GtfsDateTime, &Trip)> = Vec::new();

        loop {
            for trip in &current_day_trips {
                if let Some(start_time) = trip.stop_times[0].departure_time {
                    let start_time = GtfsDateTime::new(current_day, start_time as i32);
                    let absolute_start_time = start_time.date_time();
                    if absolute_start_time > begin && absolute_start_time <= end {
                        trip_selection.push((start_time, trip));
                    }
                }
            };
            for trip in &previous_day_trips {
                if let Some(start_time) = trip.stop_times[0].departure_time {
                    let start_time = GtfsDateTime::new(previous_day, start_time as i32);
                    let absolute_start_time = start_time.date_time();
                    if absolute_start_time > begin && absolute_start_time <= end {
                        trip_selection.push((start_time, trip));
                    }
                }
            };

            // It may happen that the initial time span contains no trips at all. In this case, the
            // predictions would never move on, as get_latest_prediction_time_from_database would
            // always return the same time. Also, if the span contains at least one trip, but only
            // a very small number, we extend the range to advance our predictions more quickly.
            if trip_selection.len() < *PREDICTION_MIN_BATCH_COUNT {
                if self.verbose {
                    println!("Only {} trips found in total after adding trips between {} and {}, extending range…", trip_selection.len(), begin, end);
                }
                begin = end;
                end = end + *PREDICTION_MIN_BATCH_DURATION;

                if begin > time_limit {
                    // in this case, stop extending the range, no matter how few trips will be added.
                    // we are simply done for the moment.
                    break;
                }

                // if the new range begins on another date - that is, we moved past midnight - we need to rebuild the trip collections
                if end.date() == current_day + Duration::days(1) {
                    current_day = end.date();
                    previous_day = end.date() - Duration::days(1);
                    previous_day_trips = current_day_trips; // we can reuse the selected trips, as the old today is the new yesterday
                    current_day_trips = self.gtfs_schedule.trips_for_date(current_day.naive_local())?;
                }
                if end.date() != current_day {
                    println!("end.date() is {} and current_day is {}, which is an invalid state.", end.date(), current_day);
                }
            } else {
                break;
            }
        }

        if trip_selection.len() == 0 {
            if self.verbose {
                println!("No more schedule-based predictions to make.");
            }
            return Ok(());
        }

        if self.verbose {
            println!("Making schedule-based predictions for {} trips starting between {} and {}.", trip_selection.len(), initial_begin, end);
        }

        // make predictions for all stops of those trips
        for (start_time, trip) in trip_selection {
            let route_id = &trip.route_id;
            let vehicle_id = VehicleIdentifier {
                trip_id: trip.id.clone(), 
                start: start_time,
            };
            for st in &trip.stop_times {
                for et in &EventType::TYPES {
                    if let Some(scheduled_time) = et.get_time_from_stop_time(&st) {
                        // try to make a prediction:
                        let result = self.predictor.predict(&trip.route_id, &trip.id, &None, st.stop_sequence, **et, begin);
                        match result {
                            Ok(PredictionResult::CurveData(c)) => {
                                let result = self.save_scheduled_prediction_to_database(c, **et, st.stop.id.clone(), st.stop_sequence, 
                                    scheduled_time, vehicle_id.clone(), route_id.to_string());
                                if let Err(e) = result {
                                    eprintln!("Error while saving scheduled predictions to database: {}", e);
                                }
                            },
                            Ok(PredictionResult::CurveSetData(_cs)) => { 
                                eprintln!("Error while trying to predict {:?} at stop {} of trip {}: result should be a Curve but is a CurveSet.",
                                **et, st.stop_sequence, trip.id);
                            },
                            Err(e) => {
                               eprintln!("Error while trying to predict {:?} at stop {} of trip {}: {}",
                                 **et, st.stop_sequence, trip.id, e);
                            }
                        };
                    } else {
                        // skip empty arrival/departure times
                        if self.verbose {
                            println!("(Scheduled predictions:) No {:?} scheduled at stop {} of trip {}. Skipping {:?} prediction.",
                                 **et, st.stop_sequence, trip.id, **et);
                        }
                    }
                }
            }
        }
        self.predictions_statements.as_ref().unwrap().write_to_database()?;

        let latest_prediction = self.get_latest_prediction_time_from_database()?;
        if latest_prediction > end {
            eprintln!("WARNING: latest prediction is {}, should not be later than {}", latest_prediction, end);
        } else {
            println!("Wrote predictions until {}.", latest_prediction);
        }

        Ok(())
    }

    // saves a given schedule-based prediction into the database
    fn save_scheduled_prediction_to_database(
        &self,
        curve_data: CurveData,
        et: EventType, 
        stop_id: String, 
        stop_sequence: u16,
        scheduled_time: i32,
        vehicle_id: VehicleIdentifier,
        route_id: String
    ) -> FnResult<()> {

        let prediction_min = date_and_time_local(&vehicle_id.start.service_day(), scheduled_time + curve_data.curve.min_x() as i32);
        let prediction_max = date_and_time_local(&vehicle_id.start.service_day(), scheduled_time + curve_data.curve.max_x() as i32);
        
        self.predictions_statements.as_ref().unwrap().add_parameter_set(Params::from(params! {
            "source" => self.importer.main.source.clone(),
            "event_type" => et.to_int(),
            "stop_id" => stop_id.clone(),
            "prediction_min" => prediction_min.naive_local(),
            "prediction_max" => prediction_max.naive_local(),
            route_id,
            "trip_id" => vehicle_id.trip_id.clone(),
            "trip_start_date" => vehicle_id.start.date().naive_local(),
            "trip_start_time" => vehicle_id.start.duration(),
            stop_sequence,
            "precision_type" => curve_data.precision_type.to_int(),
            "origin_type" => OriginType::Schedule.to_int(),
            "sample_size" => curve_data.sample_size,
            "prediction_curve" => curve_data.curve.serialize_compact_limited(120)
        }))?;
        
        Ok(())
    }

    // this helps us find the point from where we want to start/continue making predictions
    fn get_latest_prediction_time_from_database(&self) -> FnResult<DateTime<Local>> {

        let mut conn = self.importer.main.pool.get_conn()?;
        
        let select_statement = conn.prep(r"SELECT `trip_start_date`,`trip_start_time` 
            FROM `predictions` WHERE `origin_type` = :origin_type AND `source` = :source
            ORDER BY trip_start_date + INTERVAL TIME_TO_SEC(trip_start_time) SECOND DESC 
            LIMIT 1,1;").expect("Could not prepare select statement");
 
        let query_result : Option<(NaiveDate, Duration)> = conn.exec_first(select_statement, 
            params!{"source" => self.importer.main.source.clone(), "origin_type" => OriginType::Schedule.to_int()})?; 
            //actual errors will be thrown here if they occur
        if let Some((date, duration)) = query_result {
            return Ok(GtfsDateTime::new(Local.from_local_date(&date).unwrap(), duration.num_seconds() as i32).date_time());
        } else {
            // if there aren't any scheduled predictions in the database yet 
            // (this is not an error and can happen when we start),
            // we will probably want to start predicting for trips from the near past:
            return Ok(Local::now() - *MAX_ESTIMATED_TRIP_DURATION);
        }
    }

    fn init_predictions_statements(&mut self) -> FnResult<()> {
        let mut conn = self.importer.main.pool.get_conn()?;
        let insert_statement = conn.prep(r"INSERT IGNORE INTO `predictions` (
            `source`,
            `event_type`,
            `stop_id`,
            `prediction_min`,
            `prediction_max`,
            `route_id`,
            `trip_id`,
            `trip_start_date`,
            `trip_start_time`,
            `stop_sequence`,
            `precision_type`,
            `origin_type`,
            `sample_size`,
            `prediction_curve`
        ) VALUES ( 
            :source,
            :event_type,
            :stop_id,
            :prediction_min,
            :prediction_max,
            :route_id,
            :trip_id,
            :trip_start_date,
            :trip_start_time,
            :stop_sequence,
            :precision_type,
            :origin_type,
            :sample_size,
            :prediction_curve
        );")
        .expect("Could not prepare insert statement"); // Should never happen because of hard-coded statement string

        self.predictions_statements = Some(BatchedStatements::new("scheduled predictions", conn, vec![insert_statement]));
        Ok(())
    }
}