use chrono::{NaiveDateTime, NaiveDate, NaiveTime, Duration, Utc, Timelike};
use gtfs_structures::{Gtfs, Trip};
use std::sync::Arc;
use mysql::*;
use mysql::prelude::*;

use super::{Importer, VehicleIdentifier};
use super::MAX_ESTIMATED_TRIP_DURATION;
use super::batched_statements::BatchedStatements;
use crate::{FnResult, date_and_time};
use crate::types::{OriginType, EventType, PredictionResult};
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
    static ref PREDICTION_BUFFER_SIZE : Duration = Duration::days(7);
    // How many minutes of scheduled predictions we want to compute in one iteration,
    // before we try to process the next batch of realtime updates:
    static ref PREDICTION_BATCH_SIZE : Duration = Duration::minutes(10); 
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

    pub fn make_scheduled_predictions(&self) -> FnResult<()> {
        // compute the time span for which predictions shall be made in this iteration:
        let initial_begin = self.get_latest_prediction_time_from_database()?;
        let mut begin = initial_begin; 
        let time_limit = Utc::now().naive_utc() + *PREDICTION_BUFFER_SIZE; 
        let mut end = if begin >= (time_limit - *PREDICTION_BATCH_SIZE) {
            time_limit
        } else {
            begin + *PREDICTION_BATCH_SIZE
        };

        // get all trips that are scheduled for the selected date:
        let mut trip_date = begin.date();
        let mut daily_trips : Vec<&Trip> = self.gtfs_schedule.trips_for_date(trip_date)?;


        // find all trips that are scheduled to start in the selected time span
        let mut trip_selection : Vec<&Trip> = Vec::new();

        loop {
            for trip in &daily_trips {
                if let Some(start_time) = trip.stop_times[0].departure_time {
                    if start_time > begin.time().num_seconds_from_midnight() 
                        && start_time <= end.time().num_seconds_from_midnight() {
                            trip_selection.push(trip);
                        }
                }
            };

            // It may happen that the initial time span contains no trips at all. In this case, the
            // predictions would never move on, as get_latest_prediction_time_from_database would
            // always return the same time. Also, if the span contains at least one trip, but only
            // a very small number, we extend the range to advance our predictions more quickly.
            if trip_selection.len() < 50 {
                if self.verbose {
                    println!("Only {} trips starting between {} and {}, extending range…", trip_selection.len(), initial_begin, end);
                }
                begin = end;
                end += *PREDICTION_BATCH_SIZE;

                // if the new range begins on another date - that is, we moved past midnight - we need to rebuild the daily_trips
                if begin.date() != trip_date {
                    trip_date = begin.date();
                    daily_trips = self.gtfs_schedule.trips_for_date(trip_date)?;
                }
            } else {
                break;
            }
        }

        if self.verbose {
            println!("Making schedule-based predictions for {} trips starting between {} and {}.", trip_selection.len(), initial_begin, end);
        }

        // make predictions for all stops of those trips
        for trip in trip_selection {
            let route_id = &trip.route_id;
            let vehicle_id = VehicleIdentifier {
                trip_id: trip.id.clone(), 
                start_date: begin.date(), 
                start_time: NaiveTime::from_num_seconds_from_midnight(trip.stop_times[0].departure_time.unwrap(), 0) };
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

        let prediction_min = date_and_time(&vehicle_id.start_date, scheduled_time + curve_data.curve.min_x() as i32);
        let prediction_max = date_and_time(&vehicle_id.start_date, scheduled_time + curve_data.curve.max_x() as i32);
        
        self.predictions_statements.as_ref().unwrap().add_parameter_set(Params::from(params! {
            "source" => self.importer.main.source.clone(),
            "event_type" => et.to_int(),
            "stop_id" => stop_id.clone(),
            prediction_min,
            prediction_max,
            route_id,
            "trip_id" => vehicle_id.trip_id.clone(),
            "trip_start_date" => vehicle_id.start_date,
            "trip_start_time" => vehicle_id.start_time,
            stop_sequence,
            "precision_type" => curve_data.precision_type.to_int(),
            "origin_type" => OriginType::Schedule.to_int(),
            "sample_size" => curve_data.sample_size,
            "prediction_curve" => curve_data.curve.serialize_compact_limited(120)
        }))?;
        
        Ok(())
    }

    // this helps us find the point from where we want to start/continue making predictions
    fn get_latest_prediction_time_from_database(&self) -> FnResult<NaiveDateTime> {

        let mut conn = self.importer.main.pool.get_conn()?;
        
        let select_statement = conn.prep(r"SELECT `trip_start_date`,`trip_start_time` 
            FROM `predictions` WHERE `origin_type` = :origin_type AND `source` = :source
            ORDER BY `trip_start_date` DESC, `trip_start_time` DESC 
            LIMIT 1,1;").expect("Could not prepare select statement");
 
        let query_result : Option<(NaiveDate, NaiveTime)> = conn.exec_first(select_statement, 
            params!{"source" => self.importer.main.source.clone(), "origin_type" => OriginType::Schedule.to_int()})?; 
            //actual errors will be thrown here if they occur
        if let Some((date, time)) = query_result {
            return Ok(date.and_time(time));
        } else {
            // if there aren't any scheduled predictions in the database yet 
            // (this is not an error and can happen when we start),
            // we will probably want to start predicting for trips from the near past:
            return Ok(Utc::now().naive_utc() - *MAX_ESTIMATED_TRIP_DURATION);
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