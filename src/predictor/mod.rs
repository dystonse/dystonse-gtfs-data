use crate::types::{EventType, TimeSlot, RouteSection, PredictionResult, DelayStatistics};

use chrono::{DateTime, Local, NaiveDateTime};
use chrono::offset::TimeZone;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::{Gtfs, Trip};
use std::str::FromStr;

use simple_error::bail;

use crate::{Main, FileCache, FnResult, OrError};

use std::sync::Arc;

use crate::types::{PredictionBasis, DefaultCurveKey, PrecisionType, CurveData, CurveSetKey};

mod real_time;

pub struct Predictor<'a> {
    #[allow(dead_code)]
    pub main: &'a Main,
    pub args: &'a ArgMatches,
    pub schedule: Arc<Gtfs>,
    pub delay_statistics: Arc<DelayStatistics>,
}

impl<'a> Predictor<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("predict").about("Looks up delay predictions from the statistics for a specified event.")
            .subcommand(App::new("start")
                .about("Starts the predictor module and keeps running so it can answer requests for predictions.")
            )
            .subcommand(App::new("single")
                .about("Starts the predictor module and answers one request for a prediction, then quits.")
                .arg(Arg::new("route-id")
                    .short('r')
                    .long("route-id")
                    .required(true)
                    .about("Id of the route for which the prediction shall be made.")
                    .takes_value(true)
                    .value_name("ROUTE_ID")
                ).arg(Arg::new("trip-id")
                    .short('t')
                    .long("trip-id")
                    .required(true)
                    .about("Id of the trip for which the prediction shall be made.")
                    .takes_value(true)
                    .value_name("TRIP_ID")
                ).arg(Arg::new("stop-sequence")
                    .short('s')
                    .long("stop-sequence")
                    .about("Sequence number of the stop for which the prediction shall be made. May be ommitted to get predictions for all stops of the route.")
                    .takes_value(true)
                    .value_name("STOP_SEQUENCE")
                ).arg(Arg::new("event-type")
                    .short('e')
                    .long("event-type")
                    .required(true)
                    .about("Event type (arrival or departure) for which the prediction shall be made.")
                    .takes_value(true)
                    .value_name("EVENT_TYPE")
                ).arg(Arg::new("date-time")
                    .short('d')
                    .long("date-time")
                    .required(true)
                    .about("Date and time YYYY-MM-DDThh:mm:ss in UTC for which the prediction shall be made.")
                    .takes_value(true)
                    .value_name("DATE_TIME")
                ).arg(Arg::new("start-stop-id")
                    .long("start-stop-id")
                    .required(false)
                    .about("Id of a stop in the past from which the vehicle started with initial-delay.")
                    .takes_value(true)
                    .value_name("START_STOP_ID")
                ).arg(Arg::new("initial-delay")
                    .long("initial-delay")
                    .required(false)
                    .about("delay (in seconds) of departure from the start-stop.")
                    .takes_value(true)
                    .value_name("INITIAL_DELAY")
                ).arg(Arg::new("use-realtime")
                    .short('u')
                    .long("use-realtime")
                    .required(false)
                    .about("Try to get the most recent realtime update for the given trip.")
                    .takes_value(false)
                )
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> FnResult<Predictor<'a>> {
        Ok(Predictor {
            main,
            args,
            schedule: main.get_schedule()?,
            delay_statistics: FileCache::get_cached_simple(&main.statistics_cache, &format!("{}/all_curves.exp", main.dir)).or_error("No delay statistics (all_curves.exp) found.")?,
        })
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("start", Some(sub_args)) => self.run_start(sub_args),
            ("single", Some(sub_args)) => self.run_single(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }

    /// keeps running and answering requests for predictions until stopped
    fn run_start(&self, _args: &ArgMatches) -> FnResult<()> {
        //TODO: everything !!!

        Ok(())
    }

    /// looks up one prediction and then returns
    fn run_single(&self, args: &ArgMatches) -> FnResult<()> {

        // parse command line arguments into the right data types
        let route_id = args.value_of("route-id").unwrap();
        let trip_id = args.value_of("trip-id").unwrap();
        let potential_stop_sequence : Option<u16> = match args.value_of("stop-sequence") {
            None => None,
            Some(sss) => Some(str::parse::<u16>(sss)?)
        };
        let event_type = match args.value_of("event-type").unwrap() {
            "arrival" => EventType::Arrival,
            "departure" => EventType::Departure,
            _ => {panic!("Invalid event type argument!");}
        };
        let date_time = Local.from_local_datetime(&NaiveDateTime::parse_from_str(args.value_of("date-time").unwrap(), "%Y-%m-%dT%H:%M:%S")?).unwrap();

        let trip = self.schedule.get_trip(trip_id)?;

        // parse optional arguments:
        let start = match args.value_of("start-stop-id") {
            Some(s) => match args.value_of("initial-delay") {
                            Some(d) => Some (PredictionBasis {stop_id: s.to_string(), delay_departure: Some(i64::from_str(d).unwrap())}),
                            None => Some(PredictionBasis {stop_id: s.to_string(), delay_departure: None}),
                        },
            None => {
                // TODO move or delete everything related to db access for realtime data
                if args.is_present("use-realtime") {
                    match real_time::get_realtime_data(self.main, &trip) {
                        Ok((stop_id, delay)) => Some(PredictionBasis{ stop_id: stop_id.clone(), delay_departure: Some(delay as i64)}),
                        _ => None
                    }
                } else {
                    None
                }
            },
        };

        // if no single stop_sequence is given, iterate over all stop_sequences of the trip
        // TODO we currently ignore the stop_id from the args
        let stop_sequences : Vec<u16> = match potential_stop_sequence {
            Some(stop_sequence) => vec!{stop_sequence},
            None => trip.stop_times.iter().map(|st| st.stop_sequence).collect()
        };

        for stop_sequence in stop_sequences {
            let stop_id = &trip.get_stop_time_by_sequence(stop_sequence)?.stop.id;
            // data structure to hold the prediction result:
            let prediction = self.predict(route_id, trip_id, &start, stop_sequence, event_type, date_time);

            // output the resulting curve(s) to the command line:
            // TODO: we could probably use more advanced kinds of output here
            println!("prediction of {:?} delay at stop {} for route {}, trip {} on {:?}:", event_type, stop_id, route_id, trip_id, date_time);
            println!("{:?}", prediction);
        }

        Ok(())
    }


    /// finds out which kind of curve can be used for this prediction and looks up the requested curve
    pub fn predict(&self, 
            route_id: &str, 
            trip_id: &str, 
            start: &Option<PredictionBasis>, 
            stop_sequence: u16,
            et: EventType, 
            date_time: DateTime<Local>) -> FnResult<PredictionResult> {

        // parse lookup parameters from input
        let ts = TimeSlot::from_datetime(date_time);
        let trip = self.schedule.get_trip(trip_id)?;
       
        let route_variant : u64 = u64::from_str(trip.route_variant.as_ref().unwrap()).unwrap(); 
        // should never panic because we already checked the validity of 
        // the trip, and route variants are always numbers.

        // try to find a specific prediction:
        let specific_prediction = self.predict_specific(route_id, route_variant, start, stop_sequence, ts, et, &trip);

        // unwrap that, or try a default prediction if it failed:
        specific_prediction.or_else(|_| {
            // eprintln!("⚠️ No specific_prediction because: {}", e);

            // prepare some more lookup parameters
            let key = DefaultCurveKey {
                route_type: self.schedule.get_route(route_id)?.route_type,
                route_section: RouteSection::get_route_section_by_stop_sequence(&self.schedule, trip_id, stop_sequence)?,
                time_slot: ts.clone(),
                event_type: et
            };
            self.predict_default(key)
        })
    }

    // looks up a curve from default curves and returns it
    fn predict_default(&self, key: DefaultCurveKey) // rt: RouteType, rs: RouteSection, ts: &TimeSlot, et: EventType) 
            -> FnResult<PredictionResult> {

        let potential_curve_data = self.delay_statistics.general.all_default_curves.get(&key);
        
        if let Some(curve_data) = potential_curve_data {
            Ok(PredictionResult::CurveData(curve_data.clone()))
        } else {
            // Once we hat the problem that default curves could not be found even though they existed.
            // The following code helps to debug this, in case it happens again. You also need this:
            use std::hash::{Hash, Hasher};
            use std::collections::hash_map::DefaultHasher;

            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            println!("No default curve found for {:?} with hash {}.", key, hasher.finish());
            // for (p_key, _p_val) in &self.delay_statistics.general.all_default_curves {
            //     let mut hasher = DefaultHasher::new();
            //     p_key.hash(&mut hasher);
            //     println!("Instead, found key {:?} with hash {}.", p_key, hasher.finish());
            // }

            bail!("No default curve.");
        }
        
    }

    // looks up a curve (or curve set) from specific curves and returns it
    fn predict_specific(&self, 
            route_id: &str, 
            route_variant: u64, 
            start: &Option<PredictionBasis>, //&str for stop_id, f32 for initial delay
            stop_sequence: u16, 
            ts: &TimeSlot,
            et: EventType,
            trip: &Trip) -> FnResult<PredictionResult> {

        // find the route variant data that we need:
        let rvdata = &self.delay_statistics.specific.get(route_id).or_error("No specific statistics for route_id")?.variants.get(&route_variant).or_error("No specific statistics for route_variant")?;
        // find index of target stop:
        // TODO use stop_sequence instead of stop_id, which has less chance of failure since it's always unique
        let end_stop_index = trip.get_stop_index_by_stop_sequence(stop_sequence)? as u32;
        
        match start {
            None => { 
                // get general curve for target stop (a.k.a. SemiSpecific):
                let curve_data = rvdata.general_delay[et].get(&end_stop_index).or_error(&format!("No curve_data for stop_sequence {}.", stop_sequence))?;
                return Ok(PredictionResult::CurveData(curve_data.clone()));
            },
            Some(actual_start) => {
                // TODO use stop_sequence instead of stop_id, which has less chance of failure since it's always unique
                let start_stop_index = trip.get_stop_index_by_id(&actual_start.stop_id)? as u32;
                let key = CurveSetKey {
                    start_stop_index,
                    end_stop_index,
                    time_slot: ts.clone()
                };
                let potential_curveset_data = &rvdata.curve_sets[et].get(&key);
                // let route_name = &self.schedule.get_route(route_id).unwrap().short_name;
                let curve_set_data = match potential_curveset_data {
                    Some(data) => *data,
                    None => {
                        if *ts == TimeSlot::DEFAULT {
                            // println!("No specific curveset found for route {}, key {:?}", route_name, key);
                            // println!("Present Keys: {:?}", rvdata.curve_sets[et].keys());
                            bail!("No specific curveset found");
                        } else {
                            // println!("No specific curveset with specific TimeSlot found for route {}, key {:?}. Using TimeSlot::DEFAULT instead.", route_name, key);
                            return self.predict_specific(route_id, route_variant, start, stop_sequence, &TimeSlot::DEFAULT, et, trip);
                        }
                    }
                }; 
                if curve_set_data.curve_set.curves.is_empty() {
                    bail!("Found specific curveset, but it was empty.");
                }
                match actual_start.delay_departure {
                    // get curve set for start-stop:
                    None => {
                        return Ok(PredictionResult::CurveSetData(curve_set_data.clone()));
                    },
                    // get curve for start-stop and initial delay:
                    Some(delay) => {
                        let curve = curve_set_data.curve_set.curve_at_x_with_continuation(delay as f32);
                        let curve_data = CurveData {
                            curve,
                            precision_type: if *ts == TimeSlot::DEFAULT { PrecisionType::FallbackSpecific } else { PrecisionType::Specific },
                            sample_size: curve_set_data.sample_size
                        };
                        return Ok(PredictionResult::CurveData(curve_data));
                    }
                };
            },
        };
    }
}
