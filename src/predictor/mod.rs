use crate::types::{EventType, TimeSlot, RouteSection, PredictionResult, DelayStatistics};

use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::{Gtfs, RouteType};
use mysql::*;
use std::str::FromStr;
use std::convert::TryInto;

use simple_error::SimpleError;

use crate::FnResult;
use crate::Main;

use dystonse_curves::tree::{SerdeFormat, NodeData};

pub struct Predictor<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    data_dir: Option<String>,
    schedule: Gtfs,
    delay_statistics: Box<DelayStatistics>,
}

impl<'a> Predictor<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("predict")
            .subcommand(App::new("start")
                .about("Starts the predictor module and keeps running so it can answer requests for predictions.")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used to look up any static GTFS data.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                )
            )
            .subcommand(App::new("single")
                .about("Starts the predictor module and answers one request for a prediction, then quits.")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used to look up any static GTFS data.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                ).arg(Arg::new("route-id")
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
                ).arg(Arg::new("stop-id")
                    .short('i')
                    .long("stop-id")
                    .required(true)
                    .about("Id of the stop for which the prediction shall be made.")
                    .takes_value(true)
                    .value_name("STOP_ID")
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
                )
            )
            .arg(Arg::new("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .about("The directory which contains schedules and precomputed curves")
                .long_about(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and precomputed curve data (located in a subdirectory named 'curve_data')."
                )
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Predictor<'a> {
        Predictor {
            main,
            args,
            data_dir: Some(String::from(args.value_of("dir").unwrap())),
            schedule: Self::read_schedule(args).unwrap(),
            delay_statistics: Self::read_delay_statistics(args).unwrap(),
        }
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
        let stop_id = args.value_of("stop-id").unwrap();
        let event_type = match args.value_of("event-type").unwrap() {
            "arrival" => EventType::Arrival,
            "departure" => EventType::Departure,
            _ => {panic!("Invalid event type argument!");}
        };
        let date_time = NaiveDateTime::parse_from_str(args.value_of("date-time").unwrap(), "%Y-%m-%dT%H:%M:%S")?;

        // data structure to hold the prediction result:
        let prediction : PredictionResult = self.predict(route_id, trip_id, stop_id, event_type, date_time)?;

        // output the resulting curve(s) to the command line
        // TODO: we could probably use more advanced kinds of output here
        println!("prediction of {:?} delay at stop {} for route {}, trip {} on {:?}:", event_type, stop_id, route_id, trip_id, date_time);
        println!("{:?}", prediction);

        Ok(())
    }
    
    /*
    NOTE/TODO: THE FOLLOWING STUFF SHOULD BE COMPUTED BY WHOEVER CALLS THIS MODULE:
    - find out if we have current realtime data of this trip
    - if yes, find latest stop where we have a delay_departure in the past
    - get that delay and from which stop it is
    - put that into [somewhere we can read it from]
    */

    /// finds out which kind of curve can be used for this prediction and looks up the requested curve
    fn predict(&self, route_id: &str, trip_id: &str, stop_id: &str, et: EventType, date_time: NaiveDateTime) 
        -> FnResult<PredictionResult> {

        // parse lookup parameters from input
        let ts = TimeSlot::from_datetime(date_time);
        let trip = self.schedule.get_trip(trip_id)?;
       
        let route_variant : u64 = u64::from_str(trip.route_variant.as_ref().unwrap()).unwrap(); 
        // should never panic because we already checked the validity of 
        // the trip, and route variants are always numbers.

        let start : Option<(&str, Option<f32>)> = None;
        // TODO: fill this option with start-stop_id and start-delay if given !!!!

        // try to find a specific prediction:
        let specific_prediction = self.predict_specific(route_id, route_variant, start, stop_id, ts, et);

        // unwrap that, or try a default prediction if it failed:
        let prediction : PredictionResult = if specific_prediction.is_err() {
            // prepare some more lookup parameters
            let r = self.schedule.get_route(route_id)?;
            let rt = r.route_type;
            let rs = RouteSection::get_route_section(&self.schedule, trip_id, stop_id);
            // try default prediction
            self.predict_default(rt, rs, ts, et)?
        } else {
            specific_prediction.unwrap() //will not panic because error handling in if branch above
        };

        //return the prediction result
        Ok(prediction)
    }

    // looks up a curve from default curves and returns it
    #[allow(dead_code)]
    fn predict_default(&self, rt: RouteType, rs: RouteSection, ts: &TimeSlot, et: EventType) 
            -> FnResult<PredictionResult> {

        let curve = self.delay_statistics.general.all_default_curves.get(&(rt, rs.clone(), ts.clone(), et))
            .ok_or_else(|| SimpleError::new(format!("No default curve found for {:?}, {:?}, {}, {:?}", rt, rs, ts.id, et)))?;

        Ok(PredictionResult::General(Box::new(curve.clone())))
    }

    // looks up a curve (or curve set) from specific curves and returns it
    #[allow(dead_code)]
    fn predict_specific(&self, 
            route_id: &str, 
            route_variant: u64, 
            start: Option<(&str, Option<f32>)>, //&str for stop_id, f32 for initial delay
            stop_id: &str, 
            ts: &TimeSlot,
            et: EventType) -> FnResult<PredictionResult> {

        // find the route variant data that we need:
        let rvdata = &self.delay_statistics.specific[route_id].variants[&route_variant];
        // find index of target stop:
        let target_stop_index : u32 = rvdata.stop_ids.iter().position(|e| e == stop_id).unwrap()
            .try_into().unwrap(); //TODO: Error handling for unwraps

        match start {
            None => { 
                // get general curve for target stop:
                let curve = rvdata.general_delay[et][&target_stop_index].clone();
                return Ok(PredictionResult::SemiSpecific(Box::new(curve)));
            },
            Some((s_id, d)) => {
                
                let start_stop_index : u32 = rvdata.stop_ids.iter().position(|e| e == s_id).unwrap()
                    .try_into().unwrap(); //TODO: Error handling for unwraps
                let curveset = &rvdata.curve_sets[&(start_stop_index, target_stop_index, ts.clone())];
                match d {
                    // get curve set for start-stop:
                    None => {
                        return Ok(PredictionResult::SpecificCurveSet(curveset.clone()));
                    },
                    // get curve for start-stop and initial delay:
                    Some(delay) => {
                        let curve = curveset.curve_at_x_with_continuation(delay);
                        return Ok(PredictionResult::SpecificCurve(Box::new(curve)));
                    }
                };
            },
        };
    }

    fn read_schedule(sub_args: &ArgMatches) -> FnResult<Gtfs> {
        println!("Parsing schedule…");
        let schedule = Gtfs::new(sub_args.value_of("schedule").unwrap())?;
        println!("Done with parsing schedule.");
        Ok(schedule)
    }

    fn read_delay_statistics(sub_args: &ArgMatches) -> FnResult<Box<DelayStatistics>> {
        println!("parsing delay statistics…");
        let dir_name = String::from(sub_args.value_of("dir").unwrap());
        let delay_stats = (DelayStatistics::load_from_file(&dir_name, "stats", &SerdeFormat::MessagePack))?;
        println!("Done with parsing delay statistics.");
        Ok(delay_stats)
    }
}
