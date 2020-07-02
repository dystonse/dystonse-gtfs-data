use crate::types::{EventType, TimeSlot, RouteSection, PredictionResult, DelayStatistics};

use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::{Gtfs, RouteType};
use std::str::FromStr;
use std::convert::TryInto;
use itertools::Itertools;

use simple_error::bail;

use crate::{Main, FnResult, OrError};

use dystonse_curves::tree::{SerdeFormat, NodeData};
use prost::Message;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

mod real_time;

pub struct Predictor<'a> {
    #[allow(dead_code)]
    pub main: &'a Main,
    pub args: &'a ArgMatches,
    pub _data_dir: Option<String>,
    pub schedule: Arc<Gtfs>,
    pub delay_statistics: Box<DelayStatistics>,
}

impl<'a> Predictor<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("predict")
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
                ).arg(Arg::new("stop-id")
                    .short('i')
                    .long("stop-id")
                    .about("Id of the stop for which the prediction shall be made. May be ommitted to get predictions for all stops of the route.")
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
            .arg(Arg::new("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .about("The directory which contains schedules and precomputed curves")
                .long_about(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and precomputed curve data."
                )
            ).arg(Arg::new("schedule")
                .short('s')
                .long("schedule")
                .required(true)
                .about("The path of the GTFS schedule that is used to look up any static GTFS data.")
                .takes_value(true)
                .value_name("GTFS_SCHEDULE")
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Predictor<'a> {
        Predictor {
            main,
            args,
            _data_dir: Some(String::from(args.value_of("dir").unwrap())),
            schedule: Arc::new(Self::read_schedule(args).unwrap()),
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
        let potential_stop_id = args.value_of("stop-id");
        let event_type = match args.value_of("event-type").unwrap() {
            "arrival" => EventType::Arrival,
            "departure" => EventType::Departure,
            _ => {panic!("Invalid event type argument!");}
        };
        let date_time = NaiveDateTime::parse_from_str(args.value_of("date-time").unwrap(), "%Y-%m-%dT%H:%M:%S")?;

        let trip = self.schedule.get_trip(trip_id)?;

        // parse optional arguments:
        let start = match args.value_of("start-stop-id") {
            Some(s) => match args.value_of("initial-delay") {
                            Some(d) => Some ((s.to_string(), Some(f32::from_str(d).unwrap()))),
                            None => Some((s.to_string(), None)),
                        },
            None => {
                if args.is_present("use-realtime") {
                    match real_time::get_realtime_data(self.main, &trip) {
                        Ok((stop_id, delay)) => Some((stop_id.clone(), Some(delay as f32))),
                        _ => None
                    }
                } else {
                    None
                }
            },
        };

        // if no single stop_id is given, iterate over all stop_ids of the trip
        let stop_ids = match potential_stop_id {
            Some(stop_id) => vec!{stop_id},
            None => trip.stop_times.iter().map(|st| st.stop.id.as_str()).collect()
        };

        for stop_id in stop_ids {
            // data structure to hold the prediction result:
            let prediction = self.predict(route_id, trip_id, &start, stop_id, event_type, date_time);

            if let Ok(actual_prediction) = &prediction {
                println!("Got a prediction, try to write GTFS-rt message…");
                let ext = actual_prediction.to_stop_time_event_extension();
                let mut vec : Vec<u8> = Vec::new(); 
                ext.encode(&mut vec)?;
                let file_path = format!("data/message_{}.pb", stop_id);
                let mut file = match File::create(&file_path) {
                    Err(why) => panic!("couldn't create file {}: {}", file_path, why),
                    Ok(file) => file,
                };
                match file.write_all(&vec) {
                    Err(why) => panic!("couldn't write: {}", why),
                    Ok(_) => println!("successfully wrote."),
                }
            }
            // output the resulting curve(s) to the command line:
            // TODO: we could probably use more advanced kinds of output here
            // TODO / FIXME: if event type is departure, we say "departure" here but actually return the 
            // arrival delay in cases where the result is a curve set or specific curve. 
            // Event type choice is only used in default and semi specific curves.
            println!("prediction of {:?} delay at stop {} for route {}, trip {} on {:?}:", event_type, stop_id, route_id, trip_id, date_time);
            println!("{:?}", prediction);
        }

        Ok(())
    }


    /// finds out which kind of curve can be used for this prediction and looks up the requested curve
    fn predict(&self, 
            route_id: &str, 
            trip_id: &str, 
            start: &Option<(String, Option<f32>)>, 
            stop_id: &str, 
            et: EventType, 
            date_time: NaiveDateTime) -> FnResult<PredictionResult> {

        // parse lookup parameters from input
        let ts = TimeSlot::from_datetime(date_time);
        let trip = self.schedule.get_trip(trip_id)?;
       
        let route_variant : u64 = u64::from_str(trip.route_variant.as_ref().unwrap()).unwrap(); 
        // should never panic because we already checked the validity of 
        // the trip, and route variants are always numbers.

        // try to find a specific prediction:
        let specific_prediction = self.predict_specific(route_id, route_variant, start, stop_id, ts, et);

        // unwrap that, or try a default prediction if it failed:
        let prediction = specific_prediction.or_else(|_| {
            // prepare some more lookup parameters
            let r = self.schedule.get_route(route_id)?;
            let rt = r.route_type;
            let rs = RouteSection::get_route_section(&self.schedule, trip_id, stop_id)?;
            // try default prediction
            self.predict_default(rt, rs, ts, et)
        });

        //return the prediction result
        prediction
    }

    // looks up a curve from default curves and returns it
    fn predict_default(&self, rt: RouteType, rs: RouteSection, ts: &TimeSlot, et: EventType) 
            -> FnResult<PredictionResult> {

        let curve = self.delay_statistics.general.all_default_curves.get(&(rt, rs.clone(), ts.clone(), et))
            .or_error(&format!("No default curve found for {:?}, {:?}, {}, {:?}", rt, rs, ts, et))?;

        Ok(PredictionResult::General(Box::new(curve.clone())))
    }

    // looks up a curve (or curve set) from specific curves and returns it
    fn predict_specific(&self, 
            route_id: &str, 
            route_variant: u64, 
            start: &Option<(String, Option<f32>)>, //&str for stop_id, f32 for initial delay
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
                let curve = rvdata.general_delay[et].get(&target_stop_index);
                if curve.is_none() {
                    bail!("No curve for stop {}.", stop_id);
                }
                return Ok(PredictionResult::SemiSpecific(Box::new(curve.unwrap().clone())));
            },
            Some((s_id, d)) => {
                
                let start_stop_index : u32 = rvdata.stop_ids.iter().position(|e| e == s_id).unwrap()
                    .try_into().unwrap(); //TODO: Error handling for unwraps
                let potential_curveset = &rvdata.curve_sets.get(&(start_stop_index, target_stop_index, ts.clone()));
                if let Some(curveset) = potential_curveset {
                    // TODO we get an "thread 'main' panicked at 'no entry found for key'" error in the line above when we run this command:
                    // cargo build && RUST_BACKTRACE=full time cargo run -- --source vbn --host macmini.local 
                    // -p "<censored>" predict data --schedule data/schedule/gtfs-schedule-2020-06-23.zip single 
                    // --route-id 35729_3 --trip-id 133010796 --stop-id 000009014277 --event-type arrival 
                    // --date-time 2020-06-24T21:02:47 --use-realtime
                    match d {
                        // get curve set for start-stop:
                        None => {
                            return Ok(PredictionResult::SpecificCurveSet((**curveset).clone()));
                        },
                        // get curve for start-stop and initial delay:
                        Some(delay) => {
                            let curve = curveset.curve_at_x_with_continuation(*delay);
                            return Ok(PredictionResult::SpecificCurve(Box::new(curve)));
                        }
                    };
                }
                bail!("No specific curveset found");
            },
        };
    }

    fn read_schedule(sub_args: &ArgMatches) -> FnResult<Gtfs> {
        println!("Parsing schedule…");
        let schedule = Gtfs::new(sub_args.value_of("schedule").unwrap())?;
        println!("Done with parsing schedule.");
        Ok(schedule)
    }

    pub fn read_delay_statistics(sub_args: &ArgMatches) -> FnResult<Box<DelayStatistics>> {
        println!("parsing delay statistics…");
        let dir_name = String::from(sub_args.value_of("dir").unwrap());
        let delay_stats = (DelayStatistics::load_from_file(&dir_name, "all_curves", &SerdeFormat::MessagePack))?;
        println!("Done with parsing delay statistics, found:");
        for (rt, rs, ts, et) in delay_stats.general.all_default_curves.keys().sorted() {
            println!("Curve for {:?}, {:?}, {}, {:?}", rt, rs, ts, et);
        }
        Ok(delay_stats)
    }

}
