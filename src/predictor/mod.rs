use crate::analyser::route_data;
use crate::analyser::route_sections;
use crate::analyser::time_slots;
use crate::analyser::default_curves::*;

use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use mysql::*;
use regex::Regex;
use simple_error::SimpleError;

use crate::FnResult;
use crate::Main;

use std::str::FromStr;

pub struct Predictor<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    data_dir: Option<String>,
    schedule: Gtfs,
    default_curves: DefaultCurves,
    //TODO: add field with struct for "route data" curves here
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
            schedule: Predictor::read_schedule(args).unwrap(),
            default_curves: Predictor::read_default_curves(args).unwrap(),
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

    // keeps running and answering requests for predictions until stopped
    fn run_start(&self, _args: &ArgMatches) -> FnResult<()> {
        //TODO: everything


        Ok(())
    }

    // looks up one prediction and then returns
    fn run_single(&self, _args: &ArgMatches) -> FnResult<()> {
        //TODO: everything

        Ok(())
    }

    fn read_schedule(sub_args: &ArgMatches) -> FnResult<Gtfs> {
        println!("Parsing schedule…");
        let schedule = Gtfs::new(sub_args.value_of("schedule").unwrap())?; // TODO proper error message if this fails
        println!("Done with parsing schedule.");
        Ok(schedule)
    }

    fn read_default_curves(sub_args: &ArgMatches) -> FnResult<DefaultCurves> {
        println!("parsing default curves…");
        let file_path = format!("{}/curve_data/default_curves/Default_curves.crv", 
            String::from(sub_args.value_of("dir").unwrap())); //TODO: this could panic!
        let def_curves = (DefaultCurves::load_from_file(&file_path))?;
        println!("Done with parsing default curves.");
        Ok(def_curves)
    }
}
