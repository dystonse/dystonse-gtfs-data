mod count;
mod curve_analysis;
mod curve_visualisation;
mod route_data;
mod route_sections;
mod default_curves;
mod visual_schedule;
mod time_slots;

use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use mysql::*;
use regex::Regex;
use simple_error::SimpleError;

use count::*;
use curve_analysis::CurveCreator;
use default_curves::DefaultCurveCreator;
use curve_visualisation::CurveDrawer;
use visual_schedule::*;

use crate::FnResult;
use crate::Main;

use std::str::FromStr;

pub struct Analyser<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    data_dir: Option<String>,
}

impl<'a> Analyser<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("analyse")
            .subcommand(App::new("count")
                .arg(Arg::new("interval")
                    .short('i')
                    .long("interval")
                    .default_value("1h")
                    .about("Sets the step size for counting entries. The value will be parsed by the `parse_duration` crate, which acceps a superset of the `systemd.time` syntax.")
                    .value_name("INTERVAL")
                    .takes_value(true)
                )
            )
            .subcommand(App::new("graph")
                .about("Draws graphical schedules of planned and actual departures.")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used as a base for the graphical schedule.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                ).arg(Arg::new("route-ids")
                    .short('r')
                    .long("route-ids")
                    .about("If provided, graphical schedules will be created for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                    .conflicts_with("shape-ids")
                ).arg(Arg::new("shape-ids")
                    .short('p')
                    .long("shape-ids")
                    .about("If provided, graphical schedules will be created for each route variant that has the selected shape-id.")
                    .value_name("SHAPE_ID")
                    .multiple(true)
                    .conflicts_with("route-ids")
                ).arg(Arg::new("all")
                    .short('a')
                    .long("all")
                    .about("If provided, graphical schedules will be created for each route of the schedule.")
                    .conflicts_with("route-ids")
                )
            )
            .subcommand(App::new("compute-curves")
                .about("Generates curve data from realtime data out of the database")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used as a base for the curves.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                ).arg(Arg::new("route-ids")
                    .short('r')
                    .long("route-ids")
                    .about("If provided, curves will be computed for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                // TODO implement the "all" mode
                // ).arg(Arg::new("all")
                //     .short('a')
                //     .long("all")
                //     .about("If provided, curves will be computed for each route of the schedule.")
                //     .conflicts_with("route-ids")
                )
            )
            .subcommand(App::new("compute-default-curves")
                .about("Generates default curve data from realtime data out of the database")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used as a base for the curves.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                )
            )
            .subcommand(App::new("draw-curves")
                .about("Draws curves out of previously generated curve data without accessing the database")
                .arg(Arg::new("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .about("The path of the GTFS schedule that is used as a base for the curves.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                ).arg(Arg::new("route-ids")
                    .short('r')
                    .long("route-ids")
                    .about("If provided, curves will be drawn for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                // TODO implement the "all" mode
                // ).arg(Arg::new("all")
                //     .short('a')
                //     .long("all")
                //     .about("If provided, curves will be drawn for each route of the schedule.")
                //     .conflicts_with("route-ids")
                )
            )
            .arg(Arg::new("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .about("The directory which contains schedules and realtime data")
                .long_about(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and realtime data (located in a subdirectory named 'rt')."
                )
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Analyser<'a> {
        Analyser {
            main,
            args,
            data_dir: Some(String::from(args.value_of("dir").unwrap())),
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("count", Some(_sub_args)) => run_count(&self),
            ("graph", Some(sub_args)) => {
                let mut vsc = VisualScheduleCreator { 
                    main: self.main, 
                    analyser: self,
                    args: sub_args,    
                    schedule: self.read_schedule(sub_args)?
                };
                vsc.run_visual_schedule()
            },
            ("compute-curves", Some(sub_args)) => {
                let cc = CurveCreator {
                    main: self.main,
                    analyser: self,
                    args: sub_args, 
                    schedule: self.read_schedule(sub_args)?
                };
                cc.run_curves()
            },
            ("compute-default-curves", Some(sub_args)) => {
                let dcc = DefaultCurveCreator {
                    main: self.main,
                    analyser: self,
                    args: sub_args, 
                    schedule: self.read_schedule(sub_args)?
                };
                dcc.run_default_curves()
            },
            ("draw-curves", Some(sub_args)) => {
                let cc = CurveDrawer {
                    main: self.main,
                    analyser: self,
                    args: sub_args, 
                    schedule: self.read_schedule(sub_args)?
                };
                cc.run_curves()
            },
            _ => panic!("Invalid arguments."),
        }
    }

    fn read_schedule(&self, sub_args: &ArgMatches) -> FnResult<Gtfs> {
        println!("Parsing schedule…");
        let schedule = Gtfs::new(sub_args.value_of("schedule").unwrap())?; // TODO proper error message if this fails
        println!("Done with parsing schedule.");
        Ok(schedule)
    }

    pub fn date_time_from_filename(filename: &str) -> FnResult<NaiveDateTime> {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }
        let date_element_captures =
            FIND_DATE
                .captures(&filename)
                .ok_or(SimpleError::new(format!(
                "File name does not contain a valid date (does not match format YYYY-MM-DD): {}",
                filename
            )))?;
        Ok(NaiveDateTime::from_str(&date_element_captures[1])?)
    }
}
