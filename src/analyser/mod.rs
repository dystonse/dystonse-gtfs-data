mod count;
mod curve_utils;
mod curve_visualisation;
pub mod specific_curves;
pub mod default_curves;
pub mod curves;

#[cfg(feature = "visual-schedule")]
mod visual_schedule;

use chrono::{Local, DateTime};
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use regex::Regex;

use count::*;
use specific_curves::SpecificCurveCreator;
use default_curves::DefaultCurveCreator;
use curves::CurveCreator;
use curve_visualisation::CurveDrawer;

#[cfg(feature = "visual-schedule")]
use visual_schedule::*;

use crate::{Main, FnResult, OrError};

use std::str::FromStr;
use std::sync::Arc;

pub struct Analyser<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    schedule: Arc<Gtfs>,
}

impl<'a> Analyser<'a> {
    pub fn get_subcommand() -> App<'a> {
        let mut analyse = App::new("analyse").about("Performs some statistical analyses on the stored data.")
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
            .subcommand(App::new("compute-specific-curves")
                .about("Generates curve data for specific routes from realtime data out of the database")
                .arg(Arg::new("route-ids")
                    .short('r')
                    .long("route-ids")
                    .about("If provided, curves will be computed for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                ).arg(Arg::new("all")
                    .short('a')
                    .long("all")
                    .about("If provided, curves will be computed for each route of the schedule.")
                    .conflicts_with("route-ids")
                )
            )
            .subcommand(App::new("compute-default-curves")
                .about("Generates default curve data from realtime data out of the database")
            )
            .subcommand(App::new("compute-curves")
                .about("Generates default and specific curve data from realtime data out of the database")
                .arg(Arg::new("route-ids")
                    .short('r')
                    .long("route-ids")
                    .about("If provided, curves will be computed for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                ).arg(Arg::new("all")
                    .short('a')
                    .long("all")
                    .about("If provided, curves will be computed for each route of the schedule.")
                    .conflicts_with("route-ids")
                ).arg(Arg::new("default-only")
                    .short('d')
                    .long("default-only")
                    .about("If provided, only default curves will be generated, but the output format is still the same.")
                    .conflicts_with("route-ids")
                )
            )
            .subcommand(App::new("draw-curves")
                .about("Draws curves out of previously generated curve data without accessing the database")
                .arg(Arg::new("route-ids")
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
            );

            if cfg!(feature = "visual-schedule") {
                analyse = analyse.subcommand(App::new("graph")
                    .about("Draws graphical schedules of planned and actual departures.")
                    .arg(Arg::new("route-ids")
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
                );
            }
            
            return analyse;
   }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Analyser<'a> {
        Analyser {
            main,
            args,
            schedule: main.get_schedule().unwrap(),
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("count", Some(_sub_args)) => run_count(&self),
            #[cfg(feature = "visual-schedule")]
            ("graph", Some(sub_args)) => {
                let mut vsc = VisualScheduleCreator { 
                    main: self.main, 
                    analyser: self,
                    args: sub_args,
                };
                vsc.run_visual_schedule()
            },
            ("compute-specific-curves", Some(sub_args)) => {
                let scc = SpecificCurveCreator {
                    main: self.main,
                    analyser: self,
                    args: sub_args,
                };
                scc.run_specific_curves()
            },
            ("compute-default-curves", Some(sub_args)) => {
                let dcc = DefaultCurveCreator {
                    main: self.main,
                    analyser: self,
                    args: sub_args,
                };
                dcc.run_default_curves()
            },
            ("compute-curves", Some(sub_args)) => {
                let cc = CurveCreator {
                    main: self.main,
                    analyser: self,
                    args: sub_args, 
                };
                cc.run_curves()
            },
            ("draw-curves", Some(sub_args)) => {
                let cd = CurveDrawer {
                    main: self.main,
                    analyser: self,
                    args: sub_args,
                };
                cd.run_curves()
            },
            _ => panic!("Invalid arguments."),
        }
    }

    pub fn date_time_from_filename(filename: &str) -> FnResult<DateTime<Local>> {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }
        let date_element_captures = FIND_DATE.captures(&filename).or_error("File name does not contain a valid date (does not match format YYYY-MM-DD): {}")?;
        Ok(DateTime::<Local>::from_str(&date_element_captures[1])?)
    }
}