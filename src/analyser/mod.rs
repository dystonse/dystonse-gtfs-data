use crate::importer::Importer;
use crate::FnResult;
use crate::Main;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use gtfs_structures::Trip;
use mysql::prelude::*;

use itertools::Itertools;
use mysql::*;
use parse_duration::parse;
use plotters::prelude::*;
use rand::Rng;
use regex::Regex;
use simple_error::SimpleError;
use std::collections::HashSet;
use std::fs;
use std::str::FromStr;

use plotters::palette::LinSrgba;
use plotters::style::text_anchor::*;

pub struct Analyser<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    data_dir: Option<String>,
    schedule: Option<Gtfs>,
}

type DbTuple = (
    Option<i32>,
    Option<NaiveTime>,
    Option<NaiveDate>,
    String,
    String,
);

impl<'a> Analyser<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("analyse")
            .subcommand(App::new("count")
                .arg(Arg::with_name("interval")
                    .short('i')
                    .long("interval")
                    .default_value("1h")
                    .help("Sets the step size for counting entries. The value will be parsed by the `parse_duration` crate, which acceps a superset of the `systemd.time` syntax.")
                    .value_name("INTERVAL")
                    .takes_value(true)
                )
            )
            .subcommand(App::new("graph")
                .arg(Arg::with_name("schedule")
                    .short('s')
                    .long("schedule")
                    .required(true)
                    .help("The path of the GTFS schedule that is used as a base for the graphical schedule.")
                    .takes_value(true)
                    .value_name("GTFS_SCHEDULE")
                ).arg(Arg::with_name("route-ids")
                    .short('r')
                    .long("route-ids")
                    .help("If provided, graphical schedules will be created for each route variant of each of the selected routes.")
                    .value_name("ROUTE_ID")
                    .multiple(true)
                    .conflicts_with("shape-ids")
                ).arg(Arg::with_name("shape-ids")
                    .short('p')
                    .long("shape-ids")
                    .help("If provided, graphical schedules will be created for each route variant that has the selected shape-id.")
                    .value_name("SHAPE_ID")
                    .multiple(true)
                    .conflicts_with("route-ids")
                ).arg(Arg::with_name("all")
                    .short('a')
                    .long("all")
                    .help("If provided, graphical schedules will be created for each route of the schedule.")
                    .conflicts_with("route-ids")
                )
            )
        .arg(Arg::with_name("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .help("The directory which contains schedules and realtime data")
                .long_help(
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
            schedule: None,
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("count", Some(_sub_args)) => self.run_count(),
            ("graph", Some(_sub_args)) => self.run_visual_schedule(),
            // ("batch", Some(sub_args)) => {
            //     self.set_dir_paths(sub_args)?;
            //     self.run_as_non_manual(false)
            // }
            // ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
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

    fn run_count(&self) -> FnResult<()> {
        let imported_dir = format!("{}/imported", &self.data_dir.as_ref().unwrap());
        let rt_filenames = Importer::read_dir_simple(&imported_dir)?;

        if rt_filenames.is_empty() {
            return Err(Box::from(SimpleError::new("No realtime data.")));
        }

        let mut con = self.main.pool.get_conn()?;
        let (start, end): (mysql::chrono::NaiveDateTime, mysql::chrono::NaiveDateTime) = con
            .query_first("SELECT MIN(time_of_recording), MAX(time_of_recording) from realtime")?
            .unwrap();

        let std_date = parse(
            self.args
                .subcommand_matches("count")
                .unwrap()
                .value_of("interval")
                .unwrap(),
        )?;
        let step: chrono::Duration = chrono::Duration::from_std(std_date)?;
        let mut time_min = start;
        let mut time_max = start + step;
        println!(
            "time_min; time_max; stop time update count; average delay; rt file count; rt file size"
        );
        loop {
            let mut rt_file_count = 0;
            let mut rt_file_size = 0;
            let row: mysql::Row = con
                .exec_first(
                    "SELECT COUNT(*), AVG(delay_arrival) 
                FROM realtime 
                WHERE (`time_of_recording` BETWEEN ? AND ?) 
                AND (delay_arrival BETWEEN - 36000 AND 36000) 
                AND source = ?",
                    (time_min, time_max, &self.main.source),
                )?
                .unwrap();
            let count: i32 = row.get(0).unwrap();
            let delay: f32 = row.get_opt(1).unwrap().unwrap_or(-1.0);
            // println!("Between {} and {} there are {} delay values, average is {} seconds.", time_min, time_max, count, delay);

            for rt_filename in &rt_filenames {
                let rt_date = Analyser::date_time_from_filename(&rt_filename).unwrap();
                if rt_date > time_min && rt_date < time_max {
                    rt_file_count += 1;
                    rt_file_size += fs::metadata(&rt_filename)?.len();
                }
            }

            println!(
                "{}; {}; {}; {}; {}; {}",
                time_min, time_max, count, delay, rt_file_count, rt_file_size
            );
            time_min = time_max;
            time_max = time_min + step;
            if time_max > end {
                break;
            }
        }

        Ok(())
    }

    // This method was used to output a graphviz dot file of the stops of a route and its variants
    fn print_pair(schedule: &Gtfs, first_stop_id: &str, second_stop_id: &str, reverse: bool) {
        println!(
            r##""{} ()" -> "{} ()" [color={}, dir={}]"##,
            schedule.get_stop(first_stop_id).unwrap().name,
            //first_stop_id,
            schedule.get_stop(second_stop_id).unwrap().name,
            //second_stop_id,
            if reverse { "red" } else { "blue" },
            if reverse { "back" } else { "foreward" }
        );
    }

    fn run_visual_schedule(&mut self) -> FnResult<()> {
        let args = self.args.subcommand_matches("graph").unwrap();

        println!("Parsing schedule…");
        self.schedule = Some(Gtfs::new(args.value_of("schedule").unwrap())?);
        println!("Done with parsing schedule.");

        if let Some(route_ids) = args.values_of("route-ids") {
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                self.create_visual_schedule_for_route(&String::from(route_id))?;
            }
        }
        if let Some(shape_ids) = args.values_of("shape-ids") {
            println!("Handling {} shape ids…", shape_ids.len());
            for shape_id in shape_ids {
                self.create_visual_schedule_for_shapes(
                    &String::from(shape_id),
                    vec![&String::from(shape_id)],
                    &Vec::new(),
                    "unknown",
                    "unknown",
                )?;
            }
        }
        if args.is_present("all") {
            println!("Creating graphs for all routes. First, selecting route_ids for which we actually have data…");

            let mut con = self.main.pool.get_conn()?;

            let stmt = con.prep(r"SELECT DISTINCT route_id FROM realtime LIMIT 200")?;
            let result = con.exec_iter(&stmt, {})?;
            let route_ids: Vec<String> = result
                .map(|row| {
                    let id: String = from_row(row.unwrap());
                    id
                })
                .collect();

            let schedule = self.schedule.as_ref().unwrap();

            println!(
                "Found data for {} of {} route_ids.",
                route_ids.len(),
                schedule.routes.len()
            );
            let (count, success) = route_ids
                .iter()
                .map(|id| self.create_visual_schedule_for_route(&id).is_ok())
                .fold((0, 0), |a, b| (a.0 + 1, a.1 + (if b { 1 } else { 0 })));
            println!(
                "Tried to create graphs for {} routes, had success with {} of them.",
                count, success
            );
        }

        Ok(())
    }

    fn create_visual_schedule_for_route(&self, route_id: &String) -> FnResult<()> {
        let mut con = self.main.pool.get_conn()?;
        // TODO we need to find each trip for each day, and then sort the rows according to the stop_ids as they occur in the schedules route variant. Oof.
        let stmt = con.prep(
            r"SELECT 
                delay_arrival,
                TIME(time_arrival_estimate) as time,
                DATE(DATE_SUB(time_of_recording, INTERVAL 3 HOUR)) as date, 
                trip_id, 
                stop_id 
            FROM 
                realtime 
            WHERE 
                source=:source AND 
                route_id=:routeid AND
                (time_of_recording, trip_id, stop_id) IN 
                ( 
                    SELECT 
                        MAX(time_of_recording), 
                        trip_id, 
                        stop_id 
                    FROM 
                        realtime
                    WHERE
                        source=:source AND
                        route_id=:routeid
                    GROUP BY 
                        trip_id, 
                        DATE(DATE_SUB(time_of_recording, INTERVAL 3 HOUR)), 
                        stop_id
                ) 
            ORDER BY 
                date,
                trip_id,
                time;",
        )?;

        let mut result = con.exec_iter(
            &stmt,
            params! {
                "source" => &self.main.source,
                "routeid" => route_id
            },
        )?;

        let result_set = result.next_set().unwrap()?;

        let db_tuples: Vec<_> = result_set
            .map(|row| {
                let tuple: DbTuple = from_row(row.unwrap());
                tuple
            })
            .collect();

        if db_tuples.len() < 10 {
            println!(
                "Skipping route id {} because there are only {} data points.",
                route_id,
                db_tuples.len()
            );
            return Ok(());
        }

        // collect trips and trip variants for this route
        let schedule = &self.schedule.as_ref().unwrap();
        let all_trips = &schedule.trips;

        let trips_of_route: Vec<&Trip> = all_trips
            .values()
            .filter(|trip| trip.route_id == *route_id)
            .collect();

        let route_variant_ids: HashSet<String> = trips_of_route
            .iter()
            .filter_map(|trip| trip.route_variant.clone())
            .collect();

        println!(
            "Handling {} route variant ids for route id {}…",
            route_variant_ids.len(),
            route_id
        );

        // collect some meta data about the route, which will be used for naming the output files
        let route = schedule.get_route(route_id)?;
        let route_name = route.short_name.clone();
        let agency_id = route.agency_id.as_ref().unwrap().clone();

        let agency_name = schedule
            .agencies
            .iter()
            .filter(|agency| agency.id.as_ref().unwrap() == &agency_id)
            .next()
            .unwrap()
            .name
            .clone();

        // now create the actual images, one for each variant
        // TODO group variants into combined images
        for route_variant_id in route_variant_ids {
            self.create_visual_schedule_for_route_variants(
                &route_variant_id,
                vec![&route_variant_id],
                &db_tuples,
                &agency_name,
                &route_name,
            )?;
        }

        Ok(())
    }

    fn create_visual_schedule_for_route_variants(
        &self,
        primary_route_variant_id: &String,
        route_variant_ids: Vec<&String>,
        db_tuples: &Vec<DbTuple>,
        agency_name: &str,
        route_name: &str,
    ) -> FnResult<()> {
        let schedule = &self.schedule.as_ref().unwrap();
        let all_trips = &schedule.trips;
        let empty_string = String::from("");

        // select any trip with the primary route variant as the primary trip
        let primary_trip = all_trips
            .values()
            .filter(|trip| {
                trip.route_variant.as_ref().unwrap_or(&empty_string) == primary_route_variant_id
            })
            .next()
            .unwrap();

        // gather trips for all route variants
        let trips: Vec<&Trip> = all_trips
            .values()
            .filter(|trip| {
                route_variant_ids.contains(&trip.route_variant.as_ref().unwrap_or(&empty_string))
            })
            .collect();

        println!(
            "Filtered {} trips and fround {} trips with route_variant_id {}.",
            all_trips.len(),
            trips.len(),
            primary_route_variant_id
        );

        let path = &format!("data/img/agency_{}/route_{}", agency_name, route_name);
        fs::create_dir_all(path)?;
        self.create_visual_schedule_for_trips(
            primary_trip,
            trips,
            &format!("{}/variant_{}.png", path, primary_route_variant_id),
            db_tuples,
        )
    }

    fn create_visual_schedule_for_shapes(
        &self,
        primary_shape_id: &String,
        shape_ids: Vec<&String>,
        db_tuples: &Vec<DbTuple>,
        agency_name: &str,
        route_name: &str,
    ) -> FnResult<()> {
        let schedule = &self.schedule.as_ref().unwrap();
        let all_trips = &schedule.trips;
        let empty_string = String::from("");
        let primary_trip: &Trip = all_trips
            .values()
            .filter(|trip| trip.shape_id.as_ref().unwrap_or(&empty_string) == primary_shape_id)
            .next().unwrap();

        let trips: Vec<&Trip> = all_trips
            .values()
            .filter(|trip| shape_ids.contains(&trip.shape_id.as_ref().unwrap_or(&empty_string)))
            .collect();
        println!(
            "Filtered {} trips and fround {} trips with shape_id {}.",
            all_trips.len(),
            trips.len(),
            primary_shape_id
        );

        let path = &format!("data/img/agency_{}/route_{}", agency_name, route_name);
        fs::create_dir_all(path)?;
        self.create_visual_schedule_for_trips(
            primary_trip,
            trips,
            &format!("{}/shape_{}.png", path, primary_shape_id),
            db_tuples,
        )
    }

    fn create_visual_schedule_for_trips(
        &self,
        primary_trip: &Trip,
        trips: Vec<&Trip>,
        name: &str,
        db_tuples: &Vec<DbTuple>,
    ) -> FnResult<()> {
        let mut creator = GraphCreator::new(
            String::from(name),
            primary_trip,
            trips,
            &self.schedule.as_ref().unwrap(),
            self.main,
            db_tuples,
        );

        creator.create()?;

        Ok(())
    }
}

struct GraphCreator<'a> {
    name: String,
    primary_trip: &'a Trip,
    trips: Vec<&'a Trip>,
    schedule: &'a Gtfs,
    main: &'a Main,
    relevant_stop_ids: Vec<String>,
    relevant_stop_names: Vec<String>,
    db_tuples: &'a Vec<DbTuple>,
}

impl<'a> GraphCreator<'a> {
    fn new(
        name: String,
        primary_trip: &'a Trip,
        trips: Vec<&'a Trip>,
        schedule: &'a Gtfs,
        main: &'a Main,
        db_tuples: &'a Vec<DbTuple>,
    ) -> GraphCreator<'a> {
        GraphCreator {
            primary_trip,
            trips,
            name,
            main,
            schedule,
            relevant_stop_ids: Vec::new(),
            relevant_stop_names: Vec::new(),
            db_tuples,
        }
    }

    fn create(&mut self) -> FnResult<()> {
        println!(
            "Creating visual schedule of {} trips with name '{}'.",
            self.trips.len(),
            self.name
        );

        self.relevant_stop_ids = self
            .primary_trip
            .stop_times
            .iter()
            .map(|stop_time| stop_time.stop.id.clone())
            .collect();
        self.relevant_stop_names = self
            .primary_trip
            .stop_times
            .iter()
            .map(|stop_time| stop_time.stop.name.clone())
            .collect();
        let stop_count = self.relevant_stop_ids.len();

        let data_for_current_trips: Vec<&DbTuple> = self
            .db_tuples
            .iter()
            .filter(|tup| self.trips.iter().any(|trip| trip.id == tup.3))
            .collect();

        if data_for_current_trips.len() < 10 {
            println!(
                "Skipping some trips because there are only {} data points.",
                data_for_current_trips.len()
            );
            return Ok(());
        }

        // get all dates for which we have data
        let dates = data_for_current_trips
            .iter()
            .filter_map(|tup| tup.2)
            .unique();

        let mut actual_trip_shapes = Vec::new();

        let color_weekday = LinSrgba::new(0.0, 0.5, 0.0, 0.3);
        let color_saturday = LinSrgba::new(0.5, 0.5, 0.0, 0.3);
        let color_sunday = LinSrgba::new(0.5, 0.0, 0.0, 0.3);

        let mut date_count = 0;
        // iterate over those dates
        for date in dates {
            date_count += 1;
            let color = match date.weekday() {
                Weekday::Sat => color_saturday,
                Weekday::Sun => color_sunday,
                _ => color_weekday,
            };

            // get all data that belongs to this date
            let data_of_the_day = data_for_current_trips
                .iter()
                .filter(|tup| tup.2 == Some(date));

            // group the data by tup.3 (= trip_id)
            for (_trip_id, tuples) in &data_of_the_day.group_by(|tup| tup.3.clone()) {
                // for each trip_id, sort by the tup.4's (= stop_id) position in the list of relevant_stop_ids
                let sorted_tuples = tuples
                    .sorted_by_key(|tup| self.relevant_stop_ids.iter().position(|id| *id == tup.4));

                let path_for_trip = PathElement::new(
                    sorted_tuples
                        .filter_map(|tup| self.make_coordinate_from_tuple(tup))
                        .collect::<Vec<(f64, f64)>>(),
                    ShapeStyle::from(&color).stroke_width(2),
                );

                actual_trip_shapes.push(path_for_trip);
            }
        }

        println!(
            "Found {} data points for those trips spread over {} dates.",
            data_for_current_trips.len(),
            date_count
        );

        let rotated = TextStyle::from(("sans-serif", 20).into_font())
            .pos(Pos::new(HPos::Center, VPos::Center))
            .transform(FontTransform::Rotate270);
        let transparent = LinSrgba::new(0.0, 0.0, 0.0, 0.0);
        let invisible = ShapeStyle::from(&transparent);

        let mut root =
            BitMapBackend::new(&self.name, (stop_count as u32 * 30 + 40, 2048)).into_drawing_area();

        root.fill(&WHITE)?;
        root = root.margin(20, 200, 20, 20);

        let mut graphic_schedule = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(40)
            .build_ranged(-1f64..((stop_count - 1) as f64), 5f64..27f64)?;

        graphic_schedule
            .configure_mesh()
            .label_style(TextStyle::from(("sans-serif", 20).into_font()).color(&BLACK))
            .axis_style(&BLACK)
            .line_style_2(invisible)
            .x_labels(stop_count + 1)
            .x_label_offset(-7)
            .x_label_formatter(&|x| self.make_station_label(*x))
            .x_label_style(rotated)
            .y_label_formatter(&|y| self.make_time_string(*y))
            .y_labels(45)
            .draw()?;

        // DRAW REALTIME DATA
        graphic_schedule.draw_series(actual_trip_shapes)?;
        // DRAW SCHEDULE DATA
        graphic_schedule
            .draw_series(self.trips.iter().map(|trip| self.make_trip_drawable(trip)))?;
        Ok(())
    }

    fn make_trip_drawable(&self, trip: &Trip) -> PathElement<(f64, f64)> {
        PathElement::new(
            trip.stop_times
                .iter()
                .filter_map(|stop_time| {
                    self.make_coordinate(&stop_time.stop.id, stop_time.arrival_time)
                })
                .collect::<Vec<(f64, f64)>>(),
            ShapeStyle::from(&BLACK),
        )
    }

    fn make_coordinate(&self, stop_id: &str, time: Option<u32>) -> Option<(f64, f64)> {
        if let Some(mut time) = time {
            if let Some(x) = self.relevant_stop_ids.iter().position(|id| *id == stop_id) {
                let r = rand::thread_rng().gen_range(-30, 30) as f64;
                if time < 3600 * 3 {
                    time += 3600 * 24;
                }
                return Some((x as f64, (time as f64 + r) / 3600.0_f64));
            }
        }

        None
    }

    fn make_coordinate_from_tuple(&self, tuple: &DbTuple) -> Option<(f64, f64)> {
        if tuple.0.is_none() || tuple.1.is_none() {
            return None;
        }

        // Some providers seem to set the delay to 0 instead of Null when they have no data.
        if tuple.0.unwrap() == 0 {
            return None;
        }

        self.make_coordinate(
            &tuple.4,
            Some((tuple.1.unwrap().num_seconds_from_midnight() as i32) as u32),
        )
    }

    fn make_time_string(&self, t: f64) -> String {
        let hour = t.floor() as i32;
        let minute = ((t - t.floor()) * 60.0).floor() as i32;
        format!("{:02}:{:02}", hour, minute)
    }

    fn make_station_label(&self, x: f64) -> String {
        let mut name = self
            .relevant_stop_names
            .get(x as usize)
            .unwrap_or(&String::from("unbekannt"))
            .clone();
        if name.len() > 20 {
            name = format!("...{}", self.truncate(&name, 20));
        }
        format!("{}                                        .", name)
    }

    fn truncate(&self, s: &'a str, max_chars: usize) -> &'a str {
        match s.char_indices().nth(max_chars) {
            None => s,
            Some((idx, _)) => &s[..idx],
        }
    }
}
