use crate::importer::Importer;
use crate::FnResult;
use crate::Main;
use chrono::NaiveDateTime;
use chrono::Timelike;
use clap::{App, Arg, ArgMatches};
use gtfs_structures::Gtfs;
use gtfs_structures::Trip;
use mysql::prelude::*;

use mysql::*;
use parse_duration::parse;
use plotters::prelude::*;
use rand::Rng;
use regex::Regex;
use simple_error::SimpleError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::str::FromStr;

use plotters::palette::LinSrgba;

const ROUTE_ID: &'static str = "31414_3";

#[derive(Copy, Clone)]
enum Direction {
    Known(u8),
    Incompatible,
    Unknown,
}

#[derive(Copy, Clone)]
enum RelativeDirection {
    Same,
    Different,
    Incompatible,
    Unknown,
}

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
                    .help("Sets the step size for counting entries. The value will be parsed by the `parse_duration` crate, which acceps a superset of the `systemd.time` syntax.")
                    .value_name("INTERVAL")
                )
            )
            .subcommand(App::new("graph"))
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

    fn find_relative_direction(trip_a: &Trip, trip_b: &Trip) -> RelativeDirection {
        let mut dir = RelativeDirection::Unknown;

        for pair_a in trip_a.stop_times.windows(2) {
            for pair_b in trip_b.stop_times.windows(2) {
                if pair_a[0].stop.id == pair_b[0].stop.id && pair_a[1].stop.id == pair_b[1].stop.id
                {
                    // this pair has RelativeDirection::Same
                    if let RelativeDirection::Different = dir {
                        return RelativeDirection::Incompatible;
                    }
                    dir = RelativeDirection::Same;
                }
                if pair_a[0].stop.id == pair_b[1].stop.id && pair_a[1].stop.id == pair_b[0].stop.id
                {
                    // this pair has RelativeDirection::Different
                    if let RelativeDirection::Same = dir {
                        return RelativeDirection::Incompatible;
                    }
                    dir = RelativeDirection::Different;
                }
            }
        }

        dir
    }

    fn find_direction(
        known_directions: &HashMap<String, (&Trip, Direction)>,
        trip_to_check: &Trip,
    ) -> Direction {
        let mut dir = Direction::Unknown;

        for (_id, (known_trip, known_dir)) in known_directions {
            let new_dir = match Analyser::find_relative_direction(known_trip, trip_to_check) {
                RelativeDirection::Incompatible => return Direction::Incompatible,
                RelativeDirection::Same => *known_dir,
                RelativeDirection::Different => {
                    if let Direction::Known(x) = known_dir {
                        Direction::Known(1 - x)
                    } else {
                        Direction::Unknown
                    }
                }
                RelativeDirection::Unknown => Direction::Unknown,
            };
            if let Direction::Known(new_x) = new_dir {
                match dir {
                    Direction::Unknown => dir = new_dir,
                    Direction::Known(x) => {
                        if x == new_x {
                            dir = new_dir
                        } else {
                            return Direction::Incompatible;
                        }
                    }
                    Direction::Incompatible => return Direction::Incompatible,
                }
            }
        }

        dir
    }

    fn read_route(&self) -> FnResult<Vec<String>> {
        println!("Reading schedule…");

        let schedule = Gtfs::new("./data/schedule/vbn-gtfs-static-2020-03-14.zip")?;

        let route = schedule.get_route(ROUTE_ID).unwrap();

        let agency = schedule
            .agencies
            .iter()
            .filter(|a| a.id == route.agency_id)
            .nth(0)
            .unwrap();
        let trips: Vec<&gtfs_structures::Trip> = schedule
            .trips
            .values()
            .filter(|trip| trip.route_id == ROUTE_ID)
            .collect();
        println!(
            "Info für Route {} von {}. Habe {} trips gefunden.",
            route.short_name,
            agency.name,
            trips.len()
        );

        println!("Finding trip directions…");

        // First, let's find a long trip and…
        let mut longest_trip = trips[0];
        for &trip in &trips {
            if trip.stop_times.len() > longest_trip.stop_times.len() {
                longest_trip = trip;
            }
        }

        // …define its direction to be Known(0)
        let mut known_directions: HashMap<String, (&Trip, Direction)> = HashMap::new();
        known_directions.insert(longest_trip.id.clone(), (longest_trip, Direction::Known(0)));

        // Make a list of all trips that do not yet have a direction
        let mut trips_with_unknown_direction = trips.clone();
        trips_with_unknown_direction.retain(|trip| trip.id != longest_trip.id);

        loop {
            let mut trips_to_check_next_time = Vec::new();
            let mut found_something = false;
            for trip_to_check in trips_with_unknown_direction.drain(..) {
                match Analyser::find_direction(&known_directions, trip_to_check) {
                    Direction::Known(x) => {
                        known_directions.insert(
                            trip_to_check.id.clone(),
                            (&trip_to_check, Direction::Known(x)),
                        );
                        found_something = true;
                    }
                    Direction::Unknown => {
                        trips_to_check_next_time.push(trip_to_check);
                    }
                    Direction::Incompatible => {
                        println!(
                            "Trip {} is incompatible with other trips.",
                            trip_to_check.id
                        );
                    } // Just don't add to any list
                }
            }

            if !found_something {
                println!("Trips, deren Richtung nicht bestimmbar war:");
                for trip in trips_to_check_next_time {
                    println!("{}", trip.id);
                }
                break;
            }
        }

        println!("Trips with direction 0:");
        for (id, (_known_trip, known_dir)) in &known_directions {
            if let Direction::Known(0) = known_dir {
                println!("{}", id);
            }
        }

        println!("Trips with direction 1:");
        for (id, (_known_trip, known_dir)) in &known_directions {
            if let Direction::Known(1) = known_dir {
                println!("{}", id);
            }
        }

        // For each stop_id, collect the stop_id of 'next' stops across all trips.
        let mut next: HashMap<String, HashSet<String>> = HashMap::new();
        let mut stop_ids: HashSet<String> = HashSet::new();

        // iterate over all trips which have a known direction, no matter which one it is
        for (_id, (trip, dir)) in &known_directions {
            // id of previous stop
            let mut prev: Option<String> = None;

            // itetate over all stops of that trip in "natural" order
            for stop_time in &trip.stop_times {
                let stop_id = &stop_time.stop.id;
                stop_ids.insert(stop_id.clone());
                if let Some(prev_stop_id) = prev {
                    // now we have a pair of stop_id and prev_stop_id in "natural" order

                    if let Direction::Known(0) = dir {
                        // if this trip is in direction 0, just insert this pair
                        if next.get(&prev_stop_id) == None {
                            next.insert(prev_stop_id.clone(), HashSet::new());
                        }
                        let set = next.get_mut(&prev_stop_id).unwrap();
                        if set.insert(stop_id.clone()) {
                            Analyser::print_pair(&schedule, &prev_stop_id, stop_id, false);
                        }
                    }

                    if let Direction::Known(1) = dir {
                        // if this trip is in direction 1 (reverse), insert this pair reversed
                        if next.get(stop_id) == None {
                            next.insert(stop_id.clone(), HashSet::new());
                        }

                        let set = next.get_mut(stop_id).unwrap();
                        if set.insert(prev_stop_id.clone()) {
                            Analyser::print_pair(&schedule, stop_id, &prev_stop_id, true);
                        }
                    }
                }

                prev = Some(stop_id.clone());
            }
        }

        // convert HashSet to Vec to make it sortable
        let mut stop_ids: Vec<String> = stop_ids.drain().collect();

        println!("Sorting…");

        // now we know the local ordering of stops. Let's find a global ordering.

        // TODO we must break the cycles in this graph, and once it's acyclic, find a **topological** ordering.

        stop_ids.sort_by(|a, b| {
            if let Some(followers) = next.get(a) {
                if followers.contains(b) {
                    print!("L");
                    return std::cmp::Ordering::Less;
                }
            }
            if let Some(followers) = next.get(b) {
                if followers.contains(a) {
                    print!("G");
                    return std::cmp::Ordering::Greater;
                }
            }
            print!("E");
            std::cmp::Ordering::Equal
        });

        println!("\nFound order:");
        for stop_id in stop_ids {
            println!(" - {}", schedule.get_stop(&stop_id).unwrap().name);
        }

        // println!("Stops: {:?}", next);

        let stop_ids: Vec<String> = longest_trip
            .stop_times
            .iter()
            .map(|stop_time| stop_time.stop.id.clone())
            .collect();

        Ok(stop_ids)
    }

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

    fn run_visual_schedule(&self) -> FnResult<()> {
        let relevant_stop_ids = self.read_route()?;

        let mut con = self.main.pool.get_conn()?;
        let mut result = con.exec_iter(
            "SELECT `time_of_recording`, `time_arrival_estimate`, `time_arrival_schedule`, `stop_id`
            FROM realtime
            WHERE route_id = ?
            AND (delay_arrival BETWEEN - 36000 AND 36000)
            AND source = ?",
            (ROUTE_ID, &self.main.source,))?;

        let result_set = result.next_set().unwrap()?;

        let mut root =
            BitMapBackend::new("data/img/visual-schedule.png", (768, 2048)).into_drawing_area();

        root.fill(&BLACK)?;
        root = root.margin(20, 20, 20, 20);

        let transparent_green = LinSrgba::new(0.0, 1.0, 0.0, 0.125);
        let transparent_red = LinSrgba::new(1.0, 0.0, 0.0, 0.125);

        let mut coords_schedule: Vec<(f64, f64)> = Vec::new();
        let mut coords_estimate: Vec<(f64, f64)> = Vec::new();

        for row in result_set {
            let (_tor, tae, tas, si): (NaiveDateTime, NaiveDateTime, NaiveDateTime, String) =
                from_row(row.unwrap());

            if let Some(x) = relevant_stop_ids.iter().position(|id| *id == si) {
                coords_schedule.push((
                    x as f64,
                    tas.time().num_seconds_from_midnight() as f64 / 3600.0_f64,
                ));
                let r = rand::thread_rng().gen_range(-30, 30) as f64;
                coords_estimate.push((
                    x as f64,
                    (tae.time().num_seconds_from_midnight() as f64 + r) / 3600.0_f64,
                ));
            }
        }

        let mut graphic_schedule = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(40)
            .build_ranged(0f64..50f64, 5f64..24f64)?;
        graphic_schedule
            .configure_mesh()
            .label_style(TextStyle::from(("sans-serif", 20).into_font()).color(&WHITE))
            .axis_style(&WHITE)
            .disable_x_mesh()
            .disable_y_mesh()
            .draw()?;

        // TODO draw lines
        graphic_schedule.draw_series(
            coords_schedule
                .iter()
                .map(|coord| Circle::new(*coord, 2, transparent_green.filled())),
        )?;

        // TODO don't draw the schedule data from the realtime database. Instead use the data from the actual schedule and draw lines.
        graphic_schedule.draw_series(
            coords_estimate
                .iter()
                .map(|coord| Circle::new(*coord, 2, transparent_red.filled())),
        )?;
        // graphic_schedule.draw_series(paths)?;

        Ok(())
    }
}
