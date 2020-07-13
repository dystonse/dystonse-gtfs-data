use clap::ArgMatches;
use chrono::{Datelike, NaiveDate, Weekday};
use gtfs_structures::{Gtfs, Trip};
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;
use plotters::palette::LinSrgba;
use plotters::prelude::*;
use plotters::style::text_anchor::*;
use rand::Rng;
use rayon::prelude::*;

use super::Analyser;

use crate::FnResult;
use crate::Main;

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

struct DbItem {
    delay_arrival: Option<i32>,
    delay_departure: Option<i32>,
    date: Option<NaiveDate>,
    trip_id: String,
    stop_id: String
}

impl FromRow for DbItem {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(DbItem{
            delay_arrival: row.get_opt::<i32,_>(0).unwrap().ok(),
            delay_departure: row.get_opt::<i32,_>(1).unwrap().ok(),
            date: row.get_opt(2).unwrap().ok(),
            trip_id: row.get::<String, _>(3).unwrap(),
            stop_id: row.get::<String, _>(4).unwrap()
        })
    }
}


pub struct VisualScheduleCreator<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub args: &'a ArgMatches
}

impl<'a> VisualScheduleCreator<'a> {
    pub fn run_visual_schedule(&mut self) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        if let Some(route_ids) = self.args.values_of("route-ids") {
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                self.create_visual_schedule_for_route(&String::from(route_id))?;
            }
        }
        if let Some(shape_ids) = self.args.values_of("shape-ids") {
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
        if self.args.is_present("all") {
            println!("Creating graphs for all routes. First, selecting route_ids for which we actually have data…");

            let mut con = self.main.pool.get_conn()?;

            let stmt = con.prep(r"SELECT DISTINCT route_id FROM records WHERE `source`=?")?;
            let result = con.exec_iter(&stmt, (&self.main.source,))?;
            let route_ids: Vec<String> = result
                .map(|row| {
                    let id: String = from_row(row.unwrap());
                    id
                })
                .collect();

            println!(
                "Found data for {} of {} route_ids.",
                route_ids.len(),
                schedule.routes.len()
            );

            let success_counter = AtomicUsize::new(0);
            let error_counter = AtomicUsize::new(0);
            let total_count = route_ids.len();

            let (count, success) = route_ids
                .par_iter()
                .map(|id| match self.create_visual_schedule_for_route(&id) {
                    Ok(()) => {
                        let curr_suc = 1 + success_counter.fetch_add(1, Ordering::SeqCst);
                        let curr_err = error_counter.load(Ordering::SeqCst);
                        println!(
                            "Status: {} of {} ({} succeeded, {} errors)",
                            curr_suc + curr_err, total_count, curr_suc, curr_err
                        );
                        (1, 1)
                    },
                    Err(e) => {
                        let curr_err = 1 + error_counter.fetch_add(1, Ordering::SeqCst);
                        let curr_suc = error_counter.load(Ordering::SeqCst);
                        println!(
                            "Status: {} of {} ({} succeeded, {} errors)",
                            curr_suc + curr_err, total_count, curr_suc, curr_err
                        );
                        eprintln!("Error while processing route {}: {}", &id, e);
                        (1, 0)
                    }
                 })
                .reduce( 
                    || (0, 0), 
                    |a, b| (a.0 + b.0, a.1 + b.1)
                );
            println!(
                "Tried to create graphs for {} routes, had success with {} of them.",
                count, success
            );
        }

        Ok(())
    }

    fn create_visual_schedule_for_route(&self, route_id: &String) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        let mut con = self.main.pool.get_conn()?;
        let stmt = con.prep(
            r"SELECT 
                delay_arrival,
                delay_departure,
                trip_start_date,
                trip_start_time,
                trip_id,
                stop_id
            FROM 
                records 
            WHERE 
                source=:source AND 
                route_id=:routeid
            ORDER BY 
                trip_start_date,
                trip_id",
        )?;

        let mut result = con.exec_iter(
            &stmt,
            params! {
                "source" => &self.main.source,
                "routeid" => route_id
            },
        )?;

        let result_set = result.next_set().unwrap()?;

        let db_items: Vec<_> = result_set
            .map(|row| {
                let item: DbItem = from_row(row.unwrap());
                item
            })
            .collect();

        if db_items.len() < 10 {
            println!(
                "Skipping route id {} because there are only {} data points.",
                route_id,
                db_items.len()
            );
            return Ok(());
        }

        // collect trips and route variants for this route
        let all_trips = &schedule.trips;

        let trips_of_route: Vec<&Trip> = all_trips
            .values()
            .filter(|trip| trip.route_id == *route_id)
            .collect();

        let route_variant_ids: HashSet<String> = trips_of_route
            .iter()
            .filter_map(|trip| trip.route_variant.clone())
            .collect();

        // for each distinct route_variant_id, find a trip, and from there, the list of stop_ids
        let mut stop_ids_by_route_variant_id: Vec<(&String, Vec<String>)> = route_variant_ids
            .iter()
            .map(|route_variant_id| {
                let trip = trips_of_route
                    .iter()
                    .filter(move |trip| trip.route_variant.as_ref().unwrap() == route_variant_id)
                    .next()
                    .unwrap();

                (route_variant_id, trip)
            })
            .map(|(route_variant_id, trip)| {
                let stop_ids: Vec<String> = trip
                    .stop_times
                    .iter()
                    .map(|stop_time| stop_time.stop.id.clone())
                    .collect();
                (route_variant_id, stop_ids)
            })
            .collect();

        // sort those items by length, descending
        stop_ids_by_route_variant_id
            .sort_by_key(|(_route_variant_id, stop_ids)| -(stop_ids.len() as i32));

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
        loop {
            let (primary_route_variant_id, primary_stop_ids) =
                stop_ids_by_route_variant_id.first().unwrap();
            let mut reverse_primary_stop_ids = primary_stop_ids.clone();
            reverse_primary_stop_ids.reverse();

            let matching_route_variant_ids : Vec<&String> = stop_ids_by_route_variant_id
                .iter()
                .filter(|(_route_variant_id, stop_ids)| {
                    self.is_sub_trip(primary_stop_ids, stop_ids)
                        || self.is_sub_trip(&reverse_primary_stop_ids, stop_ids)
                })
                .map(|(route_variant_id, _stop_ids)| route_variant_id.clone())
                .collect();

            self.create_visual_schedule_for_route_variants(
                primary_route_variant_id,
                matching_route_variant_ids.clone(),
                &db_items,
                &agency_name,
                &route_name,
            )?;

            stop_ids_by_route_variant_id.retain(|(route_variant_id, _stop_ids)| {
                !matching_route_variant_ids.contains(route_variant_id)
            });

            if stop_ids_by_route_variant_id.is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn is_sub_trip(&self, super_stop_ids: &Vec<String>, sub_stop_ids: &Vec<String>) -> bool {
        // from https://stackoverflow.com/a/35907071
        super_stop_ids
            .windows(sub_stop_ids.len())
            .position(|window| *window == sub_stop_ids[..])
            .is_some()
    }

    fn create_visual_schedule_for_route_variants(
        &self,
        primary_route_variant_id: &String,
        route_variant_ids: Vec<&String>,
        db_items: &Vec<DbItem>,
        agency_name: &str,
        route_name: &str,
    ) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        let all_trips = &schedule.trips;
        let empty_string = String::from("");

        // select any trip with the primary route variant as the primary trip (the one that covers all stations in the output image)
        let primary_trip = all_trips
            .values()
            .filter(|trip| {
                trip.route_variant.as_ref().unwrap_or(&empty_string) == primary_route_variant_id
            })
            .next()
            .unwrap();

        // gather trips for all selected route variants
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

        let filename = if route_variant_ids.len() > 1 {
            format!("{}/variant_{}_and_{}_others.png", path, primary_route_variant_id, route_variant_ids.len() - 1)
        } else {
            format!("{}/variant_{}.png", path, primary_route_variant_id)
        };

        self.create_visual_schedule_for_trips(
            primary_trip,
            trips,
            &filename,
            db_items,
        )
    }

    fn create_visual_schedule_for_shapes(
        &self,
        primary_shape_id: &String,
        shape_ids: Vec<&String>,
        db_items: &Vec<DbItem>,
        agency_name: &str,
        route_name: &str,
    ) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        let all_trips = &schedule.trips;
        let empty_string = String::from("");
        let primary_trip: &Trip = all_trips
            .values()
            .filter(|trip| trip.shape_id.as_ref().unwrap_or(&empty_string) == primary_shape_id)
            .next()
            .unwrap();

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
            db_items,
        )
    }

    fn create_visual_schedule_for_trips(
        &self,
        primary_trip: &Trip,
        trips: Vec<&Trip>,
        name: &str,
        db_items: &Vec<DbItem>,
    ) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        let mut creator = GraphCreator::new(
            String::from(name),
            primary_trip,
            trips,
            schedule,
            self.main,
            db_items,
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
    _main: &'a Main,
    relevant_stop_ids: Vec<String>,
    relevant_stop_names: Vec<String>,
    db_items: &'a Vec<DbItem>,
}

impl<'a> GraphCreator<'a> {
    fn new(
        name: String,
        primary_trip: &'a Trip,
        trips: Vec<&'a Trip>,
        schedule: &'a Gtfs,
        main: &'a Main,
        db_items: &'a Vec<DbItem>,
    ) -> GraphCreator<'a> {
        GraphCreator {
            primary_trip,
            trips,
            name,
            _main: main,
            schedule,
            relevant_stop_ids: Vec::new(),
            relevant_stop_names: Vec::new(),
            db_items,
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

        let data_for_current_trips: Vec<&DbItem> = self
            .db_items
            .iter()
            .filter(|it| self.trips.iter().any(|trip| trip.id == it.trip_id))
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
            .filter_map(|it| it.date)
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
                .filter(|it| it.date == Some(date));

            // group the data by trip_id
            for (_trip_id, items) in &data_of_the_day.group_by(|it| it.trip_id.clone()) {
                // for each trip_id, sort by the stop_id's position in the list of relevant_stop_ids
                let sorted_items = items
                    .sorted_by_key(|it| self.relevant_stop_ids.iter().position(|id| *id == it.stop_id));

                let path_for_trip = PathElement::new(
                    sorted_items
                        .filter_map(|it| self.make_coordinate_from_item(it))
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
            BitMapBackend::new(&self.name, (stop_count as u32 * 30 + 40, 4096)).into_drawing_area();

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

    fn make_coordinate_from_item(&self, item: &DbItem) -> Option<(f64, f64)> {
        if item.delay_arrival.is_none() || item.delay_departure.is_none() {
            return None;
        }

        // Some providers seem to set the delay to 0 instead of Null when they have no data.
        if item.delay_arrival.unwrap() == 0 {
            return None;
        }

        let trip = self.schedule.get_trip(&item.trip_id).unwrap();
        // TODO there must be a prettier way to handle those cases:
        let a = trip.stop_times.iter().filter(|stop_time| stop_time.stop.id == item.stop_id).next();
        if a.is_none() {
            return None;
        }
        let b = a.unwrap().departure_time;
        if b.is_none() {
            return None;
        }
        let start_time = b.unwrap();

        self.make_coordinate(
            &item.stop_id,
            Some((item.delay_arrival.unwrap() + start_time as i32) as u32), 
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