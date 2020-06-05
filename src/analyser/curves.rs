use std::fs;

use chrono::{NaiveDate};
use clap::ArgMatches;
use gtfs_structures::{Gtfs, Route};
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;
use gnuplot::*;

use dystonse_curves::irregular_dynamic::*;
use dystonse_curves::{Curve};

use super::Analyser;

use crate::FnResult;
use crate::Main;

struct DbItem {
    delay_arrival: Option<i32>,
    delay_departure: Option<i32>,
    date: Option<NaiveDate>,
    trip_id: String,
    stop_id: String,
    route_variant: u64
}

impl FromRow for DbItem {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(DbItem{
            delay_arrival: row.get_opt::<i32,_>(0).unwrap().ok(),
            delay_departure: row.get_opt::<i32,_>(1).unwrap().ok(),
            date: row.get_opt(2).unwrap().ok(),
            trip_id: row.get::<String, _>(3).unwrap(),
            stop_id: row.get::<String, _>(4).unwrap(),
            route_variant: row.get::<u64, _>(5).unwrap(),
        })
    }
}

pub struct CurveCreator<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub schedule: Gtfs,
    pub args: &'a ArgMatches
}

impl<'a> CurveCreator<'a> {

    pub fn run_curves(&self) -> FnResult<()> {
        if let Some(route_ids) = self.args.values_of("route-ids") {
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                self.create_curves_for_route(&String::from(route_id))?;
            }
        } else {
            println!("I've got no route!");
        }
        Ok(())
    }

    fn create_curves_for_route(&self, route_id: &String)  -> FnResult<()> {
        let route = self.schedule.get_route(route_id)?;
        let agency_id = route.agency_id.as_ref().unwrap().clone();
        let agency_name = self.schedule
            .agencies
            .iter()
            .filter(|agency| agency.id.as_ref().unwrap() == &agency_id)
            .next()
            .unwrap()
            .name
            .clone();

        println!("Working on route {} of agency {}.", route.short_name, agency_name);

        let mut con = self.main.pool.get_conn()?;
        let stmt = con.prep(
            r"SELECT 
                delay_arrival,
                delay_departure,
                date,
                trip_id,
                stop_id,
                route_variant
            FROM 
                realtime 
            WHERE 
                source=:source AND 
                route_id=:routeid
            ORDER BY 
                date,
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

        let route_variants : Vec<_> = db_items.iter().map(|item| &item.route_variant).unique().collect();
        println!("For route {} there are {} variants: {:?}", route_id, route_variants.len(), route_variants);

        for route_variant in route_variants {
            self.create_curves_for_route_variant(&route, *route_variant, &agency_name, &db_items)?;
        }

        Ok(())
    }

    fn create_curves_for_route_variant(&self, route: &Route, route_variant: u64, agency_name: &str, db_items: &Vec<DbItem>) -> FnResult<()> {
        let rows_matching_variant : Vec<_> = db_items.iter().filter(|item| item.route_variant == route_variant).collect();

        let variant_as_string = Some(format!("{}", route_variant));
        let trip = self.schedule.trips.values().filter(|trip| trip.route_id == *route.id && trip.route_variant == variant_as_string).next();
        match trip {
            None => {
                println!("Could not find trip {}.", rows_matching_variant[0].trip_id);
                Ok(())
            },
            Some(trip) => {
                println!("Matching rows for route variant {} of route {}: {}", route_variant, route.short_name, rows_matching_variant.len());

                // threshold of delay secends that will be considered. 
                // Every stop with more than t or less then -t delay will be ignored.
                // TODO This is for testing / visualizing only!
                let t = 500; 

                // We need to make an image for each pair of start and end station along the route where
                // the end station comes after the start station.AccessMode

                // Iterate over all start stations
                for (i_s, st_s) in trip.stop_times.iter().enumerate() {
                    // Locally select the rows which match the start station
                    let rows_matching_start : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st_s.stop.id).collect();
                    // Iterate over end stations, and only use the ones after the start station
                    for (i_e, st_e) in trip.stop_times.iter().enumerate() {
                        if i_e > i_s {
                            // Locally select rows that are matching the end station
                            let rows_matching_end : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st_e.stop.id).collect();
                            
                            // now rows_matching_start and rows_matching_end are disjunctive sets which can be joined by their vehicle
                            // which is given by (data, trip_id).
                            let mut matching_pairs = Vec::<(f32, f32)>::with_capacity(usize::min(rows_matching_start.len(), rows_matching_end.len()));
                            for row_s in &rows_matching_start {
                                for row_e in &rows_matching_end {
                                    if row_s.date == row_e.date && row_s.trip_id == row_e.trip_id {
                                        // Only use rows where delay_arrival is not None
                                        // TODO filter those out at the DB level or in the above filter expressions
                                        if let Some(d_s) = row_s.delay_arrival {
                                            if let Some(d_e) = row_e.delay_arrival {
                                                // Filter out rows with too much positive or negative delay
                                                if d_s < t && d_s > -t && d_e < t && d_e > -t {
                                                    // Now we round the delays to multiples of 12. Much of the data that we get from the agencies
                                                    // tends to be rounded that way, and mixing up rounded and non-rounded data leads to all
                                                    // kinds of problems.
                                                    let rounded_d_s = (d_s / 12) * 12;
                                                    let rounded_d_e = (d_e / 12) * 12;
                                                    matching_pairs.push((rounded_d_s as f32, rounded_d_e as f32));
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            // For the start station i_s and the end station i_e we now have a collection of matching
                            // pairs of observations, i.e. each pair means:
                            // "The vehicle which had p.0 delay at i_s arrived with p.1 delay at i_e."

                            println!("Stop #{} and #{} have {} and {} rows each, with {} matching", i_s, i_e, rows_matching_start.len(), rows_matching_end.len(), matching_pairs.len());
                            
                            // Don't generate a graphic if we have too few pairs.
                            if matching_pairs.len() > 20 {
                                let dirname = format!("data/curve_img/{}/Linie_{}/{}", agency_name, route.short_name, route_variant);
                                fs::create_dir_all(&dirname)?;
                                let filename = format!("{}/curve_{}_to_{}.svg", &dirname, i_s, i_e);
                                let title = &format!("{} Linie {} - Verspätungsentwicklung von #{} '{}' bis #{} '{}'", agency_name, route.short_name, i_s, st_s.stop.name, i_e, st_e.stop.name);
                                self.generate_curves_for_stop_pair(matching_pairs, &filename, &title)?;
                            }
                        }
                    }
                }
        
                Ok(())
            }
        }
    }

    fn generate_curves_for_stop_pair(&self, pairs: Vec<(f32, f32)>, filename: &str, title: &str) -> FnResult<()> {
        let mut fg = Figure::new();
        fg.set_title(title);
        let axes = fg.axes2d();
        axes.set_legend(
            Graph(0.0), 
            Graph(0.95), 
            &[Title("Sekunden (Anzahl Fahrten)"), Placement(AlignLeft, AlignTop)], 
            &[]
        );

        // Clone the pairs so that we may sort them. We sort them by delay at the start station
        // because we wukk group them by that criterion.
        let mut own_pairs = pairs.clone();
        own_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let count = own_pairs.len();

        // Try to make a curve out of initial delays. This curve is different from the actual
        // output curve(s), but is needed as a intermediate result to compute the markers.
        if let Some(initial_curve) = self.make_curve_from_pairs(&own_pairs, true) {
            // We build a list of "markers", which are x-coordinates / initial delays for which we 
            // will build a curve. That curve will consist of rows with "similar" delays.
            // We add the minimal marker, next all markers in between, and the the maximal marker.
            let mut markers : Vec::<f32> = vec!{initial_curve.min_x()};
            self.recurse(&initial_curve, &mut markers, initial_curve.min_x(), initial_curve.max_x(), count as f32);
            markers.push(initial_curve.max_x());

            
            // draw the initial delay curve, which is just for debugging purposes and might be a bit confusing.
            let (x, y) = initial_curve.get_values_as_vectors();
            let caption_all_initial = format!("Anfangs - alle Daten ({})", own_pairs.len());
            axes.lines_points(&x, &y, &[LineStyle(Dot), LineWidth(3.0), Caption(&caption_all_initial), Color("#129245")]);
            
            // draw the overall destination delay
            if let Some(mut curve) = self.make_curve_from_pairs(&own_pairs, false) {
                curve.simplify(0.001);
                let (x, y) = curve.get_values_as_vectors();
                let caption_all_end = format!("Ende - alle Daten ({})", own_pairs.len());
                axes.lines_points(&x, &y, &[LineStyle(Dot), LineWidth(3.0), Caption(&caption_all_end), Color("#08421F")]);
            }

            // now generate and draw one or more actual result curves.
            // TODO the following is just a quick and dirty implementation which does not use
            // the computed markers as we intended.
            for (i,(lower, upper)) in markers.iter().tuple_windows().enumerate() {
                let min_index = (count as f32 * initial_curve.y_at_x(*lower)) as usize;
                let max_index = (count as f32 * initial_curve.y_at_x(*upper)) as usize;
                let slice = own_pairs[min_index .. max_index].to_vec();
                if slice.len() > 1 {
                    println!("Doing slice from {}:{} to {}:{}.", min_index, lower, max_index, upper);
                    let color = format!("#{:x}", colorous::PLASMA.eval_rational(i, markers.len()));
                    self.draw_to_figure(axes, &slice, &color)?;
                }
            }
            fg.save_to_svg(filename, 1024, 768)?;
        }

        Ok(())
    }

    // This method determines whether there should be another marker between the ones already present at lower and upper.
    // Upper and lower are initial delay by seconds.
    fn recurse(&self, initial_delay_curve: &IrregularDynamicCurve<f32, f32>, markers: &mut Vec<f32>, lower: f32, upper: f32, count: f32) {
        // let's recap what initial_delay_curve is: Along the x axis, we have the initial delays in seconds. Along the y axis,
        // we have the share of vehicles which had this delay or less. We need the count to make that into abolute numbers.

        // new marker mus be at least 20 seconds away from the existing ones
        let min_x_by_delay = lower + 20.0;
        let max_x_by_delay = upper - 20.0;

        // between the new marker and existing ones, at least 20 data points must exist
        // this computation is tedious because y is measured relatively but we have an
        // absolute distance (20 datapoints) to keep. 
        let lower_y = initial_delay_curve.y_at_x(lower);
        let upper_y = initial_delay_curve.y_at_x(upper);
        let min_y_by_count = lower_y + (20.0 / count);
        let max_y_by_count = upper_y - (20.0 / count);
       
        // Also, we need x bounds:
        let min_x_by_count = initial_delay_curve.x_at_y(min_y_by_count);
        let max_x_by_count = initial_delay_curve.x_at_y(max_y_by_count);
        
        // For the x axis, we have two minimum and two maximum bounds.
        // Let's find the stricter ones.
        let min_x = f32::max(min_x_by_delay, min_x_by_count);
        let max_x = f32::min(max_x_by_delay, max_x_by_count);

        // The bounds might contradict, and in that case, we won't subdivide
        if min_x <= max_x {
            let mid_x = (min_x + max_x) / 2.0;
            self.recurse(initial_delay_curve, markers, lower, mid_x, count);
            markers.push(mid_x);
            self.recurse(initial_delay_curve, markers, mid_x, upper, count);
        }
    }

    fn draw_to_figure(&self, axes: &mut gnuplot::Axes2D, pairs: &Vec<(f32, f32)>, color: &str) -> FnResult<()> {
        let min_delay = pairs.first().unwrap().0;
        let max_delay = pairs.last().unwrap().0;

        let cap = format!("{}s bis {}s ({})", min_delay, max_delay, pairs.len());

        if let Some(mut curve) = self.make_curve_from_pairs(&pairs, false) {
            curve.simplify(0.001);
            let (x, y) = curve.get_values_as_vectors();
            let width = if color == "black" { 2.0 } else { 1.0 };
            axes.lines_points(&x, &y, &[Caption(&cap), PointSize(0.6), Color(color), LineWidth(width)]);
        }
    
        Ok(())
    }
 
    fn make_curve_from_pairs(&self, pairs: &Vec<(f32, f32)>, use_initial_delay: bool) -> Option<IrregularDynamicCurve<f32, f32>> {
        let mut own_pairs = pairs.clone();
        if use_initial_delay {
            own_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        } else {
            own_pairs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        }
        let max_i = (own_pairs.len() - 1) as f32;
        let mut tups = Vec::<Tup<f32, f32>>::with_capacity(own_pairs.len());
        let mut last_x = 0.0;
        for (i, pair) in own_pairs.iter().enumerate() {
            let x = if use_initial_delay { pair.0 } else { pair.1 };
            if x != last_x {
                tups.push(Tup {x, y: (i as f32) / max_i});
                last_x = x;
            }
        }

        if tups.len() < 2 {
            println!("Curve would have only {} points, skipping.", tups.len());
            return None;
        }

        tups.first_mut().unwrap().y = 0.0;
        tups.last_mut().unwrap().y = 1.0;

        Some(IrregularDynamicCurve::new(tups))
    }
}