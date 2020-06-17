use std::fs;
use std::fs::File;
use std::io::prelude::*;

use chrono::{NaiveDate};
use clap::ArgMatches;
use gtfs_structures::{Gtfs, Trip};
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;
use simple_error::bail;

use dystonse_curves::irregular_dynamic::*;
use dystonse_curves::{Curve, curve_set::CurveSet};

use super::Analyser;
use super::route_data::*;

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
            // TODO implement handling the "all" arg
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

        let mut route_data = RouteData::new();

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
            let variant_as_string = Some(format!("{}", route_variant));
            let trip = self.schedule.trips.values().filter(|trip| trip.route_id == *route.id && trip.route_variant == variant_as_string).next();

            match trip {
                None => {
                    println!("Could not find trip for route_variant {}.", route_variant);
                },
                Some(trip) => {
                    let rows_matching_variant : Vec<_> = db_items.iter().filter(|item| item.route_variant == *route_variant).collect();

                    let variant_data = self.create_curves_for_route_variant(&rows_matching_variant, trip)?;
                    route_data.variants.insert(*route_variant, variant_data);
                }
            }
        }

        let serialized_bin = rmp_serde::to_vec(&route_data).unwrap();
        let dir_name = format!("data/curve_data/{}", agency_name);
        fs::create_dir_all(&dir_name)?;    
        let file_name = format!("{}/Linie_{}.crv", dir_name, route.short_name);
        let mut file = match File::create(&file_name) {
            Err(why) => panic!("couldn't create file: {}", why),
            Ok(file) => file,
        };
        match file.write_all(&serialized_bin) {
            Err(why) => panic!("couldn't write: {}", why),
            Ok(_) => println!("successfully wrote."),
        }

        // Print as json for debugging:
        // let serialized = serde_json::to_string(&curve_set).unwrap();
        // println!("serialized = {}", serialized);

        Ok(())
    }

    fn create_curves_for_route_variant(
        &self, 
        rows_matching_variant: &Vec<&DbItem>, 
        trip: &Trip
    ) -> FnResult<RouteVariantData> {
        let mut route_variant_data = RouteVariantData::new();
        route_variant_data.stop_ids = trip.stop_times.iter().map(|st| st.stop.id.clone()).collect();

        // threshold of delay secends that will be considered. 
        // Every stop with more than t or less then -t delay will be ignored.
        let t = 3000; 

        // Iterate over all start stations
        for (i_s, st_s) in trip.stop_times.iter().enumerate() {
            // Locally select the rows which match the start station
            let rows_matching_start : Vec<_> = rows_matching_variant.into_iter().filter(|item| item.stop_id == st_s.stop.id).map(|i| *i).collect();

            if let Ok(res) = self.generate_delay_curve(&rows_matching_start, true) {
                route_variant_data.general_delay_arrival.insert(i_s as u32, res);
            }
            if let Ok(res) = self.generate_delay_curve(&rows_matching_start, false) {
                route_variant_data.general_delay_departure.insert(i_s as u32, res);
            }
            
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
                                // Only use rows where delay is not None
                                // TODO filter those out at the DB level or in the above filter expressions
                                if let Some(d_s) = row_s.delay_departure {
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

                    // println!("Stop #{} and #{} have {} and {} rows each, with {} matching", i_s, i_e, rows_matching_start.len(), rows_matching_end.len(), matching_pairs.len());
                    
                    // Don't generate a graphic if we have too few pairs.
                    if matching_pairs.len() > 20 {
                        let stop_pair_data = self.generate_curves_for_stop_pair(matching_pairs);
                        if let Ok(actual_data) = stop_pair_data {
                            route_variant_data.curve_sets.insert((i_s as u32, i_e as u32), actual_data);
                        }
                    }
                }
            }
        }
         Ok(route_variant_data)
    }

    fn generate_delay_curve(&self, rows: &Vec<&DbItem>, use_arrival_times: bool) -> FnResult<IrregularDynamicCurve<f32,f32>> {
        let values: Vec<f32> = if use_arrival_times {
            rows.iter().filter_map(|r| r.delay_arrival).map(|t| t as f32).collect()
        } else {
            rows.iter().filter_map(|r| r.delay_departure).map(|t| t as f32).collect()
        };
        if values.len() < 20 {
            bail!("Less than 20 data rows.");
        }
        let mut curve = Self::make_curve(&values, None).unwrap().0;
        curve.simplify(0.01);
        Ok(curve)
    }

    fn generate_curves_for_stop_pair(&self, pairs: Vec<(f32, f32)>) -> FnResult<CurveSet<f32, IrregularDynamicCurve<f32,f32>>> {
        // Clone the pairs so that we may sort them. We sort them by delay at the start station
        // because we will group them by that criterion.
        let mut own_pairs = pairs.clone();
        own_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let count = own_pairs.len();

        // Try to make a curve out of initial delays. This curve is different from the actual
        // output curve(s), but is needed as a intermediate result to compute the markers.
        if let Ok((initial_curve, _sum)) = Self::make_curve(&own_pairs.iter().map(|(s,_e)| *s).collect(), None) {
            // We build a list of "markers", which are x-coordinates / initial delays for which we 
            // will build a curve. That curve will consist of rows with "similar" delays.
            // All the "middle" will be inserted in-order by the recurse function. 
            // We need to add the absolute min and absolute max markers manually before and afer that,
            // and we add them twice because this simplifies the curve generation later on.
            let mut markers = Vec::<f32>::new();
            markers.push(initial_curve.min_x());
            markers.push(initial_curve.min_x());
            self.recurse(&initial_curve, &mut markers, initial_curve.min_x(), initial_curve.max_x(), count as f32);
            markers.push(initial_curve.max_x());
            markers.push(initial_curve.max_x());
            
            let mut curve_set = CurveSet::<f32, IrregularDynamicCurve<f32, f32>>::new();
            // Now generate and draw one or more actual result curves.
            // Each cuve will focus on the mid marker, and include all the data points from
            // the min to the max marker.
            // Remember that we added the absolute min and absolute max markers twice.
            for (lower, mid, upper) in markers.iter().tuple_windows() {
                let min_index = (count as f32 * initial_curve.y_at_x(*lower)) as usize;
                let max_index = (count as f32 * initial_curve.y_at_x(*upper)) as usize;
                let slice : Vec<f32> = own_pairs[min_index .. max_index].iter().map(|(_s,e)| *e).collect();
                if slice.len() > 1 {
                    if let Ok((mut curve, _sum)) = Self::make_curve(&slice,  Some(*mid)) {
                        curve.simplify(0.001);
                        if curve.max_x() <  curve.min_x() + 13.0 {
                            continue;
                        }
            
                        curve_set.add_curve(*mid, curve);
                    }
                }
            }
            return Ok(curve_set);
        }

        bail!("Could not make curve.");
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

    fn get_weight(delay: f32, focus: Option<f32>, min_delay: f32, max_delay: f32) -> f32 {
        // handling delay values outside of given bounds: always 0.
        if delay < min_delay || delay > max_delay {
            return 0.0;
        }

        if let Some(focus) = focus {
            // if focus is given, weight is 1 at the focus and goes down to zero 
            // towards the bounds given by min_delay and max_delay
            if delay == focus {
                1.0
            } else if delay < focus {
                (delay - min_delay) / (focus - min_delay)
            } else {
                1.0 - ((delay - focus) / (max_delay - focus))
            }
        } else {
            1.0
        }
    }

    pub fn make_curve(values: &Vec<f32>, focus: Option<f32>) -> FnResult<(IrregularDynamicCurve<f32, f32>, f32)> {
        let mut own_values = values.clone(); // TODO maybe we don't need to clone this
        own_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min_delay = *own_values.first().unwrap();
        let max_delay = *own_values.last().unwrap();
      
        let pairs: Vec<(f32,f32)> = own_values.iter().map(|v| (*v, Self::get_weight(*v, focus, min_delay, max_delay))).collect();

        let sum_of_weights: f32 = pairs.iter().map(|(_v, w)| *w).sum();

        let mut tups = Vec::<Tup<f32, f32>>::with_capacity(own_values.len());
        let mut last_x :f32 = 0.0;
        let mut i = 0.0;
        for (x, w) in pairs.iter() {
            i += w;
            if *x != last_x {
                tups.push(Tup {x: *x, y: (i as f32) / sum_of_weights});
                last_x = *x;
            }
        }

        if tups.len() < 2 {
            bail!("Curve would have only {} points, skipping.", tups.len());
        }

        tups.first_mut().unwrap().y = 0.0;
        tups.last_mut().unwrap().y = 1.0;

        Ok((IrregularDynamicCurve::new(tups), sum_of_weights))
    }
}