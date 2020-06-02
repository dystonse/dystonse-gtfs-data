use std::fs;

use chrono::{NaiveDate};
use clap::ArgMatches;
use gtfs_structures::Gtfs;
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;
use gnuplot::{Figure, Caption};

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
            self.create_curves_for_route_variant(route_id, *route_variant, &db_items)?;
        }

        Ok(())
    }

    fn create_curves_for_route_variant(&self, route_id: &String, route_variant: u64, db_items: &Vec<DbItem>) -> FnResult<()> {
        let rows_matching_variant : Vec<_> = db_items.iter().filter(|item| item.route_variant == route_variant).collect();
        let route = self.schedule.get_route(route_id)?;
        let variant_as_string = Some(format!("{}", route_variant));
        let trip = self.schedule.trips.values().filter(|trip| trip.route_id == *route_id && trip.route_variant == variant_as_string).next();
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
                let t = 300; 

                for (i_s, st_s) in trip.stop_times.iter().enumerate() {
                    let rows_matching_start : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st_s.stop.id).collect();
                    for (i_e, st_e) in trip.stop_times.iter().enumerate() {
                        if i_e > i_s {
                            let rows_matching_end : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st_e.stop.id).collect();
                            let mut matching_pairs = Vec::<(f32, f32)>::with_capacity(usize::min(rows_matching_start.len(), rows_matching_end.len()));
                            for row_s in &rows_matching_start {
                                for row_e in &rows_matching_end {
                                    if row_s.date == row_e.date && row_s.trip_id == row_e.trip_id {
                                        if let Some(d_s) = row_s.delay_arrival {
                                            if let Some(d_e) = row_e.delay_arrival {
                                                if d_s < t && d_s > -t && d_e < t && d_e > -t {
                                                    matching_pairs.push((d_s as f32, d_e as f32));
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            println!("Stop #{} and #{} have {} and {} rows each, with {} matching", i_s, i_e, rows_matching_start.len(), rows_matching_end.len(), matching_pairs.len());
                            if matching_pairs.len() > 20 {
                                let dirname = format!("data/curve_img/Bremen_1/{}", route_variant);
                                fs::create_dir_all(&dirname)?;
                                let filename = format!("{}/curve_{}_to_{}.svg", &dirname, i_s, i_e);
                                self.generate_curves_for_stop_pair(matching_pairs, &filename)?;
                            }
                        }
                    }
                }
        
                Ok(())
            }
        }
    }

    fn generate_curves_for_stop_pair(&self, mut pairs: Vec<(f32, f32)>, filename: &str) -> FnResult<()> {
        let mut fg = Figure::new();
        let axes = fg.axes2d();

        let count = pairs.len();
        if count > 199 {
            // draw several curves
            let mut buckets : usize = count / 100;
            if buckets > 8 {
                buckets = 8;
            }
            let bucket_size = count / buckets;
            pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            for b in 0..buckets {
                let slice = pairs[bucket_size * b .. bucket_size * b + (bucket_size - 1)].to_vec();
                println!("Doing slice {} of {} with {} pairs.", b, buckets, slice.len());
                self.other(axes, slice)?;
            }
        } else {
            println!("Doing single slice with {} pairs.", pairs.len());
            // draw only one curve
            self.other(axes, pairs)?;
        }

        fg.save_to_svg(filename, 1024, 768)?;

        Ok(())
    }

    fn other(&self, axes: &mut gnuplot::Axes2D, mut pairs: Vec<(f32, f32)>) -> FnResult<()> {
        pairs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let max_i = (pairs.len() - 1) as f32;
        let mut tups = Vec::<Tup<f32, f32>>::with_capacity(pairs.len());
        let mut last_x = 0.0;
        let mut initial_delay_sum = 0.0;
        for (i, pair) in pairs.iter().enumerate() {
            initial_delay_sum += pair.0;
            let x = pair.1;
            if x != last_x {
                tups.push(Tup {x, y: (i as f32) / max_i});
                last_x = x;
            }
        }
        
        if tups.len() < 3 {
            println!("Curve would have only {} points, skipping.", tups.len());
            return Ok(());
        }
        tups.first_mut().unwrap().y = 0.0;
        tups.last_mut().unwrap().y = 1.0;

        let cap = format!("Anfangs ca. {}s zu spät", (initial_delay_sum / pairs.len() as f32) as i32);

        let mut curve = IrregularDynamicCurve::new(tups);
        curve.simplify(0.001);
       
        let (x, y) = curve.get_values_as_vectors();
        axes.lines_points(&x, &y, &[Caption(&cap)]);
    
        Ok(())
    }
}