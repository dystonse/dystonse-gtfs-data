use chrono::{Datelike, NaiveDate, Weekday};
use clap::ArgMatches;
use gtfs_structures::{Gtfs, Trip};
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;

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

                for (i, st) in trip.stop_times.iter().enumerate() {
                    let rows_matching_stop : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st.stop.id).collect();
                    println!("Stop #{}: {} with {} rows", i, st.stop.name, rows_matching_stop.len());
                }
        
                Ok(())
            }
        }
    }
}