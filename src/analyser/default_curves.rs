use std::collections::{HashSet, HashMap};
use std::u16;

use super::time_slots::TimeSlot;
use super::curve_analysis::CurveCreator;
use super::route_data::DbItem;
use super::route_sections::*;

use clap::ArgMatches;
use gtfs_structures::{Gtfs, Route, RouteType};
use mysql::*;
use mysql::prelude::*;
use serde::{Serialize, Deserialize};

use dystonse_curves::irregular_dynamic::*;

use super::Analyser;

use crate::{FnResult, Main, EventType, SerdeFormat, save_to_file};



// curves based on less than this number of data will be discarded:
const MIN_DATA_FOR_CURVE : usize = 10; 

/// a struct to hold a hash map of all the default curves
#[derive(Debug, Serialize, Deserialize)]
pub struct DefaultCurves {
    pub all_default_curves: HashMap<(RouteType, RouteSection, TimeSlot, EventType), 
        IrregularDynamicCurve<f32, f32>>
}

impl DefaultCurves {
    pub fn new() -> Self {
        return Self {
            all_default_curves: HashMap::new()
        };
    }

    // TODO: This is just a dummy and does not actually do anything yet!!!
    pub fn load_from_file(file_path: &str) -> FnResult<DefaultCurves> {
        return Ok(DefaultCurves::new());
    }
}

/// Create default curves for predictions on routes for which we don't have realtime data
/// Default curves are computed for delay_arrival and delay_departure 
/// and are identified by route_type, time_slot and route_section.
/// The calculations are based on the routes for which we have historic realtime data, 
/// but the curves are intended to be used for any prediction, identified by the criteria mentioned above.

pub struct DefaultCurveCreator<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub schedule: Gtfs,
    pub args: &'a ArgMatches
}

impl<'a> DefaultCurveCreator<'a> {

    pub fn run_default_curves(&self) -> FnResult<()> {

        let route_types = [
            RouteType::Tramway,
            RouteType::Subway,
            RouteType::Rail,
            RouteType::Bus,
            RouteType::Ferry
            ];
            
        let route_sections = [
            RouteSection::Beginning, 
            RouteSection::Middle, 
            RouteSection::End
            ];

        //data structures to collect all default curves:
        let mut default_arrival_curves : 
            HashMap<(&RouteType, &RouteSection, &TimeSlot), 
                Vec<IrregularDynamicCurve<f32, f32>>> = HashMap::new();
        let mut default_departure_curves : 
            HashMap<(&RouteType, &RouteSection, &TimeSlot), 
                Vec<IrregularDynamicCurve<f32, f32>>> = HashMap::new();

        // initialize them with empty vectors
        for rt in &route_types {
            for rs in &route_sections {
                for ts in &TimeSlot::TIME_SLOTS {
                    default_arrival_curves.insert((rt, rs, ts), Vec::new());
                    default_departure_curves.insert((rt, rs, ts), Vec::new());
                }
            }
        }

        //iterate over route types
        for rt in route_types.iter() {
            println!("Starting with route type {:?}", rt);

            //find all routes for this type
            let routes = self.get_routes_for_type(*rt);

            //find all their route variants
            let mut route_variants : Vec<(String, &str)> = Vec::new();
            for r in &routes {
                route_variants.extend(self.get_variants_for_route(r));
            }

            println!("Found {} route variants in {} routes", route_variants.len(), routes.len());

            //iterate over route variants
            for (ri, rv) in route_variants {


                //find one trip of this variant
                let trip = self.schedule.trips.values().filter(
                        |trip| trip.route_variant.as_ref().unwrap() == rv
                    ).next().unwrap();

                // take the list of stops from this trip
                let rv_stops = &trip.stop_times;

                //find the borders between the route sections
                let mut max_beginning_stop : u16 = 0;
                let mut max_middle_stop : u16 = 0;

                for s in rv_stops {
                    let sec : RouteSection = get_route_section(&self.schedule, &trip.id, &s.stop.id);
                    if sec == RouteSection::Beginning {
                        max_beginning_stop = s.stop_sequence;
                    }
                    else if sec == RouteSection::Middle {
                        max_middle_stop = s.stop_sequence;
                    }
                }
                //...now the borders should be known.

                println!("For route variant {} with {} stops, the route sections are at {} and {}.",
                    rv, rv_stops.len(), max_beginning_stop, max_middle_stop);

                // Get rt data from the database for all route sections in this route variant
                // TODO: fix this, because it panics if anything went wrong in the database connection etc.!
                let beginning_data = self.get_data_from_db(&ri, &rv, 0, max_beginning_stop).unwrap();
                let middle_data = self.get_data_from_db(&ri, &rv, max_beginning_stop + 1, max_middle_stop).unwrap();
                let end_data = self.get_data_from_db(&ri, &rv, max_middle_stop + 1, u16::MAX).unwrap();

                // for each of these sections, separate the data into time slots
                let beginning_data_by_timeslot = self.sort_dbitems_by_timeslot(beginning_data).unwrap();
                let middle_data_by_timeslot = self.sort_dbitems_by_timeslot(middle_data).unwrap();
                let end_data_by_timeslot = self.sort_dbitems_by_timeslot(end_data).unwrap();

                // TODO: catch errors when beginning/middle/end data was empty!

                // make a hashmap on one more meta level from what we have until here
                let mut data_by_route_section_and_timeslot : 
                    HashMap<RouteSection, HashMap<&TimeSlot, Vec<DbItem>>> = HashMap::new();

                data_by_route_section_and_timeslot.insert(RouteSection::Beginning, beginning_data_by_timeslot);
                data_by_route_section_and_timeslot.insert(RouteSection::Middle, middle_data_by_timeslot);
                data_by_route_section_and_timeslot.insert(RouteSection::End, end_data_by_timeslot);

                // for each time slot in each section, make two curves (delay for arrival and depature)
                for rs in &route_sections {
                    for ts in &TimeSlot::TIME_SLOTS {
                        println!("Create curves for section {:?} and time slot {}.", rs, ts.description);

                        // collect delays in vectors:
                        let arrival_delays : Vec<f32> = 
                            data_by_route_section_and_timeslot[rs][ts].iter()
                                .filter_map(|item| item.delay_arrival).map(|i| i as f32).collect();
                        let departure_delays : Vec<f32> = 
                            data_by_route_section_and_timeslot[rs][ts].iter()
                                .filter_map(|item| item.delay_departure).map(|i| i as f32).collect();

                        // create curves from the vectors and put them into the big hashmap:
                        if arrival_delays.len() >= MIN_DATA_FOR_CURVE {
                            if let Ok(arrival_curve) = CurveCreator::make_curve(&arrival_delays, None) {
                                let mut curve = arrival_curve.0;
                                curve.simplify(0.001);
                                default_arrival_curves.get_mut(&(rt, rs, *ts)).unwrap().push(curve);
                            }
                        }
                        if departure_delays.len() >= MIN_DATA_FOR_CURVE {
                            if let Ok(departure_curve) = CurveCreator::make_curve(&departure_delays, None) {
                                let mut curve = departure_curve.0;
                                curve.simplify(0.001);
                                default_departure_curves.get_mut(&(rt, rs, *ts)).unwrap().push(curve);
                            }
                        }
                    }
                }

            }
            
        }

        println!("Done with curves for each route variant, now computing average curves…");

        // on each leaf of the trees, there is now a vector of curves 
        // with one curve for each route_variant.
        // the next step is to interpolate between all those curves so that we have 
        // only one curve for each (route type, route section, time slot)-tuple

        // new datastructure for all the default curves:
        let mut dc : DefaultCurves = DefaultCurves::new();

        for rt in &route_types {
            for rs in &route_sections {
                for ts in &TimeSlot::TIME_SLOTS {
                    println!("Create average curve for route type {:?}, route section {:?} and time slot {}", rt, rs, ts.description);

                    // curve vectors
                    let a_curves = default_arrival_curves.get_mut(&(rt, rs, *ts)).unwrap();
                    let d_curves = default_departure_curves.get_mut(&(rt, rs, *ts)).unwrap();

                    // interpolate them into one curve each and
                    // put curves into the final datastructure:
                    if a_curves.len() > 0 {
                        let mut arrival_curve = IrregularDynamicCurve::<f32, f32>::average(a_curves);
                        arrival_curve.simplify(0.001);
                        dc.all_default_curves.insert((*rt, rs.clone(), (**ts).clone(), EventType::Arrival), arrival_curve);
                    }
                    
                    if d_curves.len() > 0 {
                        let mut departure_curve = IrregularDynamicCurve::<f32, f32>::average(d_curves);
                        departure_curve.simplify(0.001);
                        dc.all_default_curves.insert((*rt, rs.clone(), (**ts).clone(), EventType::Departure), departure_curve);
                    }
                }
            }
        }



        println!("Done with everything but saving. Result: {:?}", dc.all_default_curves);

        println!("Saving to binary file.");

        // save curve tree to a binary file
        save_to_file(&dc, "data/curve_data/default_curves", "Default_Curves.crv", SerdeFormat::MessagePack)?;
        
        // The hashmap has tuples as keys, which is not supported by json without manual conversion.
        // println!("Saving to json file.");
        // // save curve tree to a json file
        // save_to_file(&all_default_curves, "data/curve_data/default_curves", "Default_Curves.json", SerdeFormat::Json)?;

        println!("Done!");

        Ok(())
    }

    fn get_routes_for_type(&self, rt: RouteType) -> Vec<&Route> {

        let mut routes : Vec<&Route> = Vec::new();

        for r in self.schedule.routes.values() {
            if r.route_type == rt {
                routes.push(r);
            }
        }
        return routes;
    }

    fn get_variants_for_route(&self, r: &Route) -> HashSet<(String, &str)> {

        let mut variants : HashSet<(String, &str)> = HashSet::new();

        for t in self.schedule.trips.values() {
            if t.route_id == r.id {
                variants.insert((r.id.clone(), &t.route_variant.as_ref().unwrap()));
            }
        }
        return variants;
    }

    // picks all rows from the database for a given route section and variant
    fn get_data_from_db(&self, ri: &str, rv: &str, min: u16, max: u16) -> FnResult<Vec<DbItem>> {
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
                route_id = :route_id AND
                route_variant=:route_variant AND
                stop_sequence >= :lower_bound AND
                stop_sequence <= :upper_bound",
        )?;

        let mut result = con.exec_iter(
            &stmt,
            params! {
                "source" => &self.main.source,
                "route_id" => ri,
                "route_variant" => rv,
                "lower_bound" => min,
                "upper_bound" => max,
            },
        )?;

        let result_set = result.next_set().unwrap()?;

        let db_items: Vec<_> = result_set
            .map(|row| {
                let item: DbItem = from_row(row.unwrap());
                item
            })
            .collect();

        return Ok(db_items);
    }

    fn sort_dbitems_by_timeslot(&self, items: Vec<DbItem>) -> FnResult<HashMap<&TimeSlot, Vec<DbItem>>> {
        
        let mut sorted_items = HashMap::new();

        // initialize hashmap keys with time slots and values with empty vectors
        for ts in &TimeSlot::TIME_SLOTS {
            sorted_items.insert(*ts, Vec::new());
        }

        // go through all items and sort them into the vectors
        for i in items {
            let mut dt = i.get_datetime_from_schedule(&self.schedule, EventType::Arrival);
            // if arrival time is not set, use depature time instead:
            if dt.is_none() {
                dt = i.get_datetime_from_schedule(&self.schedule, EventType::Departure);
            }
            // should always be some now, but to be sure...
            if dt.is_some() {
                let ts : &TimeSlot = TimeSlot::from_datetime(dt.unwrap());
                sorted_items.get_mut(ts).unwrap().push(i);
            }
        }

        return Ok(sorted_items);
    }
}