use clap::ArgMatches;
use gtfs_structures::Trip;
use itertools::Itertools;
use mysql::*;
use mysql::prelude::*;
use simple_error::bail;

use dystonse_curves::irregular_dynamic::*;
use dystonse_curves::{Curve, curve_set::CurveSet};
use dystonse_curves::tree::{SerdeFormat, NodeData};

use super::Analyser;
use super::curve_utils::*;
use crate::types::*;

use crate::{ FnResult, Main, OrError };

use std::collections::HashMap;

pub struct SpecificCurveCreator<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub args: &'a ArgMatches
}

impl<'a> SpecificCurveCreator<'a> {

    pub fn get_specific_curves(&self) -> FnResult<HashMap<String, RouteData>> {
        let mut map = HashMap::new();
        if let Some(route_ids) = self.args.values_of("route-ids") {
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                let route_data = self.create_curves_for_route(&String::from(route_id))?;
                map.insert(String::from(route_id), route_data);
            }
        } else if self.args.is_present("all") {
            let route_ids = self.analyser.schedule.routes.keys();
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                let route_data = self.create_curves_for_route(&String::from(route_id))?;
                map.insert(String::from(route_id), route_data);
            }
        } else {
            println!("I've got no route!");
        }
        
        Ok(map)
    }

    pub fn run_specific_curves(&self) -> FnResult<()> {
        let map = self.get_specific_curves()?;
        
        map.save_to_file(&self.analyser.main.dir, "specific_curves", &SerdeFormat::Json)?;
        Ok(())
    }

    fn create_curves_for_route(&self, route_id: &String)  -> FnResult<RouteData> {
        let schedule = &self.analyser.schedule;
        let route = schedule.get_route(route_id)?;
        let agency_id = route.agency_id.as_ref().unwrap().clone();
        let agency_name = schedule
            .agencies
            .iter()
            .filter(|agency| agency.id.as_ref().unwrap() == &agency_id)
            .next()
            .unwrap()
            .name
            .clone();

        println!("Working on route {} of agency {}.", route.short_name, agency_name);

        let mut route_data = RouteData::new(route_id);

        let mut con = self.main.pool.get_conn()?;
        let stmt = con.prep(
            r"SELECT 
                delay_arrival,
                delay_departure,
                trip_start_date,
                trip_start_time,
                trip_id,
                stop_id,
                route_variant
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

        let route_variants : Vec<_> = db_items.iter().map(|item| &item.route_variant).unique().collect();
        println!("For route {} there are {} variants: {:?}", route_id, route_variants.len(), route_variants);

        for route_variant in route_variants {
            let variant_as_string = Some(format!("{}", route_variant));
            let trip = schedule.trips.values().filter(|trip| trip.route_id == *route.id && trip.route_variant == variant_as_string).next();

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

        Ok(route_data)
    }

    fn create_curves_for_route_variant(
        &self, 
        rows_matching_variant: &Vec<&DbItem>, 
        trip: &Trip
    ) -> FnResult<RouteVariantData> {
        let mut route_variant_data = RouteVariantData::new();
        route_variant_data.stop_ids = trip.stop_times.iter().map(|st| st.stop.id.clone()).collect();

        // threshold of delay (in seconds) that will be considered. 
        // Every stop with more than t or less then -t delay will be ignored.
        let t = 3000; 

        for ts in &TimeSlot::TIME_SLOTS {
            // TODO here we filter all rows based on departure:true, maybe we should actually filter twice, once for each [departure, arrival]
            let rows_matching_time_slot : Vec<_> = rows_matching_variant.iter().filter(|item| ts.matches_item(item, trip, EventType::Departure)).collect();

            // Iterate over all start stations
            for (i_s, st_s) in trip.stop_times.iter().enumerate() {
                // Locally select the rows which match the start station
                let rows_matching_start : Vec<_> = rows_matching_time_slot.iter().filter(|item| item.stop_id == st_s.stop.id).map(|i| **i).collect();

                // this is where the general_delay curves are created
                for e_t in &EventType::TYPES {
                    if let Ok(res) = self.generate_delay_curve_data(&rows_matching_start, **e_t) {
                        route_variant_data.general_delay[**e_t].insert(i_s as u32, res);
                    }
                }
                
                // Iterate over end stations, and only use the ones after the start station
                for (i_e, st_e) in trip.stop_times.iter().enumerate() {
                    if i_e > i_s {
                        // Locally select rows that are matching the end station
                        let rows_matching_end : Vec<_> = rows_matching_time_slot.iter().filter(|item| item.stop_id == st_e.stop.id).collect();
                        
                        // now rows_matching_start and rows_matching_end are disjunctive sets which can be joined by their vehicle
                        // which is given by (date, trip_id).
                        // TODO: also match start_time? 
                        // TODO: use VehicleIdentifier from PerScheduleImporter (should be moved to types)

                        let vec_size = usize::min(rows_matching_start.len(), rows_matching_end.len());

                        let mut matching_pairs : EventPair<Vec<(f32, f32)>> = EventPair{
                            arrival: Vec::<(f32, f32)>::with_capacity(vec_size), 
                            departure: Vec::<(f32, f32)>::with_capacity(vec_size)
                        };
                        for row_s in &rows_matching_start {
                            for row_e in &rows_matching_end {
                                if row_s.trip_start_date == row_e.trip_start_date && 
                                   row_s.trip_start_time == row_e.trip_start_time && 
                                           row_s.trip_id == row_e.trip_id {
                                    // Only use rows where delay is not None
                                    // TODO filter those out at the DB level or in the above filter expressions
                                    if let Some(d_s) = row_s.delay.departure {
                                        for et in &EventType::TYPES {
                                            if let Some(d_e) = row_e.delay[**et] {
                                                // Filter out rows with too much positive or negative delay
                                                if d_s < t && d_s > -t && d_e < t && d_e > -t {
                                                    // Now we round the delays to multiples of 12. Much of the data that we get from the agencies
                                                    // tends to be rounded that way, and mixing up rounded and non-rounded data leads to all
                                                    // kinds of problems.
                                                    let rounded_d_s = (d_s / 12) * 12;
                                                    let rounded_d_e = (d_e / 12) * 12;
                                                    matching_pairs[**et].push((rounded_d_s as f32, rounded_d_e as f32));
                                                }
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
                        
                        for et in &EventType::TYPES {
                            // Don't generate a graphic if we have too few pairs.
                            if matching_pairs[**et].len() > 20 {
                                let stop_pair_data = self.generate_curves_for_stop_pair(&matching_pairs[**et]);
                                if let Ok(actual_data) = stop_pair_data {
                                    let key = CurveSetKey {
                                        start_stop_index: i_s as u32, 
                                        end_stop_index: i_e as u32, 
                                        time_slot: (**ts).clone()
                                    };
                                    route_variant_data.curve_sets[**et].insert(key, actual_data);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(route_variant_data)
    }

    fn generate_delay_curve_data(&self, items: &Vec<&DbItem>, event_type: EventType) -> FnResult<CurveData> {
        let values: Vec<f32> = items.iter().filter_map(|r| r.delay[event_type]).map(|t| t as f32).collect();

        if values.len() < 20 {
            bail!("Less than 20 data rows.");
        }
        let mut curve = make_curve(&values, None)?.0;
        curve.simplify(0.01);
        Ok(CurveData {
            curve,
            precision_type: PrecisionType::SemiSpecific,
            sample_size: values.len() as u32
        })
    }

    fn generate_curves_for_stop_pair(&self, pairs: &Vec<(f32, f32)>) -> FnResult<CurveSetData> {
        // Clone the pairs so that we may sort them. We sort them by delay at the start station
        // because we will group them by that criterion.
        let mut own_pairs = pairs.clone();
        own_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let count = own_pairs.len();

        // Try to make a curve out of initial delays. This curve is different from the actual
        // output curve(s), but is needed as a intermediate result to compute the markers.
        let (initial_curve, _sum) = make_curve(&own_pairs.iter().map(|(s,_e)| *s).collect(), None).or_error("Could not make curve.")?;
        // We build a list of "markers", which are x-coordinates / initial delays for which we 
        // will build a curve. That curve will consist of rows with "similar" delays.
        // All the "middle" will be inserted in-order by the recurse function. 
        // We need to add the absolute min and absolute max markers manually before and afer that,
        // and we add them twice because this simplifies the curve generation later on.
        let mut markers = Vec::<f32>::new();
        markers.push(initial_curve.min_x());
        markers.push(initial_curve.min_x());
        recurse(&initial_curve, &mut markers, initial_curve.min_x(), initial_curve.max_x(), count as f32);
        markers.push(initial_curve.max_x());
        markers.push(initial_curve.max_x());
        
        let mut sample_size: u32 = 0;
        let mut curve_set = CurveSet::<f32, IrregularDynamicCurve<f32, f32>>::new();
        // Now generate and draw one or more actual result curves.
        // Each curve will focus on the mid marker, and include all the data points from
        // the min to the max marker.
        // Remember that we added the absolute min and absolute max markers twice.
        for (lower, mid, upper) in markers.iter().tuple_windows() {
            let min_index = (count as f32 * initial_curve.y_at_x(*lower)) as usize;
            let max_index = (count as f32 * initial_curve.y_at_x(*upper)) as usize;
            let slice : Vec<f32> = own_pairs[min_index .. max_index].iter().map(|(_s,e)| *e).collect();
            sample_size += slice.len() as u32;
            if slice.len() > 1 {
                if let Ok((mut curve, _sum)) = make_curve(&slice,  Some(*mid)) {
                    curve.simplify(0.001);
                    if curve.max_x() <  curve.min_x() + 13.0 {
                        continue;
                    }
        
                    curve_set.add_curve(*mid, curve);
                }
            }
        }
        sample_size /= curve_set.curves.len() as u32;
        return Ok(CurveSetData {
            curve_set,
            sample_size, //average amount of samples per curve
            precision_type: PrecisionType::Specific
        });
    }
}