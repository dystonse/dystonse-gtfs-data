use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Duration};
//use dystonse_curves::Curve;
use simple_error::bail;
use crate::{FnResult, OrError, date_and_time, types::EventType};
use gtfs_structures::{Gtfs, RouteType, Stop};
use std::sync::Arc;
use regex::Regex;
use super::{Monitor, route_type_to_str, DbPrediction};
use geo::prelude::*;
use geo::point;
use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;
use dystonse_curves::{Curve, IrregularDynamicCurve};
use mysql::*;
use mysql::prelude::*;

use percent_encoding::{percent_decode_str, utf8_percent_encode, CONTROLS, AsciiSet};

const PATH_ELEMENT_ESCAPE: &AsciiSet = &CONTROLS.add(b'/').add(b'?').add(b'"').add(b'`');

const EXTENDED_STOPS_MAX_DISTANCE: f32 = 500.0;

pub struct JourneyData {
    pub start_date_time: NaiveDateTime,
    pub stops: Vec<StopData>,
    pub trips: Vec<TripData>,
    pub schedule: Arc<Gtfs>,
}

#[derive(Debug, Clone)]
pub struct StopData {
    pub journey_prefix: String,
    pub stop_name: String,
    pub stop_ids: Vec<String>,
    pub extended_stop_ids: Vec<String>,
    pub extended_stop_names: Vec<String>,
    pub extended_stops_distances: HashMap<String, f32>,
    pub min_time: Option<NaiveDateTime>,
    pub max_time: Option<NaiveDateTime>,

    pub arrival_curve: Option<IrregularDynamicCurve<f32, f32>>,
    pub arrival_prob: Option<f32>,
    pub arrival_trip_id: Option<String>,
    pub arrival_trip_stop_index: Option<usize>,
    pub arrival_trip_start_date: Option<NaiveDate>,
    pub arrival_trip_start_time: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct TripData {
    pub journey_prefix: String,
    
    pub route_type: RouteType,
    pub route_name: String,
    pub trip_headsign: String,
    pub start_departure: NaiveDateTime,

    pub start_departure_curve: Option<IrregularDynamicCurve<f32, f32>>,
    pub start_departure_prob: Option<f32>,

    pub trip_id: String,
    pub route_id: String,
    pub start_id: Option<String>,
    pub start_index: Option<usize>,
    pub trip_start_date: NaiveDate,
    pub trip_start_time: Duration,
}

#[derive(Debug)]
pub enum JourneyComponent<'a> {
    None,
    Stop(&'a StopData),
    Trip(&'a TripData),
}

impl JourneyData {
    // parse string vector (from URL) to get all necessary data
    pub fn new(schedule: Arc<Gtfs>, journey: &[String], monitor: Arc<Monitor>) -> FnResult<Self> {
        println!("JourneyData::new with {:?}", journey);
        let start_date_time = NaiveDateTime::parse_from_str(&journey[0], "%d.%m.%y %H:%M")?;
        let stops : Vec<StopData> = Vec::new();
        let trips : Vec<TripData> = Vec::new();
        
        let mut journey_data = JourneyData{
            start_date_time,
            stops,
            trips,
            schedule: schedule.clone()
        };

        journey_data.parse_journey(journey, monitor)?;

        Ok(journey_data)
    }

    pub fn parse_journey(&mut self, journey: &[String], monitor: Arc<Monitor>) -> FnResult<()> {

        let mut journey_iter = journey.iter();
        let timestring = journey_iter.next().unwrap(); 
        let mut prefix = format!("/{}/", timestring);
        loop {
            // assume that the first, third, etc.. part is a stop string:
            if let Some(stop_string) = journey_iter.next() {
                let mut stop_data = self.parse_stop_data(&prefix, &utf8_percent_encode(&stop_string, PATH_ELEMENT_ESCAPE).to_string(), self.trips.last(), monitor.clone())?;
                // set min time (for first stop only):
                if stop_data.min_time.is_none() {
                    stop_data.min_time = Some(self.start_date_time);
                }

                self.stops.push(stop_data);

                prefix = format!("{}{}/", prefix, stop_string);
            } else { 
                break;
            }
            // assume that the second, fourth, etc.. part is a trip string:
            if let Some(trip_string) = journey_iter.next() {
                let trip_data = self.parse_trip_data(&prefix, &utf8_percent_encode(&trip_string, PATH_ELEMENT_ESCAPE).to_string(), self.stops.last().unwrap(), monitor.clone())?;                
                self.trips.push(trip_data);

                prefix = format!("{}{}/", prefix, trip_string);
            } else { 
                break; 
            }
        }
        Ok(())
    }

    pub fn parse_stop_data(&self, prefix: &str, stop_string: &str, prev_trip: Option<&TripData>, monitor: Arc<Monitor>) -> FnResult<StopData> {
        let stop_name = percent_decode_str(stop_string).decode_utf8_lossy().to_string();

        
        // let mut stop_names : Vec<String> = vec![stop_name.clone()];
        // // make sure that different spellings of "Bahnhof" are considered as the same stop name:
        // let bahnhofs = ["Hauptbahnhof", "Hbf", "Bahnhof", "Bf", "Hauptbahnhof (U)", "Hbf (U)", "Bahnhof (U)", "Bf (U)", "(U)"];
        // let mut contains_bahnhof = false;
        // for bahnhof in &bahnhofs {
        //     if stop_name.contains(bahnhof) {
        //         contains_bahnhof = true;
        //         for other_bahnhof in &bahnhofs {
        //             if bahnhof != other_bahnhof {
        //                 stop_names.push(stop_name.replace(bahnhof, other_bahnhof));
        //             }
        //         }
        //         stop_names.push(stop_name.replace(bahnhof, "").trim().to_string());
        //     }
        // }
        // if !contains_bahnhof {
        //     for bahnhof in &bahnhofs {
        //         stop_names.push(format!("{} {}", stop_name, bahnhof));
        //     }
        // }

        // if stop_names.len() > 1 {
        //     println!("Extended stop_names to by using name matching {:?}", stop_names);
        // }

        let stops : Vec<Arc<Stop>> = self.schedule.stops.iter().filter_map(|(_id, stop)| if stop_name == stop.name {Some(stop.clone())} else {None}).collect();

        if stops.is_empty() {
            bail!("No stops found for stop_name {}", stop_name);
        }

        let stop_geos : Vec<_> = stops.iter().map(|stop| point!(x: stop.latitude.unwrap(), y: stop.longitude.unwrap())).collect();

        // search nearby stops
        let mut extended_stop_ids : HashSet<String> = HashSet::new();
        let mut extended_stop_names : HashSet<String> = HashSet::new();
        let mut extended_stops_distances : HashMap<String, f32> = HashMap::new();
        for (other_stop_id, other_stop) in &self.schedule.stops {
            let other_stop_geo = point!(x: other_stop.latitude.unwrap(), y: other_stop.longitude.unwrap());
            for stop_geo in &stop_geos {
                let distance = stop_geo.haversine_distance(&other_stop_geo) as f32;
                if distance < EXTENDED_STOPS_MAX_DISTANCE {
                    //println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                    extended_stop_ids.insert(other_stop_id.clone());
                    if let Some(d) =  extended_stops_distances.get(other_stop_id) {
                        if *d < distance {
                            extended_stops_distances.insert(other_stop_id.clone(), distance);
                            println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                        }
                    } else {
                        if !stops.iter().any(|stop| stop.id == *other_stop_id) { //don't insert the main stop
                           extended_stops_distances.insert(other_stop_id.clone(), distance as f32); 
                           println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                        }
                    }
                    extended_stop_names.insert(other_stop.name.clone());
                }
            }
        }

        // create info for previous trip/arrival:
        let mut arrival_curve : Option<IrregularDynamicCurve<f32, f32>> = None;
        let mut arrival_time_min : Option<NaiveDateTime> = None;
        let mut arrival_prob : Option<f32> = None;
        let mut arrival_trip_id : Option<String> = None;
        let mut arrival_trip_stop_index : Option<usize> = None;
        let mut arrival_trip_start_date : Option<NaiveDate> = None;
        let mut arrival_trip_start_time : Option<Duration> = None;

        if let Some(trip_data) = prev_trip {
            if let Ok(trip) = self.schedule.get_trip(&trip_data.trip_id) {
                if let Some(stop_time) = &trip.stop_times.iter().filter(|st| st.stop.name == stop_name).next(){
                    //set some of the arrival trip info:
                    arrival_trip_id = Some(trip_data.trip_id.clone());
                    arrival_trip_stop_index = Some(trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence)?);
                    arrival_trip_start_date = Some(trip_data.trip_start_date);
                    arrival_trip_start_time = Some(trip_data.trip_start_time);
                    if let Ok(a_curve) = get_curve_for(monitor.clone(), &stop_time.stop.id, &trip_data, EventType::Arrival){
                        //set min time and curve:
                        let arrival_time_min_relative = a_curve.x_at_y(0.01);
                        let a_time_min = date_and_time(&trip_data.trip_start_date, stop_time.arrival_time.unwrap() as i32) 
                            + Duration::seconds(arrival_time_min_relative as i64);
                        arrival_time_min = Some(a_time_min);
                        arrival_curve = Some(a_curve);
                    }
                }
            }
        } else { //first stop has no trip_data for arrival
            arrival_prob = Some(1.0);
        }

        Ok(StopData{
            stop_name,
            stop_ids: stops.iter().map(|stop| stop.id.clone()).collect(),
            extended_stop_ids: Vec::from_iter(extended_stop_ids),
            extended_stop_names: Vec::from_iter(extended_stop_names),
            extended_stops_distances,
            min_time: arrival_time_min,
            max_time: None,
            journey_prefix: String::from(prefix),
            arrival_curve, //TODO: maybe we need to modify this?
            arrival_prob,
            arrival_trip_id,
            arrival_trip_stop_index,
            arrival_trip_start_date,
            arrival_trip_start_time,
        })
    }


    pub fn parse_trip_data(&self, prefix: &str, trip_string: &str, stop_data: &StopData, monitor: Arc<Monitor>) -> FnResult<TripData> {
        
        // Regex to parse stuff like: "Bus 420 nach Wolfenbüttel Bahnhof um 21:39", 
        // or more generally: route_type route_name nach trip_headsign um start_departure.time
        lazy_static! {
            static ref TRIP_REGEX: Regex = Regex::new(r"(\S+) (.+) nach (.+) um (\d\d:\d\d)").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }

        let trip_element_captures = TRIP_REGEX
            .captures(&trip_string)
            .or_error(&format!(
            "Trip string does not contain a valid trip descriptor: '{}'",
            trip_string
        ))?;

        let route_type_string: String = trip_element_captures[1].to_string();
        let mut route_type;
        let route_name: String = trip_element_captures[2].to_string();
        let trip_headsign: String = percent_decode_str(&trip_element_captures[3]).decode_utf8_lossy().to_string();
        let some_trip_headsign = Some(trip_headsign.clone());
        let time: NaiveTime = NaiveTime::parse_from_str(&trip_element_captures[4], "%H:%M")?;
        
        let start_departure_date = self.start_date_time.date();
        // here we assume that we don't have journeys that span more than 24 hours:
        // TODO Duration::hours(-5) is just a wild guess at how long ago a trip might have been scheduled
        // and still be a trip in the near future.
        let start_departure = if time - self.start_date_time.time() >= Duration::hours(-5) {
            start_departure_date.and_time(time)
        } else {
            start_departure_date.and_time(time) + Duration::days(1)
        };

        // now we will need the schedule, and info about the stop from where we want to start...

        for (id, trip) in &self.schedule.trips {
            // look up the trips by headsign
            if trip.trip_headsign != some_trip_headsign {
                continue;
            }

            // look up trips with route (by route name and route type)
            if let Ok(route) = self.schedule.get_route(&trip.route_id) {
                if route.short_name != route_name {
                    continue;
                }

                // TODO use translated route type names!!
                if route_type_to_str(route.route_type) != route_type_string {
                    continue;
                } else {
                    route_type = route.route_type;
                }
            } else {
                // could not find route -> then we don't want to use this trip
                continue; 
            }

            // then, filter trips by date (we only want trips that are scheduled to the start_departure_date or the previous or next day)
            let trip_days : Vec<u16> = self.schedule.trip_days(&trip.service_id, start_departure.date() - Duration::days(1));
            let filtered_trip_days : Vec<_> = trip_days.iter().filter(|d| **d <= 2).collect();
            if  filtered_trip_days.is_empty() {
                continue;
            } else {
                // only use trips that include the stop we want to start from:
                for stop_time in trip.stop_times.iter().filter(|st| stop_data.extended_stop_names.contains(&st.stop.name)) {
                    if let Some(scheduled_departure) = stop_time.departure_time {
                        for d in &filtered_trip_days {
                            // find out for what time this trip is scheduled to depart from the stop we're looking at:
                            let scheduled_datetime = date_and_time(&start_departure.date(), scheduled_departure as i32) + Duration::days(**d as i64 - 1);
                            // compare if this is the one we're looking for:
                            if scheduled_datetime != start_departure {
                                continue;
                            } else {
                                // now we can finally gather the remaining info:
                                let trip_id = id.clone();
                                let route_id = trip.route_id.clone();
                                let start_id = Some(stop_time.stop.id.clone());
                                let start_index = Some(trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence).unwrap());
                                let trip_start_date = start_departure.date() + Duration::days(**d as i64 - 1);
                                let trip_start_time = Duration::seconds(trip.stop_times[0].departure_time.unwrap() as i64);
                                
                                // now we can finally make our struct from all the gathered info :)
                                let mut trip_data = TripData{
                                    route_type,
                                    route_name,
                                    trip_headsign,
                                    start_departure,
                                    trip_id,
                                    route_id,
                                    start_id,
                                    start_index,
                                    trip_start_date,
                                    trip_start_time,
                                    journey_prefix: String::from(prefix),
                                    start_departure_curve: None, //will be set below
                                    start_departure_prob: None, //Has to be set from outside
                                };

                                // set curve for departure at first stop:
                                if let Ok(s_d_curve) = get_curve_for(monitor.clone(), &stop_time.stop.id, &trip_data, EventType::Departure) {
                                    trip_data.start_departure_curve = Some(s_d_curve);
                                }

                                return Ok(trip_data);
                            }
                         }
                    }
                }
            }
        }

        bail!("Trip not found")
    }

    pub fn get_last_component(&self) -> JourneyComponent {
        if self.stops.is_empty() {
            JourneyComponent::None
        } else if self.stops.len() > self.trips.len() {
            JourneyComponent::Stop(self.stops.last().unwrap())
        } else {
            JourneyComponent::Trip(self.trips.last().unwrap())
        }
    }
}

pub fn get_curve_for(monitor: Arc<Monitor>, stop_id: &String, trip_data: &TripData, et: EventType) -> FnResult<IrregularDynamicCurve<f32, f32>> {

    if let Ok(pred) = get_prediction_for_first_line(monitor, stop_id, trip_data, et) {
        return Ok(pred.prediction_curve.clone());
    };
    
    bail!("no curve found for {:?} at stop {:?} in trip {:?}", et, stop_id, trip_data.trip_id);
}

pub fn get_prediction_for_first_line(monitor: Arc<Monitor>, stop_id: &String, trip_data: &TripData, et: EventType) -> FnResult<DbPrediction> {
    
    let mut conn = monitor.pool.get_conn()?;

    let stmt = conn.prep(
        r"SELECT 
            `route_id`,
            `trip_id`,
            `trip_start_date`,
            `trip_start_time`,
            `prediction_min`, 
            `prediction_max`,
            `precision_type`,
            `origin_type`,
            `sample_size`,
            `prediction_curve`,
            `stop_id`,
            `stop_sequence`
        FROM
            `predictions` 
        WHERE 
            `source`=:source AND 
            `event_type`=:event_type AND
            `stop_id`=:stop_id AND
            `trip_id`=:trip_id AND
            `trip_start_date`=:trip_start_date AND
            `trip_start_time`=:trip_start_time",
    )?;

    let mut result = conn.exec_iter(
        &stmt,
        params! {
            "source" => &monitor.source,
            "event_type" => et.to_int(),
            "stop_id" => stop_id,
            "trip_id" => &trip_data.trip_id,
            "trip_start_date" => trip_data.trip_start_date,
            "trip_start_time" => trip_data.trip_start_time,
        },
    )?;

    let result_set = result.next_set().unwrap()?;

    let db_predictions: Vec<_> = result_set
        .map(|row| {
            let item: DbPrediction = from_row(row.unwrap());
            item
        })
        .collect();

    if let Some(pred) = db_predictions.first() {
        return Ok(pred.clone());
    };
    
    bail!("no prediction found for {:?} at stop {:?} in trip {:?}", et, stop_id, trip_data.trip_id);
}