use chrono::{NaiveTime, NaiveDateTime, Duration};
//use dystonse_curves::Curve;
use simple_error::bail;
use crate::{FnResult, OrError, date_and_time};
use gtfs_structures::{Gtfs, RouteType, Stop};
use std::sync::Arc;
use regex::Regex;
use super::route_type_to_str;
use percent_encoding::percent_decode_str;
use geo::prelude::*;
use geo::point;
use std::collections::HashSet;
use std::iter::FromIterator;

pub struct JourneyData {
    pub start_date_time: NaiveDateTime,
    pub stops: Vec<StopData>,
    pub trips: Vec<TripData>,
    pub schedule: Arc<Gtfs>,
}

#[derive(Debug)]
pub struct StopData {
    pub stop_name: String,
    pub stop_ids: Vec<String>,
    pub extended_stop_ids: Vec<String>,
    pub extended_stop_names: Vec<String>,
    pub min_time: Option<NaiveDateTime>,
    pub max_time: Option<NaiveDateTime>,
    //arrival_curve: Option<Curve>,
}

#[derive(Debug)]
pub struct TripData {
    // can be parsed from URL:
    pub route_type: RouteType,
    pub route_name: String,
    pub trip_headsign: String,
    pub start_departure: NaiveDateTime,

    // needs schedule, stopdata and maybe database for finding:
    pub trip_id: String,
    pub route_id: String,
    pub start_id: Option<String>,
    pub start_index: Option<usize>,
}

#[derive(Debug)]
pub enum JourneyComponent<'a> {
    None,
    Stop(&'a StopData),
    Trip(&'a TripData),
}

impl JourneyData {
    // parse string vector (from URL) to get all necessary data
    pub fn new(schedule: Arc<Gtfs>, journey: &[String]) -> FnResult<Self> {
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

        journey_data.parse_journey(journey)?;

        Ok(journey_data)
    }

    pub fn parse_journey(&mut self, journey: &[String]) -> FnResult<()> {

        let mut journey_iter = journey.iter();
        journey_iter.next(); // skip first item, which is the datetime.
        loop {
            // assume that the first, third, etc.. part is a stop string:
            if let Some(stop_string) = journey_iter.next() {
                let mut stop_data = self.parse_stop_data(stop_string)?;
                stop_data.min_time = Some(self.start_date_time);
                self.stops.push(stop_data);
            } else { 
                break;
            }
            // assume that the second, fourth, etc.. part is a trip string:
            if let Some(trip_string) = journey_iter.next() {
                self.trips.push(self.parse_trip_data(trip_string, self.stops.last().unwrap())?);
            } else { 
                break; 
            }
        }
        Ok(())
    }

    pub fn parse_stop_data(&self, stop_string: &str) -> FnResult<StopData> {
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
        for (other_stop_id, other_stop) in &self.schedule.stops {
            let other_stop_geo = point!(x: other_stop.latitude.unwrap(), y: other_stop.longitude.unwrap());
            for stop_geo in &stop_geos {
                let distance = stop_geo.haversine_distance(&other_stop_geo);
                if distance < 500.0 {
                    println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                    extended_stop_ids.insert(other_stop_id.clone());
                    extended_stop_names.insert(other_stop.name.clone());
                }
            }
        }

        Ok(StopData{
            stop_name,
            stop_ids: stops.iter().map(|stop| stop.id.clone()).collect(),
            extended_stop_ids: Vec::from_iter(extended_stop_ids),
            extended_stop_names: Vec::from_iter(extended_stop_names),
            min_time: None,
            max_time: None
        })
    }

    pub fn parse_trip_data(&self, trip_string: &str, stop_data: &StopData) -> FnResult<TripData> {
        
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
        let mut route_type = RouteType::Other(0);
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

        // variables to store the trip's data in:
        let mut route_id : String = String::from("");
        let mut start_id = None;
        let mut start_index = None;
        let mut trip_id : String = String::from("");

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
                if let Some(stop_time) = trip.stop_times.iter().filter(|st| stop_data.extended_stop_names.contains(&st.stop.name)).next() {
                    if let Some(scheduled_departure) = stop_time.departure_time {
                        for d in filtered_trip_days {
                            // find out for what time this trip is scheduled to depart from the stop we're looking at:
                            let scheduled_datetime = date_and_time(&start_departure.date(), scheduled_departure as i32) + Duration::days(*d as i64 - 1);
                            // compare if this is the one we're looking for:
                            if scheduled_datetime != start_departure {
                                continue;
                            } else {
                                // now we can finally gather the remaining info:
                                trip_id = id.clone();
                                route_id = trip.route_id.clone();
                                start_id = Some(stop_time.stop.id.clone());
                                start_index = Some(trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence).unwrap());
                                break; // ignore any possible further dates
                            }
                           
                         }
                    }
                }
            }
        }

        // now we can finally make our struct from all the gathered info :)
        Ok(TripData{
            route_type,
            route_name,
            trip_headsign,
            start_departure,
            trip_id,
            route_id,
            start_id,
            start_index
        })
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