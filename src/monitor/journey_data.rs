use chrono::{Date, DateTime, Local, Duration, NaiveTime};
use chrono::offset::TimeZone;
use simple_error::bail;
use crate::{FnResult, OrError, date_and_time_local, types::EventType};
use gtfs_structures::{Gtfs, RouteType, Stop, Trip};
use std::sync::Arc;
use regex::Regex;
use super::{Monitor, route_type_to_str, DbPrediction, time_curve::TimeCurve};
use geo::prelude::*;
use geo::{point, Point};
use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;
use dystonse_curves::{IrregularDynamicCurve, Tup};
use mysql::*;
use mysql::prelude::*;

use percent_encoding::{percent_decode_str, utf8_percent_encode, CONTROLS, AsciiSet};

const PATH_ELEMENT_ESCAPE: &AsciiSet = &CONTROLS.add(b'/').add(b'?').add(b'"').add(b'`');

// radius in which we look for other stops close by to include their departures in a stop's page
const EXTENDED_STOPS_MAX_DISTANCE: f32 = 300.0; 

pub struct JourneyData {
    pub start_date_time: DateTime<Local>,
    pub components: Vec<JourneyComponent>,
    pub monitor: Arc<Monitor>,
}

#[derive(Debug, Clone)]
pub struct StopData {
    pub url: String,
    pub prev_component: Option<JourneyComponent>,

    pub stop_name: String,
    pub stops: Vec<Arc<Stop>>,
    pub stop_ids: Vec<String>,
    pub extended_stops: Vec<Arc<Stop>>,
    pub extended_stop_ids: Vec<String>,
    pub extended_stop_names: Vec<String>,
    pub extended_stops_distances: HashMap<String, f32>,

    pub start_curve: TimeCurve,
    pub start_prob: f32,
    pub arrival_trip_stop_index: Option<usize>,
}

impl StopData {
    // returns the previous TripData, if the previous component is a trip (and not a walk)
    pub fn get_previous_trip_data(&self) -> Option<Arc<TripData>> {
        if let Some(JourneyComponent::Trip(trip_data)) = &self.prev_component {
            Some(trip_data.clone())
        } else {
            None
        }
    }

    // calculates the maximum airline distance between the main stops of two StopData objects
    pub fn get_max_distance(&self, other_stop_data: &StopData) -> f32 {
        let other_stop_geos : Vec<Point<f64>> = other_stop_data.stops.iter().map(|stop| point!(x: stop.latitude.unwrap(), y: stop.longitude.unwrap())).collect();
        return self.get_max_distance_from_geos(&other_stop_geos);
    }

    // calculates the maximum airline distance between the main stops of a StopData object and a vector of (geo) points
    pub fn get_max_distance_from_geos(&self, other_stop_geos: & Vec<Point<f64>>) -> f32 {
        let this_stop_geos  : Vec<Point<f64>> = self.stops.iter().map(|stop| point!(x: stop.latitude.unwrap(), y: stop.longitude.unwrap())).collect();
        
        let mut max_distance = 0.0;
        for this_stop_geo in this_stop_geos {
            for other_stop_geo in other_stop_geos {
                max_distance = f32::max(max_distance, this_stop_geo.haversine_distance(&other_stop_geo) as f32);
            }
        }
        return max_distance;
    }
}

#[derive(Debug, Clone)]
pub struct TripData {
    pub url: String,
    pub prev_component: JourneyComponent,
    
    pub route_type: RouteType,
    pub route_name: String,
    pub trip_headsign: String,
    pub start_departure: DateTime<Local>,

    pub start_curve: TimeCurve,
    pub start_prob: f32,

    pub route_id: String,
    pub start_id: Option<String>,
    pub start_index: Option<usize>,
    pub vehicle_id: VehicleIdentifier,
}

impl TripData {
    pub fn get_previous_stop_data(&self) -> Option<Arc<StopData>> {
        if let JourneyComponent::Stop(stop_data) = &self.prev_component {
            Some(stop_data.clone())
        } else {
            None
        }
    }

    pub fn get_trip<'a>(&self, schedule: &'a Gtfs) -> FnResult<&'a Trip> {
        let trip : &Trip = schedule.get_trip(&self.vehicle_id.trip_id)?;
        Ok(trip)
    }
}

#[derive(Debug, Clone)]
pub struct WalkData {
    pub url: String,
    pub prev_component: JourneyComponent,
    
    pub start_curve: TimeCurve,
    pub start_prob: f32,
}

#[derive(Debug, Clone)]
pub enum JourneyComponent {
    Stop(Arc<StopData>),
    Trip(Arc<TripData>),
    Walk(Arc<WalkData>),
}

impl JourneyComponent {
    pub fn get_curve(&self) -> &TimeCurve {
        match self {
            JourneyComponent::Stop(stop_data) => &stop_data.start_curve,
            JourneyComponent::Trip(trip_data) => &trip_data.start_curve,
            JourneyComponent::Walk(walk_data) => &walk_data.start_curve,
        }
    }

    pub fn get_prob(&self) -> f32 {
        match self {
            JourneyComponent::Stop(stop_data) => stop_data.start_prob,
            JourneyComponent::Trip(trip_data) => trip_data.start_prob,
            JourneyComponent::Walk(walk_data) => walk_data.start_prob,
        }
    }

    pub fn get_prev(&self) -> Option<JourneyComponent> {
        match self {
            JourneyComponent::Stop(stop_data) => stop_data.prev_component.clone(),
            JourneyComponent::Trip(trip_data) => Some(trip_data.prev_component.clone()),
            JourneyComponent::Walk(walk_data) => Some(walk_data.prev_component.clone()),
        }
    }

    pub fn get_url(&self) -> &String {
        match self {
            JourneyComponent::Stop(stop_data) => &stop_data.url,
            JourneyComponent::Trip(trip_data) => &trip_data.url,
            JourneyComponent::Walk(walk_data) => &walk_data.url,
        }
    }
}

impl JourneyData {
    // parse string vector (from URL) to get all necessary data
    pub fn new(journey: &[String], monitor: Arc<Monitor>) -> FnResult<Self> {
        println!("JourneyData::new with {:?}", journey);
        
        let mut journey_data = JourneyData{
            components: Vec::new(),
            monitor: monitor.clone(),
            start_date_time: Local::now(), // will be overwritten during parse 
        };

        journey_data.parse_journey(journey)?;

        Ok(journey_data)
    }

    pub fn parse_journey(&mut self, journey: &[String]) -> FnResult<()> {
        let mut journey_iter = journey.iter();
        let timestring = journey_iter.next().unwrap(); 
        self.start_date_time = Local.datetime_from_str(timestring, "%d.%m.%y %H:%M")?;

        let mut prev_component: Option<JourneyComponent> = None;
        let mut expect_stop = true;

        for string in journey_iter {
            let decoced_string = &utf8_percent_encode(&string, PATH_ELEMENT_ESCAPE).to_string();
            let component = if expect_stop {
                expect_stop = false;
                self.parse_stop_data(decoced_string, prev_component)?
            } else {
                expect_stop = true;
                if string == "Fußweg" {
                    self.parse_walk_data(decoced_string, prev_component.unwrap())?
                } else {
                    self.parse_trip_data(decoced_string, prev_component.unwrap())?
                }
            };
            self.components.push(component.clone());
            prev_component = Some(component);
        }
        Ok(())
    }

    pub fn parse_walk_data(&self, walk_string: &str, prev_component: JourneyComponent) -> FnResult<JourneyComponent> {
        Ok(JourneyComponent::Walk(Arc::new(WalkData{
            prev_component: prev_component.clone(),
            url: format!("{}{}/", prev_component.get_url(), "Fußweg"),
            start_curve: prev_component.get_curve().clone(),
            start_prob: prev_component.get_prob(),
        })))
    }

    pub fn parse_stop_data(&self, stop_string: &str, prev_component: Option<JourneyComponent>) -> FnResult<JourneyComponent> {
        let stop_name = percent_decode_str(stop_string).decode_utf8_lossy().to_string();

        let url = if let Some(prev) = &prev_component {
            format!("{}{}/", prev.get_url(), stop_string)
        } else {
            format!("/{}/{}/", self.start_date_time.format("%d.%m.%y %H:%M"), stop_string)
        };

        let stops : Vec<Arc<Stop>> = self.monitor.schedule.stops.iter().filter_map(|(_id, stop)| if stop_name == stop.name {Some(stop.clone())} else {None}).collect();

        if stops.is_empty() {
            bail!("No stops found for stop_name {}", stop_name);
        }

        let stop_geos : Vec<_> = stops.iter().map(|stop| point!(x: stop.latitude.unwrap(), y: stop.longitude.unwrap())).collect();

        // search nearby stops
        let mut extended_stops : Vec<Arc<Stop>> = Vec::new();
        let mut extended_stop_ids : HashSet<String> = HashSet::new();
        let mut extended_stop_names : HashSet<String> = HashSet::new();
        let mut extended_stops_distances : HashMap<String, f32> = HashMap::new();
        for (other_stop_id, other_stop) in &self.monitor.schedule.stops {
            let other_stop_geo = point!(x: other_stop.latitude.unwrap(), y: other_stop.longitude.unwrap());
            for stop_geo in &stop_geos {
                let distance = stop_geo.haversine_distance(&other_stop_geo) as f32;
                if distance < EXTENDED_STOPS_MAX_DISTANCE {
                    //println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                    extended_stops.push(other_stop.clone());
                    extended_stop_ids.insert(other_stop_id.clone());
                    if let Some(d) =  extended_stops_distances.get(other_stop_id) {
                        if *d < distance {
                            extended_stops_distances.insert(other_stop_id.clone(), distance);
                            // println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                        }
                    } else {
                        if !stops.iter().any(|stop| stop.id == *other_stop_id) { //don't insert the main stop
                           extended_stops_distances.insert(other_stop_id.clone(), distance as f32); 
                           // println!("Added in {:>3.0} distance: {}.", distance, other_stop.name);
                        }
                    }
                    extended_stop_names.insert(other_stop.name.clone());
                }
            }
        }

        // create info for previous trip/arrival:
        let mut start_curve: TimeCurve;
        //let mut arrival_time_min : Option<DateTime<Local>> = None;
        let mut start_prob: f32;
        let mut arrival_trip_stop_index : Option<usize> = None;
        let mut vehicle_id : Option<VehicleIdentifier> = None;
        
        if let Some(prev) = &prev_component {
            if let JourneyComponent::Trip(trip_data) = prev {
                if let Ok(trip) = self.monitor.schedule.get_trip(&trip_data.vehicle_id.trip_id) {
                    // TODO in the next line we find the stop_time at which we get off the vehicle,
                    // and we must check that it's stop_sequence is higher than the stop_sequence on
                    // which we get onto the vehicle earlyer.
                    if let Some(stop_time) = &trip.stop_times.iter().filter(|st| st.stop.name == stop_name).next(){
                        //set some of the arrival trip info:
                        arrival_trip_stop_index = Some(trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence)?);
                        vehicle_id = Some(trip_data.vehicle_id.clone());
                        
                        if let Ok(a_curve) = get_curve_for(self.monitor.clone(), &stop_time.stop.id, &trip_data.vehicle_id, EventType::Arrival){
                            let scheduled_arrival = date_and_time_local(&trip_data.vehicle_id.start_date, stop_time.arrival_time.unwrap() as i32);
                            start_curve = TimeCurve::new(a_curve, scheduled_arrival);
                            start_prob = prev.get_prob();
                        } else {
                            bail!("Could not get curve.");
                        }
                    } else {
                        bail!("Could not get matching stop_time.");
                    }
                } else {
                    bail!("Could not get trip.");
                }
            } else if let JourneyComponent::Walk(walk_data) = prev {
                if let JourneyComponent::Stop(prev_stop) = &walk_data.prev_component {
                    let distance_meters = prev_stop.get_max_distance_from_geos(&stop_geos);
                    let walk_duration_curve: IrregularDynamicCurve<f32, f32> = get_walk_time(distance_meters);
                    let walk_start_curve: TimeCurve = walk_data.start_curve.clone();
                    let walk_end_curve = walk_start_curve.add_duration_curve(&walk_duration_curve);
                    // can't touch this!
                    //walk_data.distance = Some(distance_meters);
                    
                    start_curve = walk_end_curve;
                    // the chance to miss the transfer into a walk is zero, so we can carry over the probability from before:
                    start_prob = walk_data.start_prob; 
                } else {
                    bail!("Walk has no prev stop component.");
                }
            } else {
                bail!("Stop has no plausible prev component.");
            }
        } else { //first stop has no trip_data for arrival
            start_prob = 1.0;
            start_curve = TimeCurve::new(
                IrregularDynamicCurve::new(vec![ Tup{x:-30.0, y:0.0}, Tup{x:30.0, y:1.0}, ]),
                self.start_date_time
            );
        }

        Ok(JourneyComponent::Stop(Arc::new(StopData{
            prev_component: prev_component.clone(),
            stop_name,
            stop_ids: stops.iter().map(|stop| stop.id.clone()).collect(),
            stops,
            extended_stops,
            extended_stop_ids: Vec::from_iter(extended_stop_ids),
            extended_stop_names: Vec::from_iter(extended_stop_names),
            extended_stops_distances,
            url,
            start_curve,
            start_prob,
            arrival_trip_stop_index,
        })))
    }


    pub fn parse_trip_data(&self, trip_string: &str, prev_component: JourneyComponent) -> FnResult<JourneyComponent> {
        let stop_data = if let JourneyComponent::Stop(stop) = &prev_component {
            stop
        } else {
            bail!("Need stop before trip.");
        };

        let url = format!("{}{}/", prev_component.get_url(), trip_string);

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
        
        let start_departure_date: Date<Local> = self.start_date_time.date();
        // here we assume that we don't have journeys that span more than 24 hours:
        // TODO Duration::hours(-5) is just a wild guess at how long ago a trip might have been scheduled
        // and still be a trip in the near future.
        let start_departure = if time - self.start_date_time.time() >= Duration::hours(-5) {
            start_departure_date.and_time(time).unwrap()
        } else {
            start_departure_date.and_time(time).unwrap() + Duration::days(1)
        };

        // now we will need the schedule, and info about the stop from where we want to start...

        for (id, trip) in &self.monitor.schedule.trips {
            // look up the trips by headsign
            if trip.trip_headsign != some_trip_headsign {
                continue;
            }

            // look up trips with route (by route name and route type)
            if let Ok(route) = self.monitor.schedule.get_route(&trip.route_id) {
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
            let trip_days : Vec<u16> = self.monitor.schedule.trip_days(&trip.service_id, (start_departure_date - Duration::days(1)).naive_local());
            let filtered_trip_days : Vec<_> = trip_days.iter().filter(|d| **d <= 2).collect();
            if  filtered_trip_days.is_empty() {
                continue;
            } else {
                // only use trips that include the stop we want to start from:
                for stop_time in trip.stop_times.iter().filter(|st| stop_data.extended_stop_names.contains(&st.stop.name)) {
                    if let Some(scheduled_departure) = stop_time.departure_time {
                        for d in &filtered_trip_days {
                            // find out for what time this trip is scheduled to depart from the stop we're looking at:
                            let scheduled_datetime = date_and_time_local(&start_departure.date(), scheduled_departure as i32) + Duration::days(**d as i64 - 1);
                            // compare if this is the one we're looking for:
                            if scheduled_datetime != start_departure {
                                continue;
                            } else {
                                // now we can finally gather the remaining info:
                                let route_id = trip.route_id.clone();
                                let start_id = Some(stop_time.stop.id.clone());
                                let start_index = Some(trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence).unwrap());
                                let trip_start_time = Duration::seconds(trip.stop_times[0].departure_time.unwrap() as i64);
                                let trip_start_date = start_departure.date() + Duration::days(**d as i64 - 1);
                                let vehicle_id = VehicleIdentifier {
                                    start_date: trip_start_date,
                                    start_time: trip_start_time,
                                    trip_id: id.clone()
                                };

                                // set curve and prob for departure at first stop:
                                let (start_curve, start_prob) = if let Ok(s_d_curve) = get_curve_for(
                                    self.monitor.clone(), 
                                    &stop_time.stop.id, 
                                    &vehicle_id,
                                    EventType::Departure
                                ) {
                                    let departure_curve = TimeCurve::new(s_d_curve, scheduled_datetime);
                                    let start_departure_prob = stop_data.start_curve.get_transfer_probability(&departure_curve) * stop_data.start_prob;
                                    (departure_curve, start_departure_prob)
                                } else {
                                    bail!("Could not get curves for trip.");
                                };

                                // now we can finally make our struct from all the gathered info :)
                                let trip_data = TripData{
                                    prev_component: prev_component.clone(),
                                    url,
                                    route_type,
                                    route_name,
                                    trip_headsign,
                                    start_departure,
                                    vehicle_id,
                                    route_id,
                                    start_id,
                                    start_index,
                                    start_curve,
                                    start_prob,
                                };

                                return Ok(JourneyComponent::Trip(Arc::new(trip_data)));
                            }
                         }
                    }
                }
            }
        }

        bail!("Trip not found")
    }

    pub fn get_last_component(&self) -> Option<JourneyComponent> {
        if self.components.is_empty() {
            None
        } else {
            Some(self.components.last().unwrap().clone())
        }
    }
}

pub fn get_curve_for(monitor: Arc<Monitor>, stop_id: &String, vehicle_id: &VehicleIdentifier, et: EventType) -> FnResult<IrregularDynamicCurve<f32, f32>> {

    if let Ok(pred) = get_prediction_for_first_line(monitor, stop_id, vehicle_id, et) {
        return Ok(pred.prediction_curve.clone());
    };
    
    bail!("no curve found for {:?} at stop {:?} in trip {:?}", et, stop_id, vehicle_id.trip_id);
}

pub fn get_prediction_for_first_line(monitor: Arc<Monitor>, stop_id: &String, vehicle_id: &VehicleIdentifier, et: EventType) -> FnResult<DbPrediction> {
    
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
            "trip_id" => &vehicle_id.trip_id,
            "trip_start_date" => vehicle_id.start_date.naive_local(),
            "trip_start_time" => vehicle_id.start_time,
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
    
    bail!("no prediction found for {:?} at stop {:?} in trip {:?}", et, stop_id, vehicle_id.trip_id);
}

pub fn get_walk_time(distance_meters: f32) -> IrregularDynamicCurve<f32, f32> {
    if distance_meters < 20.0 {
        return IrregularDynamicCurve::new(vec![Tup{x: -12.0, y: 0.0},Tup{x: 12.0, y: 1.0}]);
    }

    // assing a factor to the distance, which is measured as air-line distance, to account for detours.
    let min_distance_factor = 1.0;
    // for short distances (near 0m), assume a factor of 1.8, for long distances (near 500m) assume a factor of 1.4.
    let max_distance_factor = 1.4 + f32::max(0.0, f32::min(0.4, (500.0 - distance_meters) / 500.0 * 0.4));

    // people have different walking speeds. Walk speed numbers taken from https://de.wikipedia.org/wiki/Schrittgeschwindigkeit
    let min_walk_speed = 0.8; // m/s
    let _max_walk_speed = 1.65; // m/s
    let max_sprint_speed = 3.5; // m/s taken from personal training

    // additional time needed to orient, regardless of actual distance
    let min_delay = 10.0; // s
    let max_delay = 45.0; // s

    let min_duration = distance_meters * min_distance_factor / max_sprint_speed + min_delay; // s
    let max_duration = distance_meters * max_distance_factor / min_walk_speed + max_delay; // s
    
    let mut points = Vec::with_capacity(22);

    // Fake a normal distribution by taking a nice slice out of a cosine's square root.
    let pi = std::f32::consts::PI;
    for p in (0..101).step_by(5) {
        let duration = min_duration + (max_duration - min_duration) * p as f32 / 100.0;
        let scaled_x = pi + pi * p as f32 / 100.0;
        let y = (f32::cos(scaled_x).abs().sqrt() * f32::cos(scaled_x).signum() + 1.0) / 2.0;
        points.push(Tup{x: duration, y});
    }

    let mut curve = IrregularDynamicCurve::new(points);
    curve.simplify(0.01);
    return curve;
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct VehicleIdentifier {
    pub trip_id: String,
    pub start_time: Duration,
    pub start_date: Date<Local>
}