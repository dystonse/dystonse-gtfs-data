use gtfs_structures::{Gtfs, Trip};
use serde::{Serialize, Deserialize};
use crate::FnResult;

/// Route sections are sets of stops that form a part of the route (beginning, middle, or end)
#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Clone)]
pub enum RouteSection {
    Beginning,
    Middle,
    End,
}

impl RouteSection {
    pub fn get_route_section_by_stop_sequence(schedule: &Gtfs, trip_id: &str, stop_sequence: u16) -> FnResult<RouteSection> {
        // check if trip_id is valid for the given schedule
        // and get the right trip object
        let trip = schedule.get_trip(&trip_id).unwrap(); //panics if trip is not found!

        // Find the index of the stop in question
        let stop_index = trip.get_stop_index_by_stop_sequence(stop_sequence)?;
        return Self::get_route_section_by_stop_index(&trip, stop_index);
    }

    #[allow(dead_code)]
    pub fn get_route_section_by_stop_id(schedule: &Gtfs, trip_id: &str, stop_id: &str) -> FnResult<RouteSection> {
        // check if trip_id is valid for the given schedule
        // and get the right trip object
        let trip = schedule.get_trip(&trip_id).unwrap(); //panics if trip is not found!

        // Find the index of the stop in question
        let stop_index = trip.get_stop_index_by_id(stop_id)?;
        return Self::get_route_section_by_stop_index(&trip, stop_index);
    }

    // this finds out for a given stop, in which section of a route it is.
    // caution: this panics when trip or stop is not found!
    pub fn get_route_section_by_stop_index(trip: &Trip, stop_index: usize) -> FnResult<RouteSection> {
        // Find out how many stops this trip has
        let stop_count = trip.stop_times.len();
        // define the length of the beginning and end sections:
        // 1/3 of the trip for trips shorter than 15 stops, 5 stops for longer trips.
        let section_size = usize::min(5, stop_count/3);

        // find return value according to index
        // panics if stop was not found!!!
        if stop_index < section_size {
            return Ok(RouteSection::Beginning);
        } else if stop_count - stop_index <= section_size {
            return Ok(RouteSection::End);
        }
        return Ok(RouteSection::Middle);
    }
}