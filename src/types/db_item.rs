use chrono::{Date, Duration, Local, DateTime};
use chrono::offset::TimeZone;
use mysql::*;
use mysql::prelude::*;
use gtfs_structures::{Trip, Gtfs};
use super::{EventType, EventPair, GetByEventType};
use crate::date_and_time_local;

#[derive(Clone)]
pub struct DbItem {
    pub delay: EventPair<Option<i32>>,
    //pub delay_arrival: Option<i32>,
    //pub delay_departure: Option<i32>,
    pub trip_start_date: Option<Date<Local>>,
    pub trip_start_time: Option<Duration>,
    pub trip_id: String,
    pub stop_sequence: u16,
    pub stop_id: String,
    pub route_variant: u64
}

impl FromRow for DbItem {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(DbItem{
            delay: EventPair {
                arrival: row.get_opt::<i32,_>(0).unwrap().ok(),
                departure: row.get_opt::<i32,_>(1).unwrap().ok(),
            },
            trip_start_date: if let Some(naive_date) = row.get_opt(2).unwrap().ok() {
                Some(Local.from_local_date(&naive_date).unwrap())
            } else {
                None
            },
            trip_start_time: row.get_opt(3).unwrap().ok(),
            trip_id: row.get::<String, _>(4).unwrap(),
            stop_id: row.get::<String, _>(5).unwrap(),
            stop_sequence: row.get::<u16, _>(6).unwrap(),
            route_variant: row.get::<u64, _>(7).unwrap(),
        })
    }
}

impl DbItem {
    // generates a NaiveDateTime from a DbItem, given a flag for arrival or departure 
    pub fn get_datetime_from_trip(&self, trip: &Trip, et: EventType) -> Option<DateTime<Local>> {

        // find corresponding StopTime for dbItem
        let st = trip.stop_times.iter().filter(|st| st.stop_sequence == self.stop_sequence).next();

        if st.is_none() { return None; } // prevents panic before trying to unwrap

        // get arrival or departure time from StopTime:
        let seconds = st.unwrap().get_time(et);
        if seconds.is_none() { return None; } // prevents panic before trying to unwrap
        
        // get date from DbItem
        let date: Date<Local> = self.trip_start_date.unwrap(); //should never panic because date is always set
        return Some(date_and_time_local(&date, seconds.unwrap() as i32));
    }

    // generates a NaiveDateTime from a DbItem, given a flag for arrival or departure
    pub fn get_datetime_from_schedule(&self, schedule: &Gtfs, et: EventType) -> Option<DateTime<Local>> {
        // find corresponding StopTime for dbItem
        let maybe_trip = schedule.get_trip(&self.trip_id);
        if maybe_trip.is_err() {
            return None;
        }
        self.get_datetime_from_trip(maybe_trip.unwrap(), et)
    }
}

