use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use mysql::*;
use mysql::prelude::*;
use gtfs_structures::{Trip, Gtfs};
use super::{EventType, EventPair, GetByEventType};

pub struct DbItem {
    pub delay: EventPair<Option<i32>>,
    //pub delay_arrival: Option<i32>,
    //pub delay_departure: Option<i32>,
    pub date: Option<NaiveDate>,
    pub trip_id: String,
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
            date: row.get_opt(2).unwrap().ok(),
            trip_id: row.get::<String, _>(3).unwrap(),
            stop_id: row.get::<String, _>(4).unwrap(),
            route_variant: row.get::<u64, _>(5).unwrap(),
        })
    }
}

impl DbItem {
    // generates a NaiveDateTime from a DbItem, given a flag for arrival (false) or departure (true)
    pub fn get_datetime_from_trip(&self, trip: &Trip, et: EventType) -> Option<NaiveDateTime> {

        // find corresponding StopTime for dbItem
        let st = trip.stop_times.iter()
            .filter(|s| s.stop.id == self.stop_id).next();

        if st.is_none() { return None; } // prevents panic before trying to unwrap

        // get arrival or departure time from StopTime:
        let t = st.unwrap().get_time(et);
        if t.is_none() { return None; } // prevents panic before trying to unwrap
        let time = NaiveTime::from_num_seconds_from_midnight_opt(t.unwrap(), 0);
        if time.is_none() { return None; } // prevents panic before trying to unwrap
        

        // get date from DbItem
        let d : NaiveDate = self.date.unwrap(); //should never panic because date is always set

        // add date and time together
        let dt : NaiveDateTime = d.and_time(time.unwrap());

        return Some(dt);
    }

        // generates a NaiveDateTime from a DbItem, given a flag for arrival (false) or departure (true)
    pub fn get_datetime_from_schedule(&self, schedule: &Gtfs, et: EventType) -> Option<NaiveDateTime> {
        // find corresponding StopTime for dbItem
        let maybe_trip = schedule.get_trip(&self.trip_id);
        if maybe_trip.is_err() {
            return None;
        }
        self.get_datetime_from_trip(maybe_trip.unwrap(), et)
    }
}

