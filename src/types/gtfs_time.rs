use chrono::*;
use std::cmp::Ordering;
use core::cmp::Ord;
use gtfs_rt::TripDescriptor;
use regex::Regex;
use crate::{FnResult, OrError};

#[derive(Eq, Clone, Hash, Debug)]
pub struct GtfsDateTime {
    service_day: Date<Local>,
    time: i32
}

impl GtfsDateTime {
    pub fn new(service_day: Date<Local>, time: i32) -> Self {
        Self {
            service_day,
            time
        }
    }

    pub fn from_trip_descriptor(trip_descriptor: &TripDescriptor) -> FnResult<Self> {
        lazy_static! {
            static ref FIND_TIME: Regex = Regex::new(r"(\d+):(\d+):(\d+)").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }

        let start_date_string: &String = trip_descriptor.start_date.as_ref().or_error("No start_date")?;
        let naive_start_date = NaiveDate::parse_from_str(start_date_string, "%Y%m%d")?;
        let start_date = Local.from_local_date(&naive_start_date).unwrap();

        let time_element_captures = FIND_TIME
            .captures(trip_descriptor.start_time.as_ref().or_error("No start_time")?)
            .or_error(&format!(
            "Trip descriptor does not contain a valid time (does not match format HH:MM:SS): {}",
            trip_descriptor.start_time.as_ref().unwrap()
        ))?;

        let hours  : i32 = time_element_captures[1].parse().unwrap();
        let minutes: i32 = time_element_captures[2].parse().unwrap();
        let seconds: i32 = time_element_captures[3].parse().unwrap();

        Ok(Self{
            service_day: start_date,
            time: hours * 3600 + minutes * 60 + seconds,
        })
    }

    /// Return the logical date, which may be different from the actual date of this 
    pub fn service_day(&self) -> Date<Local> {
        return self.service_day;
    }

    pub fn date_time(&self) -> DateTime<Local> {
        // see https://developers.google.com/transit/gtfs/reference#field_types for this quirky thing:
        return self.service_day.and_hms(12, 0, 0) + (Duration::seconds(self.time as i64) - Duration::hours(12));
    }

    pub fn duration(&self) -> Duration {
        return Duration::seconds(self.time as i64);
    }

    #[allow(dead_code)]
    pub fn seconds(&self) -> i32 {
        return self.time;
    }

    #[allow(dead_code)]
    pub fn naive_time(&self) -> NaiveTime {
        return self.date_time().time();
    }

    pub fn date(&self) -> Date<Local> {
        return self.date_time().date();
    }
}

impl Ord for GtfsDateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date_time().cmp(&other.date_time())
    }
}

impl PartialOrd for GtfsDateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for GtfsDateTime {
    fn eq(&self, other: &Self) -> bool {
        self.date_time() == other.date_time()
    }
}