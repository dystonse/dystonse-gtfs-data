use chrono::{Weekday, NaiveDateTime, Datelike, Timelike};
use serde::{Serialize, Deserialize};
use gtfs_structures::Trip;
use crate::types::{
    EventType, DbItem
};
use std::fmt::{Display, Formatter};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Time slots are specific ranges in time that occur repeatedly. 
/// Any DateTime should be able to be mapped to exactly one TimeSlot constant.
/// TimeSlots are defined by: id, description, weekday and hour criteria

#[derive(Eq, Debug, Serialize, Deserialize, Clone)]
pub struct TimeSlot {
    pub id: u8,
    #[serde(skip)]
    pub description: &'static str,
    pub min_weekday: Weekday,
    pub max_weekday: Weekday,
    pub min_hour: u32, //including
    pub max_hour: u32, //excluding
}

impl TimeSlot {
    pub const WORKDAY_MORNING : TimeSlot = TimeSlot {
        id: 1, 
        description: "Workdays from 4 to 6h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 4,
        max_hour: 6,
    };
    pub const WORKDAY_MORNING_RUSH : TimeSlot = TimeSlot {
        id: 2, 
        description: "Workdays from 6 to 8h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 6,
        max_hour: 8,
    };
    pub const WORKDAY_LATE_MORNING : TimeSlot = TimeSlot {
        id: 3, 
        description: "Workdays from 8 to 12h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 8,
        max_hour: 12,
    };
    pub const WORKDAY_NOON_RUSH : TimeSlot = TimeSlot {
        id: 4, 
        description: "Workdays from 12 to 14h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 12,
        max_hour: 14,
    };
    pub const WORKDAY_AFTERNOON : TimeSlot = TimeSlot {
        id: 5, 
        description: "Workdays from 14 to 16h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 14,
        max_hour: 16,
    };
    pub const WORKDAY_AFTERNOON_RUSH : TimeSlot = TimeSlot {
        id: 6, 
        description: "Workdays from 16 to 18h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 16,
        max_hour: 18,
    };
    pub const WORKDAY_EVENING : TimeSlot = TimeSlot {
        id: 7, 
        description: "Workdays from 18 to 20h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 18,
        max_hour: 20,
    };
    pub const SATURDAY_DAY : TimeSlot = TimeSlot {
        id: 8, 
        description: "Saturdays from 4 to 20h",
        min_weekday: Weekday::Sat,
        max_weekday: Weekday::Sat,
        min_hour: 4,
        max_hour: 20,
    };
    pub const SUNDAY_DAY : TimeSlot = TimeSlot {
        id: 9, 
        description: "Sundays from 4 to 20h",
        min_weekday: Weekday::Sun,
        max_weekday: Weekday::Sun,
        min_hour: 4,
        max_hour: 20,
    };
    pub const NIGHT_BEFORE_WORKDAY : TimeSlot = TimeSlot {
        id: 10, 
        description: "Nights before workdays from 20 to 4h",
        min_weekday: Weekday::Sun,
        max_weekday: Weekday::Thu,
        min_hour: 20,
        max_hour: 4,
    };
    pub const NIGHT_BEFORE_WEEKEND_DAY : TimeSlot = TimeSlot {
        id: 11, 
        description: "Nights before weekend days from 20 to 4h",
        min_weekday: Weekday::Fri,
        max_weekday: Weekday::Sat,
        min_hour: 20,
        max_hour: 4,
    };

    pub const TIME_SLOTS : [&'static TimeSlot; 11] = [
        &Self::WORKDAY_MORNING, 
        &Self::WORKDAY_MORNING_RUSH, 
        &Self::WORKDAY_LATE_MORNING,
        &Self::WORKDAY_NOON_RUSH,
        &Self::WORKDAY_AFTERNOON,
        &Self::WORKDAY_AFTERNOON_RUSH,
        &Self::WORKDAY_EVENING,
        &Self::SATURDAY_DAY,
        &Self::SUNDAY_DAY,
        &Self::NIGHT_BEFORE_WORKDAY,
        &Self::NIGHT_BEFORE_WEEKEND_DAY
        ];


    /// find the matching TimeSlot for a given DateTime
    pub fn from_datetime(dt: NaiveDateTime) -> &'static TimeSlot {
        
        for ts in &Self::TIME_SLOTS {
            if ts.matches(dt) {
                return ts;
            }
        } 
        // this should never be reached if time slots are defined correctly:
        panic!("invalid time slot definition!");
    }

    /// check if a given DateTime fits inside the TimeSlot
    pub fn matches(&self, dt: NaiveDateTime) -> bool {
        
        let mut day = false;
        let mut hour = false;

        // simple case for days:
        if dt.weekday().num_days_from_monday() >= self.min_weekday.num_days_from_monday() 
            && dt.weekday().num_days_from_monday() <= self.max_weekday.num_days_from_monday()
            {
                day = true;
            }
        // complex case for days:
        else if self.min_weekday.num_days_from_monday() > self.max_weekday.num_days_from_monday() 
            && (dt.weekday().num_days_from_monday() >= self.min_weekday.num_days_from_monday() 
                || dt.weekday().num_days_from_monday() <= self.max_weekday.num_days_from_monday())
            {
                day = true;
            }
        
        //simple case for hours:
        if dt.hour() >= self.min_hour 
            && dt.hour() < self.max_hour
            {
                hour = true;
            }
        //complex case for night hours:
        else if self.min_hour > self.max_hour
            && (dt.hour() >= self.min_hour || dt.hour() < self.max_hour)
            {
                hour = true;
            }

        return day && hour;
    }

    pub fn matches_item(&self, item: &DbItem, trip: &Trip, et: EventType) -> bool {
        if let Some(dt) = item.get_datetime_from_trip(trip, et) {
            self.matches(dt)
        } else {
            false
        }
    }
}

impl Display for TimeSlot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "on {} to {} from {} to {}", self.min_weekday, self.max_weekday, self.min_hour, self.max_hour)
    }
}

impl PartialOrd for TimeSlot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_key  = ( self.min_weekday.num_days_from_monday(),  self.max_weekday.num_days_from_monday(),  self.min_hour,  self.max_hour);
        let other_key = (other.min_weekday.num_days_from_monday(), other.max_weekday.num_days_from_monday(), other.min_hour, other.max_hour);
        return self_key.partial_cmp(&other_key);
    }
}

impl Ord for TimeSlot {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_key  = ( self.min_weekday.num_days_from_monday(),  self.max_weekday.num_days_from_monday(),  self.min_hour,  self.max_hour);
        let other_key = (other.min_weekday.num_days_from_monday(), other.max_weekday.num_days_from_monday(), other.min_hour, other.max_hour);
        return self_key.cmp(&other_key);
    }
}

impl Hash for TimeSlot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for TimeSlot {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}