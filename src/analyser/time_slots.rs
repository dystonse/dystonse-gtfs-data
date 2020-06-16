use chrono::{Weekday, NaiveDateTime, Datelike, Timelike};

/// Time slots are specific ranges in time that occur repeatedly. 
/// Any DateTime should be able to be mapped to exactly one TimeSlot constant.
/// TimeSlots are defined by: id, description, weekday and hour criteria

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct TimeSlot<'a> {
    id: u8,
    description: &'a str,
    min_weekday: Weekday,
    max_weekday: Weekday,
    min_hour: u32, //including
    max_hour: u32, //excluding
}

impl<'a> TimeSlot<'a> {
    pub const WORKDAY_MORNING : TimeSlot<'a> = TimeSlot {
        id: 1, 
        description: "Workdays from 4 to 6h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 4,
        max_hour: 6,
    };
    pub const WORKDAY_MORNING_RUSH : TimeSlot<'a> = TimeSlot {
        id: 2, 
        description: "Workdays from 6 to 8h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 6,
        max_hour: 8,
    };
    pub const WORKDAY_LATE_MORNING : TimeSlot<'a> = TimeSlot {
        id: 3, 
        description: "Workdays from 8 to 12h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 8,
        max_hour: 12,
    };
    pub const WORKDAY_NOON_RUSH : TimeSlot<'a> = TimeSlot {
        id: 4, 
        description: "Workdays from 12 to 14h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 12,
        max_hour: 14,
    };
    pub const WORKDAY_AFTERNOON : TimeSlot<'a> = TimeSlot {
        id: 5, 
        description: "Workdays from 14 to 16h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 14,
        max_hour: 16,
    };
    pub const WORKDAY_AFTERNOON_RUSH : TimeSlot<'a> = TimeSlot {
        id: 6, 
        description: "Workdays from 16 to 18h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 16,
        max_hour: 18,
    };
    pub const WORKDAY_EVENING : TimeSlot<'a> = TimeSlot {
        id: 7, 
        description: "Workdays from 18 to 20h",
        min_weekday: Weekday::Mon,
        max_weekday: Weekday::Fri,
        min_hour: 18,
        max_hour: 20,
    };
    pub const SATURDAY_DAY : TimeSlot<'a> = TimeSlot {
        id: 8, 
        description: "Saturdays from 04 to 20h",
        min_weekday: Weekday::Sat,
        max_weekday: Weekday::Sat,
        min_hour: 4,
        max_hour: 20,
    };
    pub const SUNDAY_DAY : TimeSlot<'a> = TimeSlot {
        id: 9, 
        description: "Sundays from 04 to 20h",
        min_weekday: Weekday::Sun,
        max_weekday: Weekday::Sun,
        min_hour: 4,
        max_hour: 20,
    };
    pub const NIGHT_BEFORE_WORKDAY : TimeSlot<'a> = TimeSlot {
        id: 10, 
        description: "Nights before workdays from 20 to 04h",
        min_weekday: Weekday::Sun,
        max_weekday: Weekday::Thu,
        min_hour: 20,
        max_hour: 4,
    };
    pub const NIGHT_BEFORE_WEEKEND_DAY : TimeSlot<'a> = TimeSlot {
        id: 11, 
        description: "Nights before weekend days from 20 to 04h",
        min_weekday: Weekday::Fri,
        max_weekday: Weekday::Sat,
        min_hour: 20,
        max_hour: 4,
    };

    pub const TIME_SLOTS : [&'a TimeSlot<'a>; 11] = [
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
    pub fn from_datetime(dt: NaiveDateTime) -> &'static TimeSlot<'a> {
        
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
}