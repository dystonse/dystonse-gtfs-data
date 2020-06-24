use chrono::{NaiveDateTime, NaiveTime};
use gtfs_structures::Trip;
use mysql::*;
use mysql::prelude::*;

use simple_error::bail;

use crate::FnResult;
use crate::Main;

pub struct RealtimeItem {
    pub stop_sequence: u32,
    pub stop_id: String,
    pub time_of_recording: NaiveDateTime,
    pub delay_departure: Option<i32>,
    }
    
    impl FromRow for RealtimeItem {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(RealtimeItem{
            stop_sequence: row.get::<u32, _>(0).unwrap(),
            stop_id: row.get::<String, _>(1).unwrap(),
            time_of_recording: row.get::<NaiveDateTime, _>(2).unwrap(),
            delay_departure: row.get_opt::<i32,_>(3).unwrap().ok(),
        })
    }
}

pub fn get_realtime_data(main: &Main, trip: &Trip) -> FnResult<(String, i32)> {
    let mut con = main.pool.get_conn()?;
    let stmt = con.prep(
        r"SELECT 
            `stop_sequence`,
            `stop_id`,
            `time_of_recording`,
            `delay_departure` 
          FROM realtime 
          WHERE 
            source=:source AND 
            `route_id` = :route_id AND
            `route_variant` = :route_variant AND
            `trip_id`= :trip_id AND 
            `date`=CURDATE()
        ORDER BY 
            `time_of_recording` DESC,
            `stop_sequence` DESC;",
    )?;

    let mut result = con.exec_iter(
        &stmt,
        params! {
            "source" => &main.source,
            "route_id" => &trip.route_id,
            "route_variant" => &trip.route_variant.as_ref().unwrap(),
            "trip_id" => &trip.id,
        },
    )?;

    let result_set = result.next_set().unwrap()?;

    let realtime_items: Vec<_> = result_set
        .map(|row| {
            let item: RealtimeItem = from_row(row.unwrap());
            item
        })
        .collect();

    // map the (relative) delays from the db to absolute_departures, which are tuples of (stop_id, time)
    let absolute_departures : Vec<(&String, NaiveTime, i32)> = realtime_items.iter().filter_map(|item| {
        let stop_time = trip.stop_times.iter().filter(|st| st.stop.id == item.stop_id).next().unwrap();
        match (stop_time.departure_time, item.delay_departure) {
            (Some(departure_time), Some(departure_delay)) => { 
                let secs = (departure_time as i32 + departure_delay) as u32;
                Some((&item.stop_id, NaiveTime::from_num_seconds_from_midnight(secs, 0), departure_delay))
            },
            _ => None
        }
    }).collect();

    // now find the most recent absolute_departure which is in the past. Since they are ordered
    // from latest (possibly in the future) to earliest (possibly in the past), the first one
    // that is encountered is the correct one.

    let now = chrono::Utc::now().time();
    match absolute_departures.iter().filter(|(_stop_id, time, _delay)| time < &now).next() {
        Some((stop_id, _time, delay)) => Ok((stop_id.to_string(), *delay)),
        None => bail!("No current delay found")
    }
}
