use chrono::NaiveDateTime;
//use dystonse_curves::Curve;
//use simple_error::bail;
use crate::FnResult;

pub struct JourneyData {
    start_date_time: NaiveDateTime,
    stops: Vec<StopData>,
    trips: Vec<TripData>,
}

pub struct StopData {
    stop_name: String,
    stop_ids: Vec<String>,
    min_time: Option<NaiveDateTime>,
    max_time: Option<NaiveDateTime>,
    //arrival_curve: Option<Curve>,
}

pub struct TripData {
    // can be parsed from URL:
    route_type: usize, //TODO: use gtfs-structures type?
    route_name: String,
    trip_headsign: String,
    start_departure: Option<NaiveDateTime>,

    // needs schedule, stopdata and maybe database for finding:
    route_id: String,
    start_id: Option<String>,
    start_index: Option<usize>,
    trip_id: String,
}

impl JourneyData {
    // parse string vector (from URL) to get all necessary data
    pub fn parse_journey(journey: Vec<String>) -> FnResult<Self> {
        let start_date_time = NaiveDateTime::parse_from_str(&journey[0], "%d.%m.%y %H:%M")?;
        let stops : Vec<StopData> = Vec::new();
        let trips : Vec<TripData> = Vec::new();

        // TODO: parse stops and trips!

        Ok(JourneyData{
            start_date_time,
            stops,
            trips
        })
    }
}