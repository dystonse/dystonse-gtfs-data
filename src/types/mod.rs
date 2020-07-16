mod db_item;
mod default_curves;
mod delay_statistics;
mod event_type;
mod prediction_result;
mod route_data;
mod route_sections;
mod route_variant_data;
mod structured_map_serde;
mod time_slots;

pub use db_item::DbItem;
pub use default_curves::DefaultCurves;
pub use default_curves::DefaultCurveKey;
pub use default_curves::CurveData;
pub use delay_statistics::DelayStatistics;
pub use event_type::{EventType, EventPair, GetByEventType};
pub use prediction_result::PredictionResult;
pub use route_data::RouteData;
pub use route_sections::RouteSection;
pub use route_variant_data::RouteVariantData;
pub use time_slots::TimeSlot;

use serde::{Serialize, Deserialize};

#[derive(PartialEq, Eq, Clone)]
pub struct PredictionBasis {
    pub stop_id: String,
    pub delay_departure: Option<i64>
}

// used to store where a prediction was generated from
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OriginType {
    Realtime,
    Schedule,
}

// Info about how precisely the base dataset matches the curve's purpose
#[derive(Debug, Serialize, Deserialize)]
pub enum PrecisionType {
    Specific, 
    SemiSpecific,
    General,
    FallbackGeneral, //TODO: come up with better names!
    SuperGeneral
}


#[cfg(test)]
mod tests {

    use crate::FnResult;
    use super::DelayStatistics;
    use dystonse_curves::tree::{NodeData, SerdeFormat};

    #[test]
    fn test_load_save() -> FnResult<()> {
        println!("Read test file");
        let data = DelayStatistics::load_from_file("./data/test", "test_delay_statistics", &SerdeFormat::Json)?;
        println!("Save test file");
        data.save_to_file("./data/test", "test_delay_statistics_roundtrip", &SerdeFormat::Json)?;
        println!("Done with test file");

        Ok(())
    }
}