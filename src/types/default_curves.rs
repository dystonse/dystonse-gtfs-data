use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use gtfs_structures::{RouteType};

use simple_error::bail;

use dystonse_curves::{
    tree::{TreeData, SerdeFormat, NodeData},
    irregular_dynamic::*
};

use crate::FnResult;

use crate::types::{
    EventType,
    RouteSection,
    TimeSlot,
    PrecisionType,
};

/// a struct to hold a hash map of all the default curves
#[derive(Debug, Serialize, Deserialize)]
pub struct DefaultCurves {
    #[serde(with = "crate::types::structured_map_serde")]
    pub all_default_curves: HashMap<
        (RouteType, RouteSection, TimeSlot, EventType), 
        IrregularDynamicCurve<f32, f32>
    >
}

// TODO: actually use these two structs for the DefaultCurve struct
// Key type for the default curves hashmap, so we don't have to use a tuple:
#[derive(Debug, Serialize, Deserialize)]
pub struct DefaultCurveKey {
    route_type: RouteType,
    route_section: RouteSection,
    time_slot: TimeSlot,
    event_type: EventType
}

// A curve with some metadata about its quality and origin:
#[derive(Debug, Serialize, Deserialize)]
pub struct CurveData {
    curve: IrregularDynamicCurve<f32, f32>,
    precision_type: Option<PrecisionType>,
    data_points: Option<u32>,
}

impl DefaultCurves {
    pub const NAME : &'static str = "DefaultCurves";
 
    pub fn new() -> Self {
        return Self {
            all_default_curves: HashMap::new()
        };
    }
}

impl TreeData for DefaultCurves {
    fn save_tree(&self, dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, "statistics", format)?;
        } else {
            for ((route_type, route_section, time_slot, event_type), curve) in &self.all_default_curves {
                let sub_dir_name = format!("{}/{}/{:?}/{:?}/{}", dir_name, own_name, route_type, route_section, time_slot);
                let own_name = format!("route_{:?}", event_type);
                curve.save_to_file(&sub_dir_name, &own_name, format)?;
            }
        }
        Ok(())
    }

    fn load_tree(_dir_name: &str, _own_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}