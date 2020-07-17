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
    pub all_default_curves: HashMap<DefaultCurveKey, CurveData>
}

// Key type for the default curves hashmap, so we don't have to use a tuple:
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DefaultCurveKey {
    pub route_type: RouteType,
    pub route_section: RouteSection,
    pub time_slot: TimeSlot,
    pub event_type: EventType
}

// A curve with some metadata about its quality and origin:
#[derive(Debug, Serialize, Deserialize)]
pub struct CurveData {
    pub curve: IrregularDynamicCurve<f32, f32>,
    pub precision_type: Option<PrecisionType>,
    pub data_points: Option<u32>,
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
            for (key, curve) in &self.all_default_curves {
                let sub_dir_name = format!("{}/{}/{:?}/{:?}/{}", dir_name, own_name, key.route_type, key.route_section, key.time_slot);
                let own_name = format!("route_{:?}", key.event_type);
                curve.save_to_file(&sub_dir_name, &own_name, format)?;
            }
        }
        Ok(())
    }

    fn load_tree(_dir_name: &str, _own_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}