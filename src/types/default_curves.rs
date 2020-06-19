use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use gtfs_structures::{RouteType};

use simple_error::bail;

use dystonse_curves::{
    tree::{TreeData, SerdeFormat},
    irregular_dynamic::*
};

use crate::FnResult;

use crate::types::{
    EventType,
    RouteSection,
    TimeSlot
};

/// a struct to hold a hash map of all the default curves
#[derive(Debug, Serialize, Deserialize)]
pub struct DefaultCurves {
    pub all_default_curves: HashMap<(RouteType, RouteSection, TimeSlot, EventType), 
        IrregularDynamicCurve<f32, f32>>

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
    fn save_tree(&self, _dir_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<()> {
        Ok(())
    }

    fn load_tree(_dir_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}