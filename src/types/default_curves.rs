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
    pub fn new() -> Self {
        return Self {
            all_default_curves: HashMap::new()
        };
    }

    // TODO: This is just a dummy and does not actually do anything yet!!!
    pub fn load_from_file(file_path: &str) -> FnResult<DefaultCurves> {
        return Ok(DefaultCurves::new());
    }
}

impl TreeData for DefaultCurves {
    fn save_tree(&self, dir_name: &str, format: &SerdeFormat, file_levels: usize) -> FnResult<()> {
        Ok(())
    }

    fn load_tree(dir_name: &str, format: &SerdeFormat, file_levels: usize) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}