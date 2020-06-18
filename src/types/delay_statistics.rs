use std::collections::HashMap;
use serde::{Serialize, Deserialize};

use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};

use crate::{FnResult};
use crate::types::{RouteData, DefaultCurves};

#[derive(Serialize, Deserialize)]
pub struct DelayStatistics {
    pub specific: HashMap<String, RouteData>,
    pub general: DefaultCurves
}

impl DelayStatistics {
    pub fn new() -> Self {
        return Self {
            specific: HashMap::new(),
            general: DefaultCurves::new()
        };
    }
}

impl TreeData for DelayStatistics {
    fn save_tree(&self, dir_name: &str, format: &SerdeFormat, file_levels: usize) -> FnResult<()> {
        if file_levels == 0 {
            self.save_to_file(dir_name, "statistics.crv", format)?;
        } else {
            self.general.save_tree(dir_name, format, file_levels - 1)?;

            for (route_id, route_data) in &self.specific {
                let sub_dir_name = format!("{}/specific/route_{}", dir_name, route_id);
                route_data.save_tree(&sub_dir_name, format, file_levels - 1)?;
            }
        }

        Ok(())
    }
}