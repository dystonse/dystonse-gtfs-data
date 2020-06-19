use std::collections::HashMap;
use serde::{Serialize, Deserialize};

use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};

use crate::FnResult;
use crate::types::{RouteData, DefaultCurves};

use simple_error::bail;

#[derive(Serialize, Deserialize)]
pub struct DelayStatistics {
    pub specific: HashMap<String, RouteData>,
    pub general: DefaultCurves
}

impl DelayStatistics {
    pub const NAME : &'static str = "DelayStatistics";

    pub fn new() -> Self {
        return Self {
            specific: HashMap::new(),
            general: DefaultCurves::new()
        };
    }
}

impl TreeData for DelayStatistics {
    fn save_tree(&self, dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, "statistics.crv", format)?;
        } else {
            self.general.save_tree(dir_name, format, leaves)?;

            for (route_id, route_data) in &self.specific {
                let sub_dir_name = format!("{}/specific/route_{}", dir_name, route_id);
                route_data.save_tree(&sub_dir_name, format, leaves)?;
            }
        }

        Ok(())
    }

    fn load_tree(dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}