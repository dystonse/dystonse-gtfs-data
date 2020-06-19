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
    fn save_tree(&self, dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, "statistics", format)?;
        } else {
            let sub_dir_name = format!("{}/{}", dir_name, own_name);
            self.general.save_tree(&sub_dir_name, "general", format, leaves)?;

            let sub_dir_name = format!("{}/{}/specific", dir_name, own_name);
            for (route_id, route_data) in &self.specific {
                let own_name = format!("route_{}", route_id);
                route_data.save_tree(&sub_dir_name, &own_name, format, leaves)?;
            }
        }

        Ok(())
    }

    fn load_tree(_dir_name: &str, _own_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}