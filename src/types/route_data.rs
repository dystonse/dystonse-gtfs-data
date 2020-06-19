use std::collections::HashMap;

use mysql::*;
use serde::{Serialize, Deserialize};

use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};

use crate::{FnResult};
use super::RouteVariantData;

use simple_error::bail;

#[derive(Serialize, Deserialize)]
pub struct RouteData {
    pub route_id: String,
    pub variants: HashMap<u64, RouteVariantData>
}

impl RouteData {
    pub const NAME : &'static str = "RouteData";

    pub fn new(route_id: &str) -> Self {
        return Self {
            route_id: String::from(route_id),
            variants: HashMap::new()
        };
    }
}

impl TreeData for RouteData {
    fn save_tree(&self, dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            let file_name = format!("route_{}.crv", self.route_id);
            self.save_to_file(dir_name, &file_name, format)?;
        } else {
            let sub_dir_name = format!("{}/route_{}", dir_name, self.route_id);
            for (_route_variant_id, variant_data) in &self.variants {
                variant_data.save_tree(&sub_dir_name, format, leaves)?;
            }
        }

        Ok(())
    }

    fn load_tree(dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}