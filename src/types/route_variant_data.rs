use std::collections::HashMap;

use mysql::*;
use serde::{Serialize, Deserialize};

use dystonse_curves::curve_set::CurveSet;
use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};
use dystonse_curves::irregular_dynamic::*;

use crate::{FnResult};
use super::TimeSlot;
use super::EventPair;

use simple_error::bail;

#[derive(Serialize, Deserialize)]
pub struct RouteVariantData {
    pub stop_ids: Vec<String>,
    pub curve_sets: HashMap<(u32, u32, TimeSlot), CurveSet<f32, IrregularDynamicCurve<f32,f32>>>,
    pub general_delay: EventPair<HashMap<u32, IrregularDynamicCurve<f32,f32>>>,
}

impl TreeData for RouteVariantData {
    fn save_tree(&self, dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            let file_name = format!("variant_{}.crv", dir_name);
            self.save_to_file(dir_name, &file_name, format)?;
        } else {
            self.stop_ids.save_to_file(dir_name, "stop_ids", format)?;
            self.general_delay.save_to_file(dir_name, "general_delay", format)?;
            for ((i_s, i_e, time_slot), curve_set) in &self.curve_sets {
                let sub_dir_name = format!("{}/{}/from_{}_to_{}", dir_name, time_slot.description, i_s, i_e);
                curve_set.save_tree(&sub_dir_name, format, leaves)?;
            }
        }

        Ok(())
    }

    fn load_tree(dir_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}

impl RouteVariantData {
    pub const NAME : &'static str = "RouteVariantData";

    pub fn new() -> Self {
        return Self {
            stop_ids: Vec::new(),
            curve_sets: HashMap::new(),
            general_delay: EventPair{
                arrival: HashMap::new(),
                departure: HashMap::new(),
            }
        };
    }
}