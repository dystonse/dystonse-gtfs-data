use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use dystonse_curves::curve_set::CurveSet;
use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};
use dystonse_curves::irregular_dynamic::*;

use crate::{FnResult};
use super::TimeSlot;
use super::{EventPair, EventType};

use simple_error::bail;

#[derive(Serialize, Deserialize)]
pub struct CurveSetsMap {
    #[serde(with = "crate::types::structured_map_serde")]
    pub map: HashMap<(u32, u32, TimeSlot), CurveSet<f32, IrregularDynamicCurve<f32,f32>>>
}

#[derive(Serialize, Deserialize)]
pub struct RouteVariantData {
    pub stop_ids: Vec<String>,
    pub curve_sets: EventPair<CurveSetsMap>,
    pub general_delay: EventPair<HashMap<u32, IrregularDynamicCurve<f32,f32>>>,
}

impl TreeData for RouteVariantData {
    fn save_tree(&self, dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, own_name, format)?;
        } else {
            self.stop_ids.save_to_file(dir_name, "stop_ids", format)?;
            self.general_delay.save_to_file(dir_name, "general_delay", format)?;
            for et in &EventType::TYPES {
                for ((i_s, i_e, time_slot), curve_set) in &self.curve_sets[**et].map {
                    let sub_dir_name = format!("{}/{}/{}/{:?}", dir_name, own_name, time_slot.description, et);
                    let own_name = format!("from_{}_to_{}", i_s, i_e);
                    curve_set.save_tree(&sub_dir_name, &own_name, format, leaves)?;
                }
            }

        }

        Ok(())
    }

    fn load_tree(_dir_name: &str, _own_name: &str, _format: &SerdeFormat, _leaves: &Vec<&str>) -> FnResult<Self>{
        bail!("Not yet implemented!");
    }
}

impl RouteVariantData {
    pub const NAME : &'static str = "RouteVariantData";

    pub fn new() -> Self {
        return Self {
            stop_ids: Vec::new(),
            curve_sets: EventPair{
                arrival: CurveSetsMap{map: HashMap::new()},
                departure: CurveSetsMap{map:HashMap::new()},
            },
            general_delay: EventPair{
                arrival: HashMap::new(),
                departure: HashMap::new(),
            }
        };
    }
}
