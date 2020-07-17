use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use dystonse_curves::tree::{SerdeFormat, TreeData, NodeData};

use crate::{FnResult};
use super::{TimeSlot, CurveSetData, CurveData, EventPair, EventType};

use simple_error::bail;

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct CurveSetKey {
    pub start_stop_index: u32,
    pub end_stop_index: u32,
    pub time_slot: TimeSlot
}

#[derive(Serialize, Deserialize)]
pub struct RouteVariantData {
    pub stop_ids: Vec<String>,
    pub curve_sets: EventPair<HashMap<CurveSetKey, CurveSetData>>,
    pub general_delay: EventPair<HashMap<u32, CurveData>>,
}

impl TreeData for RouteVariantData {
    fn save_tree(&self, dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, own_name, format)?;
        } else {
            self.stop_ids.save_to_file(dir_name, "stop_ids", format)?;
            self.general_delay.save_to_file(dir_name, "general_delay", format)?;
            for et in &EventType::TYPES {
                for (key, curve_set_data) in &self.curve_sets[**et] {
                    let sub_dir_name = format!("{}/{}/{}/{:?}", dir_name, own_name, key.time_slot.description, et);
                    let own_name = format!("from_{}_to_{}", key.start_stop_index, key.end_stop_index);
                    curve_set_data.curve_set.save_tree(&sub_dir_name, &own_name, format, leaves)?;
                    //TODO: this ignores the CurveSetData's meta data, but we don't use it anyway, so we can fix this later.
                }
            }

        }

        Ok(())
    }


    //TODO: implement this :D
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
                arrival: HashMap::new(),
                departure: HashMap::new(),
            },
            general_delay: EventPair{
                arrival: HashMap::new(),
                departure: HashMap::new(),
            }
        };
    }
}
