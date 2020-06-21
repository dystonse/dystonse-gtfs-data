use gtfs_structures::StopTime;
use std::ops::{Index, IndexMut};
use serde::{Serialize, Deserialize};
use simple_error::bail;
use crate::FnResult;
use dystonse_curves::tree::{TreeData, LeafData, SerdeFormat, NodeData};

#[derive(Hash, Eq, PartialEq, Debug, Serialize, Deserialize, Clone, Copy)]
pub enum EventType {
    Arrival,
    Departure,
}

impl EventType {
    pub const TYPES: [&'static EventType; 2] = [&EventType::Arrival, &EventType::Departure];
}

#[derive(Hash, Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct EventPair<T> {
    pub arrival: T,
    pub departure: T
}

impl<T> EventPair<T> {
    pub const NAME : &'static str = "EventPair";
}

impl<T> Index<EventType> for EventPair<T> {
    type Output = T;

    fn index(&self, event_type: EventType) -> &Self::Output {
        match event_type {
            EventType::Arrival => &self.arrival,
            EventType::Departure => &self.departure
        }
    }
}

impl<T> IndexMut<EventType> for EventPair<T> {
    fn index_mut(&mut self, event_type: EventType) -> &mut Self::Output {
        match event_type {
            EventType::Arrival => &mut self.arrival,
            EventType::Departure => &mut self.departure
        }
    }
}

impl<T> TreeData for EventPair<T>
    where T: TreeData,
    EventPair<T>: NodeData
{
    fn save_tree(&self, dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<()> {
        if leaves.contains(&Self::NAME) {
            self.save_to_file(dir_name, own_name, format)?;
        } else {
            let sub_dir_name = format!("{}/{}", dir_name, own_name);
            self.arrival.save_tree(&sub_dir_name, "arrival", format, leaves)?;
            self.departure.save_tree(&sub_dir_name, "departure", format, leaves)?;
        }
        Ok(())
    }

    fn load_tree(dir_name: &str, own_name: &str, format: &SerdeFormat, leaves: &Vec<&str>) -> FnResult<Self> {
        bail!("Nerv nicht.");
    }
}

impl<T> LeafData for EventPair<T> {
}

pub trait GetByEventType {
    fn get_time(&self, e_t: EventType) -> Option<u32>;
}

impl GetByEventType for StopTime {
    fn get_time(&self, e_t: EventType) -> Option<u32> {
        if e_t == EventType::Departure {
            self.departure_time
        } else {
            self.arrival_time
        }
    }
}