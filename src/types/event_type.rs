use gtfs_structures::StopTime;
use std::ops::{Index, IndexMut};
use serde::{Serialize, Deserialize};

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