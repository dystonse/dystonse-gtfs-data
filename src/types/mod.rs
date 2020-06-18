mod db_item;
mod default_curves;
mod route_data;
mod route_sections;
mod route_variant_data;
mod time_slots;

pub use db_item::DbItem;
pub use default_curves::DefaultCurves;
pub use route_data::RouteData;
pub use route_sections::RouteSection;
pub use route_variant_data::RouteVariantData;
pub use time_slots::TimeSlot;

use std::ops::{Index, IndexMut};

use mysql::*;
use serde::{Serialize, Deserialize};

#[derive(Hash, Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum EventType {
    Arrival,
    Departure,
}

#[derive(Hash, Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct EventPair<T> {
    arrival: T,
    departure: T
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

