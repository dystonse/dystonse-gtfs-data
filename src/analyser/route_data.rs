use dystonse_curves::curve_set::CurveSet;
use dystonse_curves::irregular_dynamic::*;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use super::time_slots::TimeSlot;
use mysql::*;
use mysql::prelude::*;
use chrono::NaiveDate;

#[derive(Serialize, Deserialize)]
pub struct RouteData {
    pub variants: HashMap<u64, RouteVariantData>
}

impl RouteData {
    pub fn new() -> Self {
        return Self {
            variants: HashMap::new()
        };
    }
}

#[derive(Serialize, Deserialize)]
pub struct RouteVariantData {
    pub stop_ids: Vec<String>,
    pub curve_sets: HashMap<(u32, u32, TimeSlot), CurveSet<f32, IrregularDynamicCurve<f32,f32>>>,
    pub general_delay_arrival: HashMap<u32, IrregularDynamicCurve<f32,f32>>,
    pub general_delay_departure: HashMap<u32, IrregularDynamicCurve<f32,f32>>,
}

impl RouteVariantData {
    pub fn new() -> Self {
        return Self {
            stop_ids: Vec::new(),
            curve_sets: HashMap::new(),
            general_delay_arrival: HashMap::new(),
            general_delay_departure: HashMap::new(),
        };
    }
}

pub struct DbItem {
    pub delay_arrival: Option<i32>,
    pub delay_departure: Option<i32>,
    pub date: Option<NaiveDate>,
    pub trip_id: String,
    pub stop_id: String,
    pub route_variant: u64
}

impl FromRow for DbItem {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(DbItem{
            delay_arrival: row.get_opt::<i32,_>(0).unwrap().ok(),
            delay_departure: row.get_opt::<i32,_>(1).unwrap().ok(),
            date: row.get_opt(2).unwrap().ok(),
            trip_id: row.get::<String, _>(3).unwrap(),
            stop_id: row.get::<String, _>(4).unwrap(),
            route_variant: row.get::<u64, _>(5).unwrap(),
        })
    }
}

