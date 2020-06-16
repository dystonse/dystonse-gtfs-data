use dystonse_curves::curve_set::CurveSet;
use dystonse_curves::irregular_dynamic::*;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

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
    pub curve_sets: HashMap<(u32, u32), CurveSet<f32, IrregularDynamicCurve<f32,f32>>>
}

impl RouteVariantData {
    pub fn new() -> Self {
        return Self {
            stop_ids: Vec::new(),
            curve_sets: HashMap::new()
        };
    }
}
