use serde::{Serialize, Deserialize};

use dystonse_curves::{
    irregular_dynamic::*,
    CurveSet
};

use super::PrecisionType;

// A curve with some metadata about its quality and origin:
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CurveData {
    pub curve: IrregularDynamicCurve<f32, f32>,
    pub precision_type: PrecisionType,
    pub sample_size: u32,
}

// A curveset with some metadata about its quality and origin:
#[derive(Debug, Serialize, Deserialize)]
pub struct CurveSetData {
    pub curve_set: CurveSet<f32, IrregularDynamicCurve<f32,f32>>,
    pub precision_type: PrecisionType,
    pub sample_size: u32,
}