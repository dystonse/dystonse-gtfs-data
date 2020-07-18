use serde::{Serialize, Deserialize};
use simple_error::bail;
use crate::FnResult;

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

impl CurveData {
    pub fn average(data: &Vec<CurveData>, precision_type: PrecisionType) -> FnResult<Self> {
        if data.len() == 0 {
            bail!("Can't compute average of 0 curves.");
        }

        let mut sample_size: u32 = 0;

        let mut curves : Vec<&IrregularDynamicCurve<f32, f32>> = Vec::with_capacity(data.len());
        for curve_data in data {
            curves.push(&curve_data.curve);
            sample_size += curve_data.sample_size;
        }
        sample_size /= curves.len() as u32;

        let curve = IrregularDynamicCurve::<f32, f32>::average(&curves);

        Ok(CurveData {
            curve,
            precision_type,
            sample_size
        })
    } 
}

// A curveset with some metadata about its quality and origin:
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CurveSetData {
    pub curve_set: CurveSet<f32, IrregularDynamicCurve<f32,f32>>,
    pub precision_type: PrecisionType,
    pub sample_size: u32,
}