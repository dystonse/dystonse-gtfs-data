use dystonse_curves::{Curve, IrregularDynamicCurve};
use gtfs_rt::{StopTimeEventExtension, PredictionType};
use itertools::multizip;
use std::fmt::{Debug, Display, Formatter};
use crate::types::{CurveData, CurveSetData};

/*
pub enum PredictionResult {
    General(Box<dyn Curve>), // for the route type, independant of initial delay
    SemiSpecific(Box<dyn Curve>), // for the specific route, route_variant and end_stop_id, but independant of initial delay
    SpecificCurve(Box<dyn Curve>), // for the specific route, route_variant, {start and end}_stop_id and initial delay
    SpecificCurveSet(CurveSet<f32, IrregularDynamicCurve<f32, f32>>), // for the specific route, route_variant, {start and end}_stop_id, but independant of initial delay
}
*/

#[derive(Debug)]
pub enum PredictionResult {
    CurveData(CurveData),
    CurveSetData(CurveSetData),
}

impl PredictionResult {
    //This is used for our possible gfts realtime format extension:
    #[allow(dead_code)]
    pub fn to_stop_time_event_extension(&self) -> StopTimeEventExtension {
        match self {
            Self::CurveData(curve_data) => Self::ext_from_curve(&curve_data.curve, PredictionType::General), 
            // TODO we need to separate type / source / precision in GTFS rt, like we did in the database
            Self::CurveSetData(_)  => panic!("Can't process SpecificCurveSet yet."),
        }
    } 

    #[allow(dead_code)]
    fn ext_from_curve(curve: &IrregularDynamicCurve<f32, f32>, p_type: PredictionType) -> StopTimeEventExtension {
        StopTimeEventExtension {
            curve: Some(gtfs_rt::Curve {
                point: Self::points_from_curve(curve)
            }),
            prediction_type: p_type as i32,
            delay_1:  Some(curve.x_at_y(0.01)),
            delay_5:  Some(curve.x_at_y(0.05)),
            delay_25: Some(curve.x_at_y(0.25)),
            delay_50: Some(curve.x_at_y(0.50)),
            delay_75: Some(curve.x_at_y(0.75)),
            delay_95: Some(curve.x_at_y(0.95)),
            delay_99: Some(curve.x_at_y(0.99))
        }
    }

    #[allow(dead_code)]
    fn points_from_curve(curve: &IrregularDynamicCurve<f32, f32>) -> Vec<gtfs_rt::Point> {
        multizip(curve.get_values_as_vectors()).map(|(x, y)| gtfs_rt::Point {time: x, probability: y} ).collect()
    }

    #[allow(dead_code)]
    pub fn to_type_int(&self) -> u8 {
        match self {
            Self::CurveData(_) => 1,
            Self::CurveSetData(_) => 2,
        }
    }
}

impl Display for PredictionResult
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Aligned output is nice for debugging / logs.
            Self::CurveData(cd)     => write!(f, "CurveData: {:?}          ({})", cd.precision_type, cd.curve),
            Self::CurveSetData(csd) => write!(f, "CurveSetData: {:?}       ({})", csd.precision_type, csd.curve_set)
        }
    }
}