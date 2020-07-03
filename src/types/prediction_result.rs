use dystonse_curves::{Curve, CurveSet, IrregularDynamicCurve};
use gtfs_rt::{StopTimeEventExtension, PredictionType};
use itertools::multizip;
use std::fmt::{Debug, Display, Formatter};

#[allow(dead_code)] 
#[derive(Debug)]
pub enum PredictionResult {
    General(Box<dyn Curve>), // for the route type, independant of initial delay
    SemiSpecific(Box<dyn Curve>), // for the specific route, route_variant and end_stop_id, but independant of initial delay
    SpecificCurve(Box<dyn Curve>), // for the specific route, route_variant, {start and end}_stop_id and initial delay
    SpecificCurveSet(CurveSet<f32, IrregularDynamicCurve<f32, f32>>), // for the specific route, route_variant, {start and end}_stop_id, but independant of initial delay
}

impl PredictionResult {
    pub fn to_stop_time_event_extension(&self) -> StopTimeEventExtension {
        match self {
            Self::General(curve)       => Self::ext_from_curve(curve, PredictionType::General),
            Self::SemiSpecific(curve)  => Self::ext_from_curve(curve, PredictionType::SemiSpecific),
            Self::SpecificCurve(curve) => Self::ext_from_curve(curve, PredictionType::SpecificCurve),
            Self::SpecificCurveSet(_)  => panic!("Can't process SpecificCurveSet yet."),
        }
    } 

    fn ext_from_curve(curve: &Box<dyn Curve>, p_type: PredictionType) -> StopTimeEventExtension {
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

    fn points_from_curve(curve: &Box<dyn Curve>) -> Vec<gtfs_rt::Point> {
        multizip(curve.get_values_as_vectors()).map(|(x, y)| gtfs_rt::Point {time: x, probability: y} ).collect()
    }
}

impl Display for PredictionResult
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::General(curve)               => write!(f, "General ({})", curve),
            Self::SemiSpecific(curve)          => write!(f, "SemiSpecific ({})", curve),
            Self::SpecificCurve(curve)         => write!(f, "SpecificCurve ({})", curve),
            Self::SpecificCurveSet(curve_set)  => write!(f, "SpecificCurveSet ({})", curve_set),
        }
    }
}