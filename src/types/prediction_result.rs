use dystonse_curves::{Curve, CurveSet, IrregularDynamicCurve};

#[allow(dead_code)]
pub enum PredictionResult {
    General(Box<dyn Curve>), // for the route type, independant of initial delay
    SemiSpecific(Box<dyn Curve>), // for the specific route, route_variant and end_stop_id, but independant of initial delay
    SpecificCurve(Box<dyn Curve>), // for the specific route, route_variant, {start and end}_stop_id and initial delay
    SpecificCurveSet(CurveSet<f32, IrregularDynamicCurve<f32, f32>>), // for the specific route, route_variant, {start and end}_stop_id, but independant of initial delay
}