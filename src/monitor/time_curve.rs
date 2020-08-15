use dystonse_curves::{Curve, TypedCurve, IrregularDynamicCurve};
use chrono::{DateTime, Local, Duration};

#[derive(Debug, Clone)]
pub struct TimeCurve {
    pub curve: IrregularDynamicCurve<f32, f32>,
    pub ref_time: DateTime<Local>
}

impl TimeCurve {
    pub fn new(curve: IrregularDynamicCurve<f32, f32>, ref_time: DateTime<Local>) -> Self {
        TimeCurve{
            curve,
            ref_time
        }
    }

    pub fn get_transfer_probability(
        &self,
        departure: &TimeCurve
    ) -> f32 {
        let mut total_miss_prob = 0.0;
        let step_size = 1;
        for percentile in (0..100).step_by(step_size) {
            // compute the absolute time at which the arrival occurs for this percentile
            let arrival_time_abs = self.typed_x_at_y(percentile as f32 / 100.0);
            // compute the pobability of missing the transfer for this arrival percentile
            let transfer_missed_prob = departure.typed_y_at_x(arrival_time_abs);
            total_miss_prob += transfer_missed_prob / (100.0 / step_size as f32);
        }
        1.0 - total_miss_prob 
    }
}

impl TypedCurve<DateTime<Local>, f32> for TimeCurve {
    fn typed_min_x(&self) -> DateTime<Local> {
        self.ref_time + Duration::seconds(self.curve.min_x() as i64)
    }

    fn typed_max_x(&self) -> DateTime<Local> {
        self.ref_time + Duration::seconds(self.curve.max_x() as i64)
    }

    fn typed_y_at_x(&self, x: DateTime<Local>) -> f32 {
        let rel_time = x.signed_duration_since(self.ref_time).num_seconds();
        self.curve.y_at_x(rel_time as f32)
    }

    fn typed_x_at_y(&self, y: f32) -> DateTime<Local> {
        self.ref_time + Duration::seconds(self.curve.x_at_y(y) as i64)
    }

}