use dystonse_curves::{Curve, TypedCurve, IrregularDynamicCurve, Tup};
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

    pub fn add_duration_curve(&self, duration: &IrregularDynamicCurve<f32, f32>) -> TimeCurve {
        // domain of the resulting curve:
        let mut min_n : i32 = (self.curve.x_at_y(0.01) + duration.x_at_y(0.01)).floor() as i32;
        let mut max_n : i32 = (self.curve.x_at_y(0.99) + duration.x_at_y(0.99)).ceil()  as i32;

        let step_size : i32 = i32::max(12, (max_n - min_n) / 200 * 2);
        let half_step = step_size / 2;

        min_n -= step_size;
        max_n += step_size;

        // domain of the duration curve:
        let min_k : i32 = duration.min_x() as i32 - step_size;
        let max_k : i32 = duration.max_x().ceil() as i32 + step_size;

        let mut points = Vec::with_capacity(((max_n - min_n)/step_size + 2) as usize);

        let mut sum = 0.0;
        for n in (min_n..max_n).step_by(step_size as usize) { // create one point for every step_size seconds
            for k in (min_k..max_k).step_by(step_size as usize) {
                // Formula (as LaTeX) from Wikipedia: https://de.wikipedia.org/wiki/Faltung_(Mathematik)#Diskrete_Faltung
                // (f*g)(n)=\sum _{{k\in D}}f(k)g(n-k).

                // also converting y values into non-cumulated form:
                let self_at_n_minus_k = self.curve.y_at_x((n - k + half_step) as f32) - self.curve.y_at_x((n - k - half_step) as f32);
                let duration_at_k     = duration.y_at_x((k + half_step) as f32) - duration.y_at_x((k - half_step) as f32);
                
                sum += f32::max(0.0, self_at_n_minus_k * duration_at_k); // should never be negative anyway, but somehow it sometimes was ¯\_(ツ)_/¯
            }
            if points.is_empty() {
                points.push(Tup {x: n as f32 - step_size as f32, y: 0.0});
            }
            if sum > 1.0 {
                break;
            }
            points.push(Tup {x: n as f32, y: sum});
        }
        points.push(Tup {x: max_n as f32 + step_size as f32, y: 1.0});
        let mut rel_result_curve = IrregularDynamicCurve::<f32, f32>::new(points);
        rel_result_curve.simplify(0.05);
        let abs_result_curve = TimeCurve::new(rel_result_curve, self.ref_time);

        abs_result_curve
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