use simple_error::bail;

use dystonse_curves::irregular_dynamic::*;
use dystonse_curves::Curve;

use crate::FnResult;

// This method determines whether there should be another marker between the ones already present at lower and upper.
// Upper and lower are initial delay by seconds.
pub fn recurse(initial_delay_curve: &IrregularDynamicCurve<f32, f32>, markers: &mut Vec<f32>, lower: f32, upper: f32, count: f32) {
    // let's recap what initial_delay_curve is: Along the x axis, we have the initial delays in seconds. Along the y axis,
    // we have the share of vehicles which had this delay or less. We need the count to make that into abolute numbers.

    // new marker mus be at least 20 seconds away from the existing ones
    let min_x_by_delay = lower + 20.0;
    let max_x_by_delay = upper - 20.0;

    // between the new marker and existing ones, at least 20 data points must exist
    // this computation is tedious because y is measured relatively but we have an
    // absolute distance (20 datapoints) to keep. 
    let lower_y = initial_delay_curve.y_at_x(lower);
    let upper_y = initial_delay_curve.y_at_x(upper);
    let min_y_by_count = lower_y + (20.0 / count);
    let max_y_by_count = upper_y - (20.0 / count);
    
    // Also, we need x bounds:
    let min_x_by_count = initial_delay_curve.x_at_y(min_y_by_count);
    let max_x_by_count = initial_delay_curve.x_at_y(max_y_by_count);
    
    // For the x axis, we have two minimum and two maximum bounds.
    // Let's find the stricter ones.
    let min_x = f32::max(min_x_by_delay, min_x_by_count);
    let max_x = f32::min(max_x_by_delay, max_x_by_count);

    // The bounds might contradict, and in that case, we won't subdivide
    if min_x <= max_x {
        let mid_x = (min_x + max_x) / 2.0;
        recurse(initial_delay_curve, markers, lower, mid_x, count);
        markers.push(mid_x);
        recurse(initial_delay_curve, markers, mid_x, upper, count);
    }
}

pub fn get_weight(delay: f32, focus: Option<f32>, min_delay: f32, max_delay: f32) -> f32 {
    // handling delay values outside of given bounds: always 0.
    if delay < min_delay || delay > max_delay {
        return 0.0;
    }

    if let Some(focus) = focus {
        // if focus is given, weight is 1 at the focus and goes down to zero 
        // towards the bounds given by min_delay and max_delay
        if delay == focus {
            1.0
        } else if delay < focus {
            (delay - min_delay) / (focus - min_delay)
        } else {
            1.0 - ((delay - focus) / (max_delay - focus))
        }
    } else {
        1.0
    }
}

pub fn make_curve(values: &Vec<f32>, focus: Option<f32>) -> FnResult<(IrregularDynamicCurve<f32, f32>, f32)> {
    let mut own_values = values.clone(); // TODO maybe we don't need to clone this
    own_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min_delay = *own_values.first().unwrap();
    let max_delay = *own_values.last().unwrap();
    
    let pairs: Vec<(f32,f32)> = own_values.iter().map(|v| (*v, get_weight(*v, focus, min_delay, max_delay))).collect();

    let sum_of_weights: f32 = pairs.iter().map(|(_v, w)| *w).sum();

    let mut tups = Vec::<Tup<f32, f32>>::with_capacity(own_values.len());
    let mut last_x :f32 = 0.0;
    let mut i = 0.0;
    for (x, w) in pairs.iter() {
        i += w;
        if *x != last_x {
            tups.push(Tup {x: *x, y: (i as f32) / sum_of_weights});
            last_x = *x;
        }
    }

    if tups.len() < 2 {
        bail!("Curve would have only {} points, skipping.", tups.len());
    }

    tups.first_mut().unwrap().y = 0.0;
    tups.last_mut().unwrap().y = 1.0;

    Ok((IrregularDynamicCurve::new(tups), sum_of_weights))
}