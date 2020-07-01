use std::fs;
use std::fs::File;
use std::io::prelude::*;

use clap::ArgMatches;
use gtfs_structures::{RouteType, Trip};
use gnuplot::*;

use dystonse_curves::irregular_dynamic::*;
use dystonse_curves::{Curve, curve_set::CurveSet};

use crate::types::{RouteData, RouteVariantData, TimeSlot};

use super::Analyser;

use crate::FnResult;
use crate::Main;

pub struct CurveDrawer<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub args: &'a ArgMatches
}

impl<'a> CurveDrawer<'a> {

    pub fn run_curves(&self) -> FnResult<()> {
        if let Some(route_ids) = self.args.values_of("route-ids") {
            println!("Handling {} route ids…", route_ids.len());
            for route_id in route_ids {
                self.create_curves_for_route(&String::from(route_id))?;
            }
        } else {
            println!("I've got no route!");
        }
        Ok(())
    }

    fn create_curves_for_route(&self, route_id: &String)  -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        let route = schedule.get_route(route_id)?;
        let agency_id = route.agency_id.as_ref().unwrap().clone();
        let agency_name = schedule
            .agencies
            .iter()
            .filter(|agency| agency.id.as_ref().unwrap() == &agency_id)
            .next()
            .unwrap()
            .name
            .clone();

        let dir_name = format!("data/curve_data/{}", agency_name);
        let file_name = format!("{}/Linie_{}.crv", dir_name, route.short_name);
        
        let mut f = File::open(file_name).unwrap();
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;

        let route_data: RouteData = rmp_serde::from_read_ref(&buffer).unwrap();


        println!("Working on route {} of agency {}.", route.short_name, agency_name);

        for (route_variant, route_variant_data) in route_data.variants {
            let variant_as_string = Some(format!("{}", route_variant));
            let trip = schedule.trips.values().filter(|trip| trip.route_id == *route.id && trip.route_variant == variant_as_string).next();

            match trip {
                None => {
                    println!("Could not find trip for route_variant {}.", route_variant);
                },
                Some(trip) => {
                    let mode = match route.route_type {
                        RouteType::Tramway => "Straßenbahn",
                        RouteType::Bus => "Bus",
                        RouteType::Rail => "Zug",
                        RouteType::Subway => "U-Bahn",
                        _ => ""
                    };
                
                    let headsign = trip.trip_headsign.as_ref().unwrap_or(&trip.stop_times.last().unwrap().stop.name).clone();    
                    let dir_name = format!("data/curve_img/{}/Linie_{}/{}", agency_name, route.short_name, route_variant);
                    
                    fs::create_dir_all(&dir_name)?;                
                    let title_prefix = &format!("{} - {} Linie {} nach {}", agency_name, mode, route.short_name, headsign);
                    
                    //self.create_percentile_curves_for_route_variant(title_prefix, &dir_name, trip, &rows_matching_variant)?;
                    //self.create_delay_curves_for_route_variant(title_prefix, &dir_name, trip, &rows_matching_variant, false)?;
                    //self.create_delay_curves_for_route_variant(title_prefix, &dir_name, trip, &rows_matching_variant, true)?;
                    self.create_curves_for_route_variant(route_variant_data, trip, title_prefix, &dir_name)?;
                }
            }
        }

        Ok(())
    }

    // // create a single figure with stations along the x axis.
    // // the y axis is, as usual, the proability between 0 and 1,
    // // and the curves will be for specific delays.
    // fn create_percentile_curves_for_route_variant(
    //     &self, title_prefix: &str, 
    //     dir_name: &str, 
    //     trip: &Trip, 
    //     rows_matching_variant: &Vec<&DbItem>
    // ) -> FnResult<()> {
    //     let delays = [-120, -60, 0, 30, 60, 120, 300, 500];

    //     let mut fg = Figure::new();
    //     fg.set_title(&format!("{} - Verspätung in Perzentilen", title_prefix));

    //     let axes = fg.axes2d();
    //     axes.set_legend(
    //         Graph(0.97), 
    //         Graph(0.03), 
    //         &[Title("Verspätung in Sekunden"), Placement(AlignRight, AlignBottom), Invert], 
    //         &[]
    //     );
    //     axes.set_x_ticks_custom(
    //         trip.stop_times.iter().enumerate().map(|(i, s)| Major(i as f32, Fix(s.stop.name.clone()))),
	// 		&[MajorScale(1.0), OnAxis(false)],
	// 		&[Rotate(-90.0), TextAlign(AlignRight)],
    //     );
    //     axes.set_grid_options(true, &[LineStyle(Dot), Color("#AAAAAA")]).set_y_grid(true);
    //     axes.set_y_ticks(Some((Fix(10.0), 1)), &[MinorScale(0.5), MajorScale(1.0), Format("%.0f %%")], &[]);


    //     let stop_count = trip.stop_times.len();

    //     // Create a temporary curve for each stop, with the distribution of delays at that stop
    //     let mut curves = Vec::<Option<IrregularDynamicCurve<f32, f32>>>::with_capacity(stop_count);
    //     for st in trip.stop_times.iter() {
    //         // Locally select the rows which match the start station
    //         let rows_matching_start : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st.stop.id).filter_map(|r| r.delay_departure).map(|d| d as f32).collect();
    //         if rows_matching_start.len() > 10 {
    //             let potential_curve = self.make_curve(&rows_matching_start, None);
    //             match potential_curve {
    //                 Some(curve) => {
    //                     curves.push(Some(curve.0));
    //                 },
    //                 None => {
    //                     curves.push(None);
    //                 }
    //             }
    //         } else {
    //             curves.push(None);
    //         }
    //     }

    //     // Now, for every one of the predefined delays, draw a curve
    //     for (i, delay) in delays.iter().enumerate() {
    //         // for this delay, map the the probability for each station
    //         let mut x_coords = Vec::<f32>::new();
    //         let mut y_coords = Vec::<f32>::new();

    //         for (i, potential_curve) in curves.iter().enumerate() {
    //             match potential_curve {
    //                 Some(curve) => {
    //                     x_coords.push(i as f32);
    //                     y_coords.push(curve.y_at_x(*delay as f32) * 100.0);
    //                 },
    //                 None => {
                        
    //                 }
    //             }
    //         }
    //         let color = format!("#{:x}", colorous::TURBO.eval_rational(i, delays.len()));
    //         axes.lines_points(&x_coords, &y_coords, &[Caption(&format!("{}s", delay)), PointSize(0.6), Color(&color), LineWidth(1.0)]);
    //     }

    //     let filename = format!("{}/all_stops_by_percentile.svg", dir_name);
    //     fg.save_to_svg(filename, 1024, 768)?;


    //     Ok(())
    // }

    // // create a single figure with stations along the x axis.
    // // the y axis is the amount of delay,
    // // and the curves will be for specific percentiles.
    // fn create_delay_curves_for_route_variant(
    //     &self, title_prefix: &str, 
    //     dir_name: &str, 
    //     trip: &Trip, 
    //     rows_matching_variant: &Vec<&DbItem>,
    //     draw_box_plot: bool
    // ) -> FnResult<()> {
    //     let percentiles = [0.0, 0.025, 0.05, 0.25, 0.5, 0.75, 0.95, 0.975, 1.0];

    //     let mut fg = Figure::new();
    //     if draw_box_plot {
    //         fg.set_title(&format!("{} - Verspätung als Box-Plot", title_prefix));
    //     } else {
    //         fg.set_title(&format!("{} - Verspätung in Perzentilen", title_prefix));
    //     }
    //     let axes = fg.axes2d();
    //     axes.set_y_range(gnuplot::AutoOption::Fix(-150.0),gnuplot::AutoOption::Fix(450.0));
    //     axes.set_legend(
    //         Graph(0.97), 
    //         Graph(0.03), 
    //         &[Title("Perzentile"), Placement(AlignRight, AlignBottom), Invert], 
    //         &[]
    //     );
       
    //     let stop_count = trip.stop_times.len();

    //     // Create a temporary curve for each stop, with the distribution of delays at that stop
    //     let mut curves = Vec::<Option<IrregularDynamicCurve<f32, f32>>>::with_capacity(stop_count);
    //     let mut rows_per_stop: Vec<usize> = Vec::with_capacity(stop_count);
    //     for st in trip.stop_times.iter() {
    //         // Locally select the rows which match the start station
    //         let rows_matching_start : Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st.stop.id).filter_map(|r| r.delay_departure).map(|d| d as f32).collect();
    //         rows_per_stop.push(rows_matching_start.len());
    //         if rows_matching_start.len() > 10 {
    //             let potential_curve = self.make_curve(&rows_matching_start, None);
    //             match potential_curve {
    //                 Some(curve) => {
    //                     curves.push(Some(curve.0));
    //                 },
    //                 None => {
    //                     curves.push(None);
    //                 }
    //             }
    //         } else {
    //             curves.push(None);
    //         }
    //     }

    //     axes.set_x_ticks_custom(
    //         trip.stop_times.iter().enumerate().map(|(i, s)| {
    //             let tick_name = format!("({}) {}", rows_per_stop[i], s.stop.name.clone());
    //             Major(i as f32, Fix(tick_name))
    //         }),
	// 		&[MajorScale(1.0), OnAxis(false)],
	// 		&[Rotate(-90.0), TextAlign(AlignRight)],
    //     );
    //     axes.set_grid_options(true, &[LineStyle(Dot), Color("#AAAAAA")]).set_y_grid(true);
    //     axes.set_y_ticks(Some((Fix(60.0), 4)), &[MinorScale(0.5), MajorScale(1.0)], &[]);


    //     let actual_curves: Vec<_> = curves.iter().enumerate().filter_map(|(i, c)| {
    //         match c {
    //             Some(c) => Some((i, c)),
    //             None => None
    //         }
    //     }).collect();

    //     if draw_box_plot {
    //         let mut rng = rand::thread_rng();

    //         axes.box_and_whisker(
    //             actual_curves.iter().map(|(i, _c)| i),
    //             actual_curves.iter().map(|(_i, c)| c.x_at_y(0.25)),
    //             actual_curves.iter().map(|(_i, c)| c.x_at_y(0.025)),
    //             actual_curves.iter().map(|(_i, c)| c.x_at_y(0.975)),
    //             actual_curves.iter().map(|(_i, c)| c.x_at_y(0.75)),
    //             &[WhiskerBars(1.0), Color("black")]
    //         );

    //          // draw medians (somehow can't pass them to box_and_whisker)
    //         axes.points(
    //             actual_curves.iter().map(|(i, _c)| i),
    //             actual_curves.iter().map(|(_i, c)| c.x_at_y(0.5)),
    //             &[Color("black"), PointSymbol('+')]
    //         );

    //         // draw outliers
    //         for (i, st) in trip.stop_times.iter().enumerate() {
    //             // Locally select the rows which match the start station
    //             let rows_matching_start: Vec<_> = rows_matching_variant.iter().filter(|item| item.stop_id == st.stop.id).filter_map(|r| r.delay_departure).map(|d| d as f32).collect();
                
    //             let potential_curve = actual_curves.iter().filter(|(actual_i, _c)| *actual_i == i).next();
    //             let delays = match potential_curve {
    //                 Some((_i, c)) => {
    //                     let min_inlier = c.x_at_y(0.025);
    //                     let max_inlier = c.x_at_y(0.975);
    //                     rows_matching_start.iter().filter(|d| **d < min_inlier || **d > max_inlier).map(|d| *d).collect()
    //                 },
    //                 None => {
    //                     rows_matching_start
    //                 }
    //             };

    //             if delays.len() > 0 {
    //                 let size = f32::max(0.25, 0.6 - (delays.len() as f32 / 50.0));
    //                 axes.points(
    //                     std::iter::repeat(i).take(delays.len()).map(|x| x as f32 + rng.gen_range(-0.15, 0.15)),
    //                     delays,
    //                     &[Color("#99440000"), PointSymbol('O'), PointSize(size as f64)]
    //                 );
    //             }
    //         }


    //         let filename = format!("{}/all_stops_by_delay_box.svg", dir_name);
    //         fg.save_to_svg(filename, 1024, 768)?;
    //     } else {
    //         // Now, for every one of the predefined delays, draw a curve
    //         for (i, percentile) in percentiles.iter().enumerate() {
    //             // for this delay, map the the probability for each station
    //             let points: Vec<_> = actual_curves.iter().map(|(i, curve)| {
    //                 (*i as f32, curve.x_at_y(*percentile))
    //             }).collect();
    //             let color = format!("#{:x}", colorous::TURBO.eval_rational(i, percentiles.len()));
    //             axes.lines_points(
    //                 points.iter().map(|(x, _y)| x), 
    //                 points.iter().map(|(_x, y)| y), 
    //                 &[Caption(&format!("{:.1}%", percentile * 100.0)), PointSize(0.6), Color(&color), LineWidth(1.0)]);
    //         }
    //         let filename = format!("{}/all_stops_by_delay.svg", dir_name);
    //         fg.save_to_svg(filename, 1024, 768)?;
    //     }

    //     Ok(())
    // }

    fn create_curves_for_route_variant(
        &self, 
        data: RouteVariantData, 
        _trip: &Trip, title_prefix: &str,
        dir_name: &str
    ) -> FnResult<()> {
        let schedule = &self.analyser.schedule;
        // let stop_count = trip.stop_times.len();

        // We need to make an image for each pair of start and end station along the route where
        // the end station comes after the start station.

        // Also we will make a figure with departure delays at every stop:
        // let mut fg_all_stops = Figure::new();
        // fg_all_stops.set_title(&format!("{} - Verspätung je Halt", title_prefix));
        // let axes_all_stops = fg_all_stops.axes2d();
        // axes_all_stops.set_x_range(gnuplot::AutoOption::Fix(-150.0),gnuplot::AutoOption::Fix(450.0));
        // axes_all_stops.set_legend(
        //     Graph(0.97), 
        //     Graph(0.03), 
        //     &[Placement(AlignRight, AlignBottom)], 
        //     &[]
        // );
        // axes_all_stops.set_x_ticks(Some((Fix(60.0), 4)), &[MinorScale(0.5), MajorScale(1.0)], &[]);
        // axes_all_stops.set_y_ticks(Some((Fix(10.0), 1)), &[MinorScale(0.5), MajorScale(1.0), Format("%.0f %%")], &[]);
        // axes_all_stops.set_grid_options(true, &[LineStyle(Dot), Color("#AAAAAA")]).set_x_grid(true).set_y_grid(true);

        // Iterate over all start stations
        for ((i_s, i_e, ts), stop_pair_data) in data.curve_sets {
            // let departues : Vec<f32> = rows_matching_start.iter().filter_map(|item| item.delay_departure).map(|d| d as f32).collect();
            // if departues.len() > 5 {
            //     let color = format!("#{:x}", colorous::TURBO.eval_rational(i_s, stop_count));
            //     let mut options = vec!{Color(color.as_str()), Caption(st_s.stop.name.as_str()), PointSize(0.6)};
            //     self.draw_to_figure(axes_all_stops, &departues, &mut options, None, false, true)?;
            // }

            let st_s = schedule.get_stop(&data.stop_ids[i_s as usize]).unwrap();
            let st_e = schedule.get_stop(&data.stop_ids[i_e as usize]).unwrap();

            let sub_dir_name = format!("{}/{}", &dir_name, self.get_time_slot_description(&ts));
            fs::create_dir_all(&sub_dir_name)?;
            let file_name = format!("{}/curve_{}_to_{}.svg", &sub_dir_name, i_s, i_e);
            let title = &format!("{} - Verspätungsentwicklung von #{} '{}' bis #{} '{}'", title_prefix, i_s, st_s.name, i_e, st_e.name);
            self.draw_curves_for_stop_pair(stop_pair_data, data.general_delay.departure.get(&i_s), data.general_delay.arrival.get(&i_e), &file_name, &title)?;
        }

        // let filename = format!("{}/all_stops.svg", &dir_name);
        // fg_all_stops.save_to_svg(filename, 1024, 768)?;

        Ok(())
    }

    fn get_time_slot_description(&self, semi_ts: &TimeSlot) -> String {
        let original_ts = TimeSlot::TIME_SLOTS.iter().filter(|ts| ts.id == semi_ts.id).next();
        if let Some(ts) = original_ts {
            return String::from(ts.description);
        } else {
            return format!("unknown_time_slot_from_{}_to_{}", semi_ts.min_hour, semi_ts.max_hour);
        }
    }

    fn draw_curves_for_stop_pair(
        &self, 
        data: CurveSet<f32, IrregularDynamicCurve<f32, f32>>, 
        general_delay_arrival: Option<&IrregularDynamicCurve<f32, f32>>, 
        general_delay_departure: Option<&IrregularDynamicCurve<f32, f32>>, 
        filename: &str, title: &str
    ) -> FnResult<()> {
        let mut fg = Figure::new();
        fg.set_title(title);
        let axes = fg.axes2d();
        axes.set_x_range(gnuplot::AutoOption::Fix(-150.0),gnuplot::AutoOption::Fix(450.0));
        axes.set_legend(
            Graph(0.97), 
            Graph(0.03), 
            &[Title("Sekunden (Anzahl Fahrten)"), Placement(AlignRight, AlignBottom)], 
            &[]
        );
        axes.set_grid_options(true, &[LineStyle(Dot), Color("#AAAAAA")]).set_x_grid(true).set_y_grid(true);
        axes.set_x_ticks(Some((Fix(60.0), 4)), &[MinorScale(0.5), MajorScale(1.0)], &[]);
        axes.set_y_ticks(Some((Fix(10.0), 1)), &[MinorScale(0.5), MajorScale(1.0), Format("%.0f %%")], &[]);

        // let mut fg_na = Figure::new();
        // fg_na.set_title(title);
        // let axes_na = fg_na.axes2d();
        // axes_na.set_x_range(gnuplot::AutoOption::Fix(-150.0),gnuplot::AutoOption::Fix(450.0));
        // axes_na.set_legend(
        //     Graph(0.97), 
        //     Graph(0.97), 
        //     &[Title("Sekunden (Anzahl Fahrten)"), Placement(AlignRight, AlignTop)], 
        //     &[]
        // );
        // axes_na.set_grid_options(true, &[LineStyle(Dot), Color("#AAAAAA")]).set_x_grid(true);
        // axes_na.set_x_ticks(Some((Fix(60.0), 4)), &[MinorScale(0.5), MajorScale(1.0)], &[]);
        // axes_na.set_y_ticks(Some((Fix(1.0), 1)), &[MinorScale(0.5), MajorScale(1.0), Format("%.0f %%")], &[]);

        // // draw the initial delay curve, which is just for debugging purposes and might be a bit confusing.
        // let (x, mut y) = initial_curve.get_values_as_vectors();
        // y = y.iter().map(|y| y*100.0).collect();
        // let caption_all_initial = format!("Anfangs - alle Daten ({})", sum as i32);
        // axes.lines_points(&x, &y, &[LineStyle(Dot), LineWidth(3.0), Caption(&caption_all_initial), Color("#129245")]);
        // //axes_na.lines_points(&[-100], &[0.005], &[Caption(""), Color("white")]);
        // let start_delays: Vec<f32> = own_pairs.iter().map(|(s,_e)| *s).collect();
        // let mut options = vec!{ Color("#129245"), Caption(&caption_all_initial), LineStyle(Dot), LineWidth(3.0), PointSize(0.6)};
        // self.draw_to_figure(axes_na, &start_delays, &mut options, None, true, false)?;
            
        // draw the overall destination delay
        
        if let Some(general_curve) = general_delay_departure {
            let (x, mut y) = general_curve.get_values_as_vectors();
            y = y.iter().map(|y| y*100.0).collect();
            axes.lines_points(&x, &y, &[LineStyle(Dot), LineWidth(3.0), Caption("Abfahrt am Start"), Color("#129245")]);
        }

        if let Some(general_curve) = general_delay_arrival {
            let (x, mut y) = general_curve.get_values_as_vectors();
            y = y.iter().map(|y| y*100.0).collect();
            axes.lines_points(&x, &y, &[LineStyle(Dash), LineWidth(3.0), Caption("Ankunft am Ende"), Color("#08421F")]);
        }

        // Add an invisible curve to display an additonal line in the legend
        axes.lines_points(&[-100], &[0.95], &[Caption("Nach Anfangsverspätung:"), Color("white")]);
        // axes_na.lines_points(&[-100], &[0.005], &[Caption("Nach Anfangsverspätung (Gewicht):"), Color("white")]);

         // Now generate and draw one or more actual result curves.
        // Each cuve will focus on the mid marker, and include all the data points from
        // the min to the max marker.
        // Remember that we added the absolute min and absolute max markers twice.
        for (i,(focus, curve)) in data.curves.iter().enumerate() {
            // println!("Doing curve for {} with values from {} to {}.", mid, lower, upper);
            let color = format!("#{:x}", colorous::PLASMA.eval_rational(i, data.curves.len() + 2)); // +2 because the end of the MAGMA scale is too light

            let options = vec!{ Color(color.as_str()), PointSize(0.6)};
            //self.draw_to_figure(axes, &slice, &mut options, Some(*mid), false, false)?;
        
            self.actually_draw_to_figure(axes, &curve, 0.0, &options, Some(*focus), false, false)?;
            
            //self.draw_to_figure(axes_na, &slice, &mut options, Some(*focus), true, false); // histogram mode
        }
        fg.save_to_svg(filename, 1024, 768)?;
        //fg_na.save_to_svg(filename.replace(".svg", "_na.svg"), 1024, 400)?;
        
        Ok(())
    }

    /// Draws a curve into `axes` using the data from `pairs`. If `focus` is Some, the data points whose delay is close to
    /// `focus` will be weighted most, whereas those close to the extremes (see local variables `min_delay` and `max_delay`) 
    /// will be weighted close to zero. Otherwise, all points will be weighted equally.
    fn actually_draw_to_figure(&self, axes: &mut gnuplot::Axes2D, curve: &IrregularDynamicCurve<f32, f32>, sum: f32, plot_options: &Vec<PlotOption<&str>>, focus: Option<f32>, non_accumulated: bool, no_points: bool) -> FnResult<()> {
        
        let mut own_options = plot_options.clone();
        
        let cap = if let Some(focus) = focus { 
            format!("ca. {}s", focus as i32)
        } else {
            let min_delay = curve.min_x();
            let max_delay = curve.max_x();
            format!("{}s bis {}s ({})", min_delay, max_delay, sum as i32)
        };
        if !own_options.iter().any(|opt| match opt { Caption(_) => true, _ => false}) {
            own_options.push(Caption(&cap));
        }

        if curve.max_x() <  curve.min_x() + 13.0 {
            println!("Curve too short.");
            return Ok(());
        }

        if non_accumulated {
            let mut x_coords = Vec::<f32>::new();
            let mut y_coords = Vec::<f32>::new();
            for x in (curve.min_x() as i32 .. curve.max_x() as i32).step_by(12) {
                let y = curve.y_at_x(x as f32 + 0.5) - curve.y_at_x(x as f32 - 0.5);
                x_coords.push(x as f32);
                y_coords.push(y * 100.0);
            }
            if no_points {
                axes.lines(&x_coords, &y_coords, &own_options);
            } else {
                axes.lines_points(&x_coords, &y_coords, &own_options);
            }
        } else {
            let (x_coords, mut y_coords) = curve.get_values_as_vectors();
            y_coords = y_coords.iter().map(|y| y*100.0).collect();
            if no_points {
                axes.lines(&x_coords, &y_coords, &own_options);
            } else {
                axes.lines_points(&x_coords, &y_coords, &own_options);
            }
        }
    
    
        Ok(())
    }
}