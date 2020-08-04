use crate::{FnResult, Main, date_and_time, OrError};
use chrono::{NaiveDate, NaiveDateTime, Utc, Duration};
use clap::{App, ArgMatches};
use crate::types::*;
use std::sync::Arc;
use gtfs_structures::Gtfs;
use mysql::*;
use mysql::prelude::*;

use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::header::{HeaderValue};
use hyper::service::{make_service_fn, service_fn};
use itertools::Itertools;

mod css;
use css::CSS;

use typed_html::{html, dom::DOMTree, text};
use percent_encoding::percent_decode_str;

use dystonse_curves::{IrregularDynamicCurve, Tup, Curve};

#[derive(Clone)]
pub struct Monitor {
    pub schedule: Arc<Gtfs>,
    pub pool: Arc<Pool>,
    pub source: String
}

impl Monitor {
    pub fn get_subcommand() -> App<'static>{
        App::new("monitor").about("Starts a web server that serves the monitor website.")
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(main: &Main, _sub_args: &ArgMatches) -> FnResult<()> {
        let monitor = Monitor {
            schedule: main.get_schedule()?.clone(),
            pool: main.pool.clone(),
            source: main.source.clone(),
        };

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            serve_monitor(Arc::new(monitor)).await
        });

        Ok(())
    }
}


async fn serve_monitor(monitor: Arc<Monitor>) {
    let port = 3000;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let monitor = monitor.clone();

    // A `Service` is needed for every connection, so this
    // creates one from our `handle_request` function.
    let make_svc = make_service_fn(move |_conn| {

        let monitor = monitor.clone();
        async move {
            // service_fn converts our function into a `Service`
            let monitor = monitor.clone();
            Ok::<_, Infallible>(service_fn( move |request: Request<Body>| {
                let monitor = monitor.clone();
                async move {
                    handle_request(request, monitor.clone()).await
                }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    println!("Waiting for connections on {}…", addr);
    // Run this server for... forever!
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn handle_request(req: Request<Body>, monitor: Arc<Monitor>) -> std::result::Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::empty());

    let path_parts : Vec<String> = req.uri().path().split('/').map(|part| percent_decode_str(part).decode_utf8_lossy().into_owned()).collect();
    let path_parts_str : Vec<&str> = path_parts.iter().map(|string| string.as_str()).collect();
    match &path_parts_str[1..] {
        [""] => generate_search_page(&mut response, &monitor),
        ["stop-by-name"] => {
            // an "stop-by-name" URL just redirects to the corresponding "stop" URL. We can't have pretty URLs in the first place because of the way HTML forms work
            let query_params = url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes());
            let stop_name = query_params.filter_map(|(key, value)| if key == "start" { Some(value)} else { None } ).next().unwrap();
            let new_path = format!("/stop/{}", stop_name);
            response.headers_mut().append(hyper::header::LOCATION, HeaderValue::from_str(&new_path).unwrap());
            *response.status_mut() = StatusCode::FOUND;
        },
        ["stop", ..] => {
            let journey = &path_parts[2..]; // we would need half-open pattern matching to get rid of this line, see https://github.com/rust-lang/rust/issues/67264
            let start_time: f32 = 123.0; // TODO insert f32 represention of the current time here
            let points = vec![Tup{x: start_time, y: 0.0}, Tup{x: start_time + 1.0, y: 1.0}];
            let arrival = IrregularDynamicCurve::new(points);
            handle_route_with_stop(&mut response, &monitor, arrival, journey);
        },
        slice => {
            generate_error_page(&mut response, StatusCode::NOT_FOUND, &format!("Keine Seite entsprach dem Muster {:?}.", slice));
        },
    };

    Ok(response)
}

fn generate_search_page(response: &mut Response<Body>, monitor: &Arc<Monitor>) {
    println!("{} Haltestellen gefunden.", monitor.schedule.stops.len());
    //TODO: handle the different GTFS_SOURCE_IDs in some way
    let doc: DOMTree<String> = html!(
        <html>
            <head>
                <title>"ÖPNV-Reiseplaner"</title>
                <style>{ text!("{}", CSS)}</style>
            </head>
            <body>
                <h1>"Reiseplaner"</h1>
                <p class="official">
                    "Herzlich willkommen. Hier kannst du deine Reiseroute mit dem ÖPNV im VBN (Verkehrsverbund Bremen/Niedersachsen) planen."
                </p>
                <form method="get" action="/stop-by-name">
                    <p class="dropdown" >
                        <label for="start">"Start-Haltestelle:"</label>
                        <input list="stop_list" id="start" name="start" />
                        <datalist id="stop_list">
                            { monitor.schedule.stops.iter().map(|(_, stop)| stop.name.clone()).sorted().unique().map(|name| html!(
                                <option>{ text!("{}", name) }</option>
                            )) }
                        </datalist>
                    </p>
                    <input type="submit" value="Absenden"/>
                </form>
            </body>
        </html>
    );
    let doc_string = doc.to_string();
    *response.body_mut() = Body::from(doc_string);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
}

fn handle_route_with_stop(response: &mut Response<Body>, monitor: &Arc<Monitor>, _arrival: IrregularDynamicCurve<f32, f32>, journey: &[String]) {
    if journey.len() == 1 {
        let _res = generate_stop_page(response, monitor, journey[0].clone());
    } else {
        generate_error_page(response, StatusCode::BAD_REQUEST, &format!("Currently, only journeys with length 1 are supported, found '{:?}'.", journey));
    }
}

fn generate_error_page(response: &mut Response<Body>, code: StatusCode, message: &str) {
    let doc_string = format!("{}: {}", code.as_str(), message);
    *response.body_mut() = Body::from(doc_string);
    *response.status_mut() = code;
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
}

fn generate_stop_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, stop_name: String) -> FnResult<()> {
    let stop_ids : Vec<String> = monitor.schedule.stops.iter().filter_map(|(id, stop)| if stop.name == stop_name {Some(id.to_string())} else {None}).collect();
    
    println!("Found {} stop_ids for {}: {:?}", stop_ids.len(), stop_name, stop_ids);

    // TODO get real departures from the database and/or schedule
    // probably we need to query the database to know which departures we should show, and then use the schedule to 
    // get all the data needed to actually show something
    
    let mut departures : Vec<DbPrediction> = Vec::new();
    let min_time = Utc::now().naive_utc();
    let max_time = min_time + Duration::minutes(30);
    let fmt = "%H:%M";

    for stop_id in stop_ids {
        departures.extend(get_predictions(monitor, monitor.source.clone(), EventType::Departure, stop_id, min_time, max_time)?);
    }

    println!("Found {} departure predictions.", departures.len());

    for dep in &mut departures {
        let _res = dep.compute_meta_data(monitor);
    }

    // Remove the top and bottom 1% of the predicted time span. 
    // They mostly contain outliers with several hours of (sometimes negative) delay.
    departures.retain(|dep| {
        if dep.meta_data.is_some() {
            let time_absolute_01 = dep.get_absolute_time_for_probability(0.01).unwrap();
            let time_absolute_99 = dep.get_absolute_time_for_probability(0.99).unwrap();
            
            time_absolute_01 < max_time && time_absolute_99 > min_time
        } else {
            false
        }
    });

    println!("Kept {} departure predictions based on removing the top and bottom 1%.", departures.len());


    // Remove duplicates, for which there is a scheduled predcition and a realtime prediction
    // which concern the same vehicle, but have not been overwritten in the DB  due to
    // different primary keys (probably a changed trip_id).
    let departures_copy = departures.clone();

    // local function, which is used in the retain predicate below
    fn is_duplicate(a: &DbPrediction, b: &DbPrediction) -> bool {
        b.route_id == a.route_id &&
        b.trip_start_date == a.trip_start_date &&
        b.trip_start_time == a.trip_start_time &&
        b.origin_type == OriginType::Realtime
    }

    departures.retain(|dep| {
        dep.origin_type == OriginType::Realtime || !departures_copy.iter().any(|dc| is_duplicate(dep, dc))
    });

    println!("Kept {} departure predictions after removing duplicates.", departures.len());

    departures.sort_by_cached_key(|dep| dep.get_absolute_time_for_probability(0.50).unwrap());

    let doc: DOMTree<String> = html!(
        <html>
            <head>
                <title>"ÖPNV-Reiseplaner"</title>
                <style>{ text!("{}", CSS)}</style>
            </head>
            <body>
                <h1>{ text!("Abfahrten für {} von {} bis {}", stop_name, min_time.format(fmt), max_time.format(fmt)) }</h1>
                <ul>
                    { 
                        departures.iter().map(|dep| {
                            match create_departure_output(&dep) {
                                Ok(string) => html!(<li>{text!("{}", string)}</li>),
                                Err(e) => html!(<li>{text!("Fehler: {:?}", e)}</li>)
                            }
                        })
                    }
                </ul>
            </body>
        </html>
    );
    let doc_string = doc.to_string();
    *response.body_mut() = Body::from(doc_string);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

fn create_departure_output(dep: &DbPrediction) -> FnResult<String> {
    let md = dep.meta_data.as_ref().unwrap();
    let p_05 = dep.get_absolute_time_for_probability(0.05).unwrap();
    let p_50 = dep.get_absolute_time_for_probability(0.50).unwrap();
    let p_95 = dep.get_absolute_time_for_probability(0.95).unwrap();
    
    // let mut fg = Figure::new();
    // let axes = fg.axes2d();
    // let c_plot = dep.prediction_curve.get_values_as_vectors();
    // axes.lines_points(&c_plot.0, &c_plot.1, &[Color("grey")]);
    // // TODO generate a unique name for a temporary file here, 
    // // generate an img-Element with that filename, and then
    // // when the request for the image arrives, wait until the file is written.
    // fg.save_to_svg("data/monitor/tmp.svg", 800, 128)?;

    let fmt = "%H:%M";

    Ok(format!("{} nach {} um {} ({} bis {}, zu 95% zwischen {} und {}, Median {}), Origin: {:?}, Precision: {:?}, Sample Size: {}", 
        md.route_name, md.headsign, 
        md.scheduled_time_absolute.format(fmt), dep.prediction_min.format(fmt), dep.prediction_max.format(fmt), 
        p_05.format(fmt), p_95.format(fmt), p_50.format(fmt),
        dep.origin_type, dep.precision_type, dep.sample_size
    ))
}

#[derive(Debug, Clone)]
struct DbPrediction {
    pub route_id: String,
    pub trip_id: String,
    pub trip_start_date: NaiveDate,
    pub trip_start_time: Duration, // time from midnight, may be outside 0:00 .. 24:00
    pub prediction_min: NaiveDateTime, 
    pub prediction_max: NaiveDateTime,
    pub precision_type: PrecisionType,
    pub origin_type: OriginType,
    pub sample_size: i32,
    pub prediction_curve: IrregularDynamicCurve<f32, f32>,
    pub stop_id: String,

    pub meta_data: Option<DbPredictionMetaData>,
}

#[derive(Debug, Clone)]
struct DbPredictionMetaData {
    pub route_name : String,
    pub headsign : String,
    pub stop_index : usize,
    pub scheduled_time_seconds : u32,
    pub scheduled_time_absolute : NaiveDateTime,
}

impl DbPrediction {
    pub fn compute_meta_data(&mut self, monitor: &Arc<Monitor>) -> FnResult<()> {
        let trip = monitor.schedule.get_trip(&self.trip_id)?;
        let route_name = monitor.schedule.get_route(&self.route_id)?.short_name.clone();
        let headsign = trip.trip_headsign.as_ref().or_error("trip_headsign is None")?.clone();
        let stop_index = trip.get_stop_index_by_id(&self.stop_id).or_error("stop_index is None")?;
        let scheduled_time_seconds = trip.stop_times[stop_index].departure_time.or_error("departure_time is None")?;
        let scheduled_time_absolute = date_and_time(&self.trip_start_date, scheduled_time_seconds as i32);

        self.meta_data = Some(DbPredictionMetaData{ 
            route_name,
            headsign,
            stop_index,
            scheduled_time_seconds,
            scheduled_time_absolute,
        });
        
        Ok(())
    }

    pub fn get_absolute_time_for_probability(&self, prob: f32) -> FnResult<NaiveDateTime> {
        let x = self.prediction_curve.x_at_y(prob);
        Ok(date_and_time(&self.trip_start_date, self.meta_data.as_ref().or_error("Prediction has no meta_data")?.scheduled_time_seconds as i32 + x as i32))
    }
}

impl FromRow for DbPrediction {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        Ok(DbPrediction{
            route_id:           row.get_opt(0).unwrap().unwrap(),
            trip_id:            row.get_opt(1).unwrap().unwrap(),
            trip_start_date:    row.get_opt(2).unwrap().unwrap(),
            trip_start_time:    row.get_opt(3).unwrap().unwrap(),
            prediction_min:     row.get_opt(4).unwrap().unwrap(),
            prediction_max:     row.get_opt(5).unwrap().unwrap(),
            precision_type:     PrecisionType::from_int(row.get_opt(6).unwrap().unwrap()),
            origin_type:        OriginType::from_int(row.get_opt(7).unwrap().unwrap()),
            sample_size:        row.get_opt(8).unwrap().unwrap(),
            prediction_curve:   IrregularDynamicCurve::<f32, f32>
                                    ::deserialize_compact(row.get_opt(9).unwrap().unwrap()),
            stop_id:            row.get_opt(10).unwrap().unwrap(),
            meta_data:          None,
        })
    }
}

fn get_predictions(
    monitor: &Arc<Monitor>,
    source: String, 
    event_type: EventType, 
    stop_id: String, 
    min_time: NaiveDateTime, 
    max_time: NaiveDateTime
) -> FnResult<Vec<DbPrediction>> {
    let mut conn = monitor.pool.get_conn()?;
    let stmt = conn.prep(
        r"SELECT 
            `route_id`,
            `trip_id`,
            `trip_start_date`,
            `trip_start_time`,
            `prediction_min`, 
            `prediction_max`,
            `precision_type`,
            `origin_type`,
            `sample_size`,
            `prediction_curve`,
            `stop_id`
        FROM
            `predictions` 
        WHERE 
            `source`=:source AND 
            `event_type`=:event_type AND
            `stop_id`=:stop_id AND
            `prediction_min` < :max_time AND 
            `prediction_max` > :min_time;",
    )?;

    let mut result = conn.exec_iter(
        &stmt,
        params! {
            "source" => source,
            "event_type" => event_type.to_int(),
            "stop_id" => stop_id,
            "min_time" => min_time,
            "max_time" => max_time,
        },
    )?;

    let result_set = result.next_set().unwrap()?;

    let db_predictions: Vec<_> = result_set
        .map(|row| {
            let item: DbPrediction = from_row(row.unwrap());
            item
        })
        .collect();

    Ok(db_predictions)
}