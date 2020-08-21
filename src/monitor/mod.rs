mod journey_data;
mod time_curve;

use crate::{FnResult, Main, date_and_time_local, OrError};
use chrono::{Date, DateTime, Local, Duration, Timelike};
use clap::{App, ArgMatches};
use crate::types::{EventType, OriginType, PrecisionType, CurveSetKey, TimeSlot, DelayStatistics, VehicleIdentifier};
use crate::FileCache;
use std::sync::Arc;
use gtfs_structures::{Gtfs, RouteType, Trip, StopTime};
use mysql::*;
use mysql::prelude::*;

use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::header::{HeaderValue};
use hyper::service::{make_service_fn, service_fn};
use itertools::Itertools;
use simple_error::bail;

use percent_encoding::{percent_decode_str, utf8_percent_encode, CONTROLS, AsciiSet};

const PATH_ELEMENT_ESCAPE: &AsciiSet = &CONTROLS.add(b'/').add(b'?').add(b'"').add(b'`');


use dystonse_curves::{IrregularDynamicCurve, Curve, TypedCurve};
use std::io::Write;
use colorous::*;

use journey_data::*;
use time_curve::TimeCurve;

const CSS:&'static str = include_str!("style.css");
const FAVICON_HEADERS: &'static str = r##"
<link rel="apple-touch-icon" sizes="180x180" href="/favicons/apple-touch-icon.png">
<link rel="icon" type="image/png" sizes="32x32" href="/favicons/favicon-32x32.png">
<link rel="icon" type="image/png" sizes="16x16" href="/favicons/favicon-16x16.png">
<link rel="manifest" href="/favicons/site.webmanifest">
<link rel="mask-icon" href="/favicons/safari-pinned-tab.svg" color="#5bbad5">
<meta name="msapplication-TileColor" content="#00aba9">
<meta name="theme-color" content="#ffffff">
"##;

#[derive(Clone)]
pub struct Monitor {
    pub schedule: Arc<Gtfs>,
    pub pool: Arc<Pool>,
    pub source: String,
    pub stats: Arc<DelayStatistics>
}

impl Monitor {
    pub fn get_subcommand() -> App<'static>{
        App::new("monitor").about("Starts a web server that serves the monitor website.")
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(main: &Main, _sub_args: &ArgMatches) -> FnResult<()> {
        let stats = FileCache::get_cached_simple(&main.statistics_cache, &format!("{}/all_curves.exp", main.dir)).or_error("No delay statistics (all_curves.exp) found.")?;

        let monitor = Monitor {
            schedule: main.get_schedule()?.clone(),
            pool: main.pool.clone(),
            source: main.source.clone(),
            stats,
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

    let path_parts : Vec<String> = req.uri().path().split('/').map(|part| percent_decode_str(part).decode_utf8_lossy().into_owned()).filter(|p| !p.is_empty()).collect();
    let path_parts_str : Vec<&str> = path_parts.iter().map(|string| string.as_str()).collect();
    println!("path_parts_str: {:?}", path_parts_str);
    let res = match &path_parts_str[..] {
        [] => generate_search_page(&mut response, &monitor, false),
        ["grad.png"] | ["fonts", _] => generate_error_page(&mut response, StatusCode::NOT_FOUND, "Static resources not suppported."),
        ["embed"] => generate_search_page(&mut response, &monitor, true),
        ["stop-by-name"] => {
            // an "stop-by-name" URL just redirects to the corresponding "stop" URL. We can't have pretty URLs in the first place because of the way HTML forms work
            let query_params = url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes());
            let stop_name = query_params.filter_map(|(key, value)| if key == "start" { Some(value)} else { None } ).next().unwrap();
            let start_time = Local::now().format("%d.%m.%y %H:%M");
            let new_path = format!("/{}/{}/", 
                start_time, 
                utf8_percent_encode(&stop_name, PATH_ELEMENT_ESCAPE).to_string(),
            );
            response.headers_mut().append(hyper::header::LOCATION, HeaderValue::from_str(&new_path).unwrap());
            *response.status_mut() = StatusCode::FOUND;
            Ok(())
        },
        ["info", ..] => {
            let journey = JourneyData::new(&path_parts[1..], monitor.clone()).unwrap();

            generate_info_page(
                &mut response, 
                &monitor, 
                &journey
            )
        },
        _ => {
            if path_parts[0].starts_with("favicon") {
                generate_error_page(&mut response, StatusCode::NOT_FOUND, "Static resources not suppported.").unwrap();
                return Ok(response);
            }

            // TODO use https://crates.io/crates/chrono_locale for German day and month names
            handle_route_with_stop(&mut response, &monitor,  &path_parts)
        },
    };

    if let Err(e) = res {
        eprintln!("Fehler: {}", e);
        generate_error_page(&mut response, StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).unwrap();  
    }

    Ok(response)
}

fn generate_search_page(response: &mut Response<Body>, monitor: &Arc<Monitor>, embed: bool) -> FnResult<()> {
    println!("{} Haltestellen gefunden.", monitor.schedule.stops.len());
    // TODO: handle the different GTFS_SOURCE_IDs in some way
    // TODO: compress output, of this page specifically. Adding compression to hyper is
    // explained / shown in the middle of this blog post: https://dev.to/deciduously/hyper-webapp-template-4lj7

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>
{css}
        </style>

{favicon_headers}
        <meta name=viewport content="width=device-width, initial-scale=1">
    </head>"#,
        css = CSS,
        favicon_headers = FAVICON_HEADERS,
    )?;
    
    if embed {
        write!(&mut w, r#"
    <body class="embed">"#)?;
    }

    if !embed {
        write!(&mut w, r#"
    <body>
        <h1>Reiseplaner</h1>
        <p class="official">
            Herzlich willkommen. Hier kannst du deine Reiseroute mit dem ÖPNV im VBN (Verkehrsverbund Bremen/Niedersachsen) planen.
        </p>"#)?;
    }

    write!(&mut w, r#"
        <form method="get" action="/stop-by-name" target="{target}">
            <div class="search">
                <label for="start">Start-Haltestelle:</label>
                <input list="stop_list" id="start" name="start" value="{initial_value}" />
                <datalist id="stop_list">"#,
        target = if embed { "_blank" } else { "_self" },
        initial_value = if embed { "Bremen Hauptbahnhof" } else { "" },
    )?;
    for name in monitor.schedule.stops.iter().map(|(_, stop)| stop.name.clone()).sorted().unique() {
        write!(&mut w, r#"
                    <option>{name}</option>"#,
        name=name)?;
    }

    if embed {
        write!(&mut w, r#"
        </datalist>
        <input class="btn project-btn" type="submit" value="Abfahrten anzeigen"/>
        </div>
        </form>
        </body>
        </html>"#
        )?;
    } else {
        write!(&mut w, r#"
        </datalist>
        <input id="button" type="submit" value="Abfahrten anzeigen"/>
        </div>
        </form>
        </body>
        </html>"#
        )?;
    }

    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

fn handle_route_with_stop(response: &mut Response<Body>, monitor: &Arc<Monitor>, journey: &[String]) -> FnResult<()> {
    let journey = JourneyData::new(&journey, monitor.clone())?;

    // println!("Parsed journey: time: {}\n\nstops: {:?}\n\ntrips: {:?}", journey.start_date_time, journey.stops, journey.trips);
    
    let res = match journey.get_last_component() {
        Some(JourneyComponent::Stop(stop_data)) => generate_stop_page(response, monitor, &journey, &stop_data),
        Some(JourneyComponent::Trip(trip_data)) => generate_trip_page(response, monitor, &journey, &trip_data),
        Some(JourneyComponent::Walk(_)) => generate_error_page(response, StatusCode::BAD_REQUEST, &format!("Journey may not end with a walk.")),
        None => generate_error_page(response, StatusCode::BAD_REQUEST, &format!("Empty journey.")),
    };

    if let Err(e) = res {
        eprintln!("Fehler: {}", e);
        generate_error_page(response, StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).unwrap();
    }
    
    Ok(())
}

fn generate_error_page(response: &mut Response<Body>, code: StatusCode, message: &str) -> FnResult<()> {
    let doc_string = format!("{}: {}", code.as_str(), message);
    *response.body_mut() = Body::from(doc_string);
    *response.status_mut() = code;
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    Ok(())
}

fn generate_stop_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, journey_data: &JourneyData, stop_data: &StopData) -> FnResult<()> {
    let mut departures : Vec<DbPrediction> = Vec::new();
    let exact_min_time = stop_data.start_curve.typed_x_at_y(0.01);
    let exact_max_time = stop_data.start_curve.typed_x_at_y(0.99);
    let min_time = exact_min_time - Duration::minutes(exact_min_time.time().minute() as i64 % 5); // round to previous nice time
    let exact_len_time: i64 = exact_max_time.signed_duration_since(exact_min_time).num_minutes() + 30;
    let len_time: i64 = exact_len_time - (exact_len_time % 5);
    let max_time = min_time + Duration::minutes(len_time);

    let mut trip_arrival_option : Option<DbPrediction> = None;

    //first line: arrival at this stop
    if let Some(arrival_trip) = stop_data.get_previous_trip_data() {
        //let arrival_stop_id = arrival_trip.get_trip(&monitor.schedule)?.stop_times[stop_data.arrival_trip_stop_index.unwrap()].stop.id.clone();
        let arrival_stop_sequence = arrival_trip.get_trip(&monitor.schedule)?.stop_times[stop_data.arrival_trip_stop_index.unwrap()].stop_sequence;

        if let Ok(arrival) = get_prediction_for_first_line(monitor.clone(), arrival_stop_sequence, &arrival_trip.vehicle_id, EventType::Arrival) {
            trip_arrival_option = Some(arrival);
        }
    }
    
    for stop_id in &stop_data.extended_stop_ids {
        departures.extend(get_predictions_for_stop(monitor, monitor.source.clone(), EventType::Departure, stop_id, min_time, max_time)?);
    }

    println!("Found {} departure predictions.", departures.len());

    for dep in &mut departures {
        if let Err(e) = dep.compute_meta_data(monitor){
            eprintln!("Could not compute metadata for departure with trip_id {}: {}", dep.trip_id , e);
        }
    }

    // Remove the top and bottom 5% of the predicted time span. 
    // They mostly contain outliers with several hours of (sometimes negative) delay.
    departures.retain(|dep| {
        if dep.meta_data.is_some() {
            let time_absolute_05 = dep.get_absolute_time_for_probability(0.05).unwrap();
            let time_absolute_95 = dep.get_absolute_time_for_probability(0.95).unwrap();
            
            time_absolute_05 < max_time && time_absolute_95 > min_time
        } else {
            false
        }
    });

    println!("Kept {} departure predictions based on removing the top and bottom 5%.", departures.len());
 

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

    // remove departures where the current stop is the last one (which seem to happen for trains quite often):
    
    // local function for use in predicate below
    fn is_at_last_stop(dep: &DbPrediction, monitor: &Arc<Monitor>) -> bool {
        if let Ok(trip) = &monitor.schedule.get_trip(&dep.trip_id) {
            if let Some(stop_time) = &trip.stop_times.last() {
                let last_stop_id = &stop_time.stop.id;
                return dep.stop_id == *last_stop_id && dep.stop_sequence == stop_time.stop_sequence as usize;
            }
        }
        false
    }
    
    departures.retain(|dep| !is_at_last_stop(&dep, &monitor.clone()));

    println!("Kept {} departure predictions after removing trips that are at their last stop.", departures.len());

    // sort by median departure time:
    departures.sort_by_cached_key(|dep| dep.get_absolute_time_for_probability(0.50).unwrap());

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>
        
        {favicon_headers}

        <meta name=viewport content="width=device-width, initial-scale=1">
    </head>
    <body>"#,         
    css = CSS,
    favicon_headers = FAVICON_HEADERS,)?;

    generate_breadcrumbs(&mut w, journey_data)?;

    write!(&mut w, r#"
        <h1>Abfahrten für {stop_name} <span class="extended_stops" title="{stop_names}">(und {stops_number} weitere)</span>, {date} von {min_time} bis {max_time}</h1>
            <div class="header">
            <div class="timing">
                <div class="head time">Plan</div>
                <div class="head min" title="Früheste Abfahrt, die in 99% der Fälle nicht unterschritten wird">-</div>
                <div class="head med" title="Mittlere Abfahrt">○</div>
                <div class="head max" title="Späteste Abfahrt, die in 99% der Fälle nicht überschritten wird">+</div>
            </div>
            <div class="head type">Typ</div>
            <div class="head route">Linie</div>
            <div class="head headsign">Ziel</div>
            <div class="head prob">Chance</div>
            <div class="head source">Daten</div>
        </div>
        <div class="timeline">"#,
        stop_name = stop_data.stop_name,
        stop_names = stop_data.extended_stop_names.join(",\n"),
        stops_number = stop_data.extended_stop_names.len() - 1,
        date = min_time.format("%A, %e. %B"),
        min_time = min_time.format("%H:%M"),
        max_time = max_time.format("%H:%M")
    )?;

    //optional first line for arrival by walk:
    if let Some(JourneyComponent::Walk(walk_data)) = &stop_data.prev_component {
        write_walk_arrival_output(&mut w, walk_data, stop_data, monitor, min_time, max_time)?;
    }

    //optional first line for arrival by trip:
    if let Some(mut arrival) = trip_arrival_option {
        arrival.compute_meta_data(monitor)?;
        write_departure_output(&mut w, &arrival, &journey_data, &stop_data, &monitor.clone(), min_time, max_time, EventType::Arrival)?;
    }

    for dep in departures {
        write_departure_output(&mut w, &dep, &journey_data, &stop_data, &monitor.clone(), min_time, max_time, EventType::Departure)?;
    }
    generate_timeline(&mut w, min_time, len_time)?;
    write!(&mut w, r#"
</body>
</html>"#,
    )?;
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

fn generate_timeline(mut w: &mut Vec<u8>, min_time: DateTime<Local>, len_time: i64) -> FnResult<()> {
    for m in (0..(len_time + 1)).step_by(1) {
        if m % 5 == 0 {
            writeln!(&mut w, r#"    <div class="timebar" style="left: {percent:.1}%;"></div>"#,
                //time = (min_time + Duration::minutes(m)).format("%H:%M"),
                percent = m as f32 / (len_time as f32) * 100.0,
            )?;
        } else if len_time < 90 {
            writeln!(&mut w, r#"    <div class="small_timebar" style="left: {percent:.1}%;"></div>"#,
                percent = m as f32 / (len_time as f32) * 100.0,
            )?;
        }
    }
    generate_timeline_labels(w, min_time, len_time)?;
    write!(&mut w, r#"</div>"#)?;
    Ok(())
}

fn generate_timeline_labels(mut w: &mut Vec<u8>, min_time: DateTime<Local>, len_time: i64) -> FnResult<()> {
    writeln!(&mut w, r#"<div class="timelabels_footer"><div class="timelabels">"#)?;
    for m in (0..(len_time + 1)).step_by(1) {
        if m % 5 == 0 {
            writeln!(&mut w, r#"    <div class="timelabel" style="left: {percent:.1}%;"><span>{time}</span></div>"#,
                time = (min_time + Duration::minutes(m)).format("%H:%M"),
                percent = m as f32 / (len_time as f32) * 100.0,
            )?;
        }
    }
    write!(&mut w, r#"</div></div>"#)?;
    Ok(())
}

fn generate_breadcrumbs(mut w: &mut Vec<u8>, journey_data: &JourneyData) -> FnResult<()> {

    //write link to search page:
    write!(&mut w, r#"<div class="breadcrumbs"><a href="/" title="Startseite">&#128269;</a>"#)?;

    let mut journey_iter = journey_data.components.iter();
    let mut stop_text: String; 

    //first stop has to be set in any case:
    if let JourneyComponent::Stop(stop_data) = journey_iter.next().unwrap() {
        stop_text = stop_data.stop_name.clone();
    } else {
        bail!("No stop found, but a journey always has to begin at a stop.");
    }
    let mut trip_text : String;
    let mut walked : bool;

    loop{
        if let Some(component) = journey_iter.next() {
            match component {
                JourneyComponent::Trip(trip_data) => {
                    trip_text = trip_data.route_name.clone();
                    walked = false;
                    //write link for previous stop:
                    write!(&mut w, r#" > <a href="{}">{}</a>"#, trip_data.prev_component.get_url(), stop_text)?;
                },
                JourneyComponent::Walk(walk_data) => {
                    trip_text = String::from(""); // dummy, never used
                    walked = true;
                    //write link for previous stop:
                    write!(&mut w, r#" > <a href="{}">{}</a>"#, walk_data.prev_component.get_url(), stop_text)?;
                },
                JourneyComponent::Stop(stop_data) => { // there should not be a stop here!
                    bail!("Expected trip or walk, found stop: {}", stop_data.stop_name);
                }
            } 
        }else { // previus stop was the last stop
            //write non-link for last stop:
            write!(&mut w, r#" > <span>{}<span>"#, stop_text)?;
            break;
        }
        if let Some(JourneyComponent::Stop(stop_data)) = journey_iter.next() {
            stop_text = stop_data.stop_name.clone();
            if walked {
                //write non-link for previous walk:
                write!(&mut w, r#" > <span>Fußweg<span>"#)?;
            } else {
                //write link for previous trip:
                write!(&mut w, r#" > <a href="{}">{}</a>"#, stop_data.prev_component.as_ref().unwrap().get_url(), trip_text)?;
            }
        } else if !walked {
            //write non-link for last trip:
            write!(&mut w, r#" > <span>{}<span>"#, trip_text)?;
            break;
        }
    }

    // close the wrapping div:
    write!(&mut w, r#"</div>"#)?;
    Ok(())
}

fn generate_trip_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, journey_data: &JourneyData, trip_data: &TripData) -> FnResult<()> {
    let trip = monitor.schedule.get_trip(&trip_data.vehicle_id.trip_id)?;
    let route = monitor.schedule.get_route(&trip.route_id)?;
    
    let start_sequence = trip.stop_times[trip_data.boarding_stop_index.unwrap()].stop_sequence;
    //let start_id = &trip.stop_times[trip_data.start_index.unwrap()].stop.id;

    // departure from first stop: this is where the user changes into this trip
    let mut departure = get_prediction_for_first_line(monitor.clone(), start_sequence, &trip_data.vehicle_id, EventType::Departure)?;

    let mut arrivals = get_predictions_for_trip(
        monitor,
        monitor.source.clone(), 
        EventType::Arrival,
        &trip_data.vehicle_id,
        start_sequence + 1)?;

    if arrivals.is_empty() {
        generate_error_page(response, StatusCode::INTERNAL_SERVER_ERROR, "No predictions for this trip").unwrap();
        return Ok(());
    }

    for arr in &mut arrivals {
        if let Err(e) = arr.compute_meta_data(monitor){
            eprintln!("Could not compute metadata for arrival with trip_id {}: {}", arr.trip_id , e);
        }
    }

    departure.compute_meta_data(monitor)?;
    let exact_min_time = departure.get_absolute_time_for_probability(0.01).unwrap();

    let exact_max_time = if let Some(time) = arrivals.iter().filter_map(|arr| arr.get_absolute_time_for_probability(0.99).ok()).max() {
        time
    } else {
        arrivals.iter().map(|arr| arr.meta_data.as_ref().expect("No metadata").scheduled_time_absolute).max().or_error("No maximum")?
    };

    let min_time = exact_min_time - Duration::minutes(exact_min_time.time().minute() as i64 % 5); // round to previous nice time
    let len_time: i64 = ((exact_max_time.signed_duration_since(min_time).num_minutes() as i64 + 6) / 5) * 5;
    let max_time = min_time + Duration::minutes(len_time);
    

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>

        {favicon_headers}

        <meta name=viewport content="width=device-width, initial-scale=1">
    </head>
    <body>"#,
    css = CSS,
    favicon_headers = FAVICON_HEADERS
    )?;

    generate_breadcrumbs(&mut w, journey_data)?;
    
    write!(&mut w, r#"
        <h1>Halte für {route_type} Linie {route_name} nach {headsign}</h1>
            <div class="header">
            <div class="timing">
                <div class="head time">Plan</div>
                <div class="head min" title="Früheste Ankunft, die in 99% der Fälle nicht unterschritten wird">-</div>
                <div class="head med">○</div>
                <div class="head max">+</div>
            </div>
            <div class="head stopname">Haltestelle</div>
            <!-- div class="head prob">Chance</div-->
            <div class="head source">Daten</div>
        </div>
        <div class="timeline">"#,
        route_type = route_type_to_str(route.route_type),
        route_name = route.short_name,
        headsign = trip.trip_headsign.as_ref().unwrap(),
    )?;
    for stop_time in &trip.stop_times {
        // don't display stops that are before the stop where we change into this trip
        if trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence)? == trip_data.boarding_stop_index.unwrap() {
            write_stop_time_output(&mut w, &stop_time, Some(&departure), min_time, max_time, EventType::Departure, Some(trip_data.start_prob))?;

        } else if trip.get_stop_index_by_stop_sequence(stop_time.stop_sequence)? > trip_data.boarding_stop_index.unwrap() {
            //arrivals at later stops:
            let arrival = arrivals.iter().filter(|a| a.stop_sequence == stop_time.stop_sequence as usize).next();
            write_stop_time_output(&mut w, &stop_time, arrival, min_time, max_time, EventType::Arrival, None)?;
        }
        
    }

    generate_timeline(&mut w, min_time, len_time)?;

    write!(&mut w, r#"
</body>
</html>"#,
    )?;
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

fn write_walk_arrival_output(
    mut w: &mut Vec<u8>, 
    walk_data: &WalkData,
    stop_data: &StopData,
    _monitor: &Arc<Monitor>,
    min_time: DateTime<Local>,
    max_time: DateTime<Local>,
) -> FnResult<()> {

    let a_01 = stop_data.start_curve.typed_x_at_y(0.01);
    let a_50 = stop_data.start_curve.typed_x_at_y(0.50);
    let a_99 = stop_data.start_curve.typed_x_at_y(0.99);
    let stop_name = &stop_data.stop_name;
    let distance = if let JourneyComponent::Stop(prev_stop) = &walk_data.prev_component {
        prev_stop.get_max_distance(&stop_data)
    } else {
        bail!("Walk has no prev_stop");
    };
    
    let image_url = generate_png_data_url(&stop_data.start_curve, min_time, max_time, 120, EventType::Arrival)?;
    let prob = stop_data.start_prob * 100.0;

    write!(&mut w, r#"
        <div class="outer">    
            <div class="line">
                <div class="timing">
                    <div class="area time" title="Mittlere Ankunftszeit: {time}">{time}</div>
                    <div class="area min" title="Frühestmögliche Ankunft">{min}</div>
                    <div class="area med" title="Mittlere Ankunft">{med}</div>
                    <div class="area max" title="Spätestmögliche Ankunft">{max}</div>
                </div>
                <!--div class="area type"><span class="bubble w">Fuß</span></div-->
                <div class="area distance">{distance:.0} m Fußweg</div>
                <div class="area headsign">Ankunft an {stop_name}</div>
                <div class="area prob {probclass}">{prob:.0} %</div>
                <div class="area source"></div>
            </div>
            <div class="visu" style="background-image:url('{image_url}')"></div>
        </div>"#,
        time = a_50.format("%H:%M"),
        min = format_delay((a_01 - a_50).num_minutes() as i32),
        med = format_delay((a_50 - a_50).num_minutes() as i32),
        max = format_delay((a_99 - a_50).num_minutes() as i32),
        distance = distance,
        stop_name = stop_name,
        image_url = image_url,
        probclass = if prob == 100.0 { "hundred" } else { "" },
        prob = prob,
    )?;
    Ok(())
}

fn write_departure_output(
    mut w: &mut Vec<u8>, 
    dep: &DbPrediction, 
    _journey_data: &JourneyData,
    stop_data: &StopData,
    monitor: &Arc<Monitor>,
    min_time: DateTime<Local>,
    max_time: DateTime<Local>,
    event_type: EventType,
) -> FnResult<()> {
    let md = dep.meta_data.as_ref().unwrap();
    let a_scheduled = dep.meta_data.as_ref().unwrap().scheduled_time_absolute;
    let scheduled_percent = a_scheduled.signed_duration_since(min_time).num_seconds() as f32 / (max_time.signed_duration_since(min_time).num_seconds() as f32) * 100.0;
    let a_01 = dep.get_absolute_time_for_probability(0.01).unwrap();
    let a_50 = dep.get_absolute_time_for_probability(0.50).unwrap();
    let a_99 = dep.get_absolute_time_for_probability(0.99).unwrap();
    let r_01 = dep.get_relative_time_for_probability(0.01) / 60;
    let r_50 = dep.get_relative_time_for_probability(0.50) / 60;
    let r_99 = dep.get_relative_time_for_probability(0.99) / 60;

    // prepare walk time. Even for a distance of 0 there is some walk time involved.
    let walk_distance = *stop_data.extended_stops_distances.get(&dep.stop_id).unwrap_or(&0.0);
    let walk_time = get_walk_time(walk_distance);

    // compute local probability of getting the transfer (not accumulated for the whole journey, just for here)
    let local_prob = match event_type {
        EventType::Arrival => 100.0, // arrival is always 100%
        EventType::Departure => stop_data.start_curve
            .add_duration_curve(&walk_time)
            .get_transfer_probability(&dep.get_time_curve()) * 100.0
    };

    // don't display anything below 5% local chance:
    if local_prob < 5.0 {
        println!("write departure output for stop page: Skipping departure with less than 5% chance.");
        return Ok(());
    }

    // compute actual probability of getting the transfer (for later use in the output)
    let prob = stop_data.start_prob * local_prob;

    //let trip_link =  format!("{}/", dep.trip_id);
    let _trip_start_date_time = dep.trip_start_date.and_hms(0, 0, 0) + dep.trip_start_time;

    // let source_link = format!("/info/{}/{}/{}/{}", dep.route_id, dep.trip_id, dep.trip_start_date, dep.trip_start_time.num_seconds());

    let (type_letter, type_class) = match md.route_type {
        RouteType::Bus     => ("Bus", "b"),
        RouteType::Rail    => {
            // RB RE S RS IC DPN MEX
            if md.route_name.starts_with("RB") {
                ("RB"  , "r")
            } else if md.route_name.starts_with("RE") {
                ("RE"  , "r")
            } else if md.route_name.starts_with("S") {
                ("S"  , "s")
            } else if md.route_name.starts_with("RS") {
                ("RS"  , "s")
            } else if md.route_name.starts_with("IC") {
                ("IC"  , "r")
            } else {
                ("Bahn"  , "z")
            }
        },
        RouteType::Subway  => ("U"  , "u"),
        RouteType::Tramway => ("Tram"  , "m"),
        RouteType::Ferry   => ("F"  , "f"),
        _                  => ("?"  , "d"),
    };

    let mut stop_url = stop_data.url.clone();

    // prepare info for departure from extended stops list
    let mut extended_stop_info : String = String::from("");
    if let Some(d) = stop_data.extended_stops_distances.get(&dep.stop_id) {
        let alternative_stop_name = monitor.schedule.get_stop(&dep.stop_id)?.name.clone();
        extended_stop_info = format!(
            r#"<div class="area walk" title="{min_walk_time} bis {max_walk_time} Fußweg bis {alternative_stop_name}"><span>{d:.0} m</span></div>"#,
            alternative_stop_name = alternative_stop_name,
            d = d, 
            min_walk_time = format_duration(Duration::seconds(walk_time.min_x() as i64)),
            max_walk_time = format_duration(Duration::seconds(walk_time.max_x() as i64))
        );
        stop_url = format!(
            "{original_url}Fußweg/{alternative_stop_name}/", 
            original_url = stop_data.url,
            alternative_stop_name = utf8_percent_encode(&alternative_stop_name, PATH_ELEMENT_ESCAPE).to_string(),
        );
    }
    
    // trip link
    let trip_link = match event_type {
        EventType::Arrival => String::from("<div"),
        EventType::Departure => format!(r#"<a href="{stop_url}{r_type} {route} nach {headsign} um {time}/""#, 
            stop_url = stop_url,
            r_type = route_type_to_str(md.route_type), 
            route = md.route_name, 
            headsign = utf8_percent_encode(&md.headsign, PATH_ELEMENT_ESCAPE).to_string(),
            time = md.scheduled_time_absolute.format("%H:%M")
        )
    };
    let trip_link_type = match event_type {
        EventType::Arrival => "div",
        EventType::Departure => "a"
    };


    let image_url = generate_png_data_url(&dep.get_time_curve(), min_time, max_time, 120, event_type)?;

    let headsign = match event_type {
        EventType::Arrival => format!("Ankunft an {}", stop_data.stop_name),
        EventType::Departure => md.headsign.clone()
    };

    write!(&mut w, r#"
        {trip_link} class="outer">    
            <div class="line">
                <div class="timing">
                    <div class="area time">{time}</div>
                    <div class="area min" title="Frühestens {min_tooltip}">{min}</div>
                    <div class="area med" title="Vermutlich {med_tooltip}">{med}</div>
                    <div class="area max" title="Spätstens {max_tooltip}">{max}</div>
                </div>
                <div class="area type"><span class="bubble {type_class}">{type_letter}</span></div>
                <div class="area route">{route_name}</div>
                <div class="area headsign">{headsign}</div>
                {extended_stop_info}
                <div class="area prob {probclass}">{prob:.0} %</div>
                {source_area}
            </div>
            <div class="visu" style="background-image:url('{image_url}')"></div>
            <div class="schedulepoint" style="left:{scheduled_percent:.2}%;">▲</div>
        </{trip_link_type}>"#,
        trip_link = trip_link,
        trip_link_type = trip_link_type,
        time = md.scheduled_time_absolute.format("%H:%M"),
        min = format_delay(r_01),
        min_tooltip = a_01.format("%H:%M:%S"),
        med = format_delay(r_50),
        med_tooltip = a_50.format("%H:%M:%S"),
        max = format_delay(r_99),
        max_tooltip = a_99.format("%H:%M:%S"),
        type_letter = type_letter,
        type_class = type_class,
        route_name = md.route_name,
        headsign = headsign,
        extended_stop_info = extended_stop_info,
        image_url = image_url,
        prob = prob,
        source_area = get_source_area(Some(dep)),
        probclass = if prob == 100.0 { "hundred" } else { "" },
        scheduled_percent = scheduled_percent
    )?;
    Ok(())
}

fn get_source_area(db_prediction: Option<&DbPrediction>) -> String {
    if let Some(db_prediction) = db_prediction {
        let (origin_letter, origin_description) = match (&db_prediction.origin_type, &db_prediction.precision_type) {
            (OriginType::Realtime, PrecisionType::Specific) => ("E","Aktuelle Echtzeitdaten"),
            (OriginType::Realtime, PrecisionType::FallbackSpecific) => ("E","Aktuelle Echtzeitdaten"),
            (OriginType::Realtime, _) => ("U","Ungenutzte Echtzeitdaten"),
            (OriginType::Schedule, _) => ("P","Fahrplandaten"),
            (OriginType::Unknown, _)  => ("?","Unbekannte Datenquelle")
        };

        let (precision_letter, precision_description) = match db_prediction.precision_type {
            PrecisionType::Specific           => ("S+", "Spezifische Prognose für diese Linie, Haltestelle und Tageszeit"),
            PrecisionType::FallbackSpecific   => ("S" , "Spezifische Prognose für diese Linie und Haltestelle"),
            PrecisionType::SemiSpecific       => ("S-", "Spezifische Prognose für diese Linie und Haltestelle, jedoch ohne Echtzeitdaten zu nutzen"),
            PrecisionType::General            => ("G+", "Generelle Prognose für Fahrzeugart, Tageszeit und Routenabschnitt"),
            PrecisionType::FallbackGeneral    => ("G" , "Generelle Prognose für Fahrzeugart"),
            PrecisionType::SuperGeneral       => ("G-", "Standardprognose, sehr ungenau"),
            PrecisionType::Unknown            => ("?" , "Unbekanntes Prognoseverfahren"),
        };

        let source_class = match (origin_letter, precision_letter) {
            ("E","S+") => "a",
            ("E","S") => "a",
            (_,"S-") => "b",
            (_,"G+") => "c",
            (_,"G") => "d",
            (_,"G-") => "d",
            (_,_) => "e",
        };

        return format!(
            r#"<div class="area source" title="{source_long}"><span class="bubble {source_class}">{source_short}</span></div>"#,
            source_long = format!("{} und {}, basierend auf {} vorherigen Aufnahmen.", origin_description, precision_description, db_prediction.sample_size),
            source_short = format!("{}/{}", origin_letter, precision_letter),
            source_class = source_class,
        );
    } else {
        return format!(
            r#"<div class="area source" title="{source_long}"><span class="bubble {source_class}">{source_short}</span></div>"#,
            source_long = "Keine Prognose verfügbar",
            source_short = "-",
            source_class = "e",
        );
    }
}

fn write_stop_time_output(
    mut w: &mut Vec<u8>, 
    stop_time: &StopTime, 
    prediction: Option<&DbPrediction>, 
    min_time: DateTime<Local>, 
    max_time: DateTime<Local>, 
    event_type: EventType,
    prob: Option<f32>
) -> FnResult<()> {
    
    let stop_link = match event_type {
        EventType::Arrival => format!(r#"<a href="{}/""#, stop_time.stop.name),
        EventType::Departure => String::from("<div") //no link for first line
    };
    let stop_link_type = match event_type {
        EventType::Arrival => "a",
        EventType::Departure => "div"
    };

    let scheduled_time = match event_type {
        EventType::Arrival   => date_and_time_local(&prediction.unwrap().trip_start_date, stop_time.arrival_time  .unwrap() as i32),
        EventType::Departure => date_and_time_local(&prediction.unwrap().trip_start_date, stop_time.departure_time.unwrap() as i32)
    };

    let (r_01, r_50,r_99) = if let Some(prediction) = prediction {
        (
            prediction.get_relative_time_for_probability(0.01),
            prediction.get_relative_time_for_probability(0.50),
            prediction.get_relative_time_for_probability(0.99),
        )
    } else {
        (0,0,0)
    };
    let a_01 = scheduled_time + Duration::seconds(r_01 as i64);
    let a_50 = scheduled_time + Duration::seconds(r_50 as i64);
    let a_99 = scheduled_time + Duration::seconds(r_99 as i64);

    let scheduled_percent = scheduled_time.signed_duration_since(min_time).num_seconds() as f32 / (max_time.signed_duration_since(min_time).num_seconds() as f32) * 100.0;

    let image_url = if let Some(prediction) = prediction {
        generate_png_data_url(&prediction.get_time_curve(), min_time, max_time, 120, event_type)?
    } else {
        String::new()
    };

    let prob_area = if let Some(actual_prob) = prob {
        format!(
            r#"<div class="area prob {probclass}">{prob:.0} %</div>"#, 
            probclass = if actual_prob == 1.0 { "hundred" } else { "" },
            prob = actual_prob * 100.0)
    } else {
        String::new()
    };

    write!(&mut w, r#"
        {stop_link} class="outer">
            <div class="line">
                <div class="timing">
                    <div class="area time">{time}</div>
                    <div class="area min" title="Frühestens {min_tooltip}">{min}</div>
                    <div class="area med" title="Vermutlich {med_tooltip}">{med}</div>
                    <div class="area max" title="Spätstens {max_tooltip}">{max}</div>
                </div>
                <div class="area stopname">{stopname}</div>
                {prob_area}
                {source_area}
            </div>
            <div class="visu" style="background-image:url('{image_url}')"></div>
            <div class="schedulepoint" style="left:{scheduled_percent:.2}%;">▲</div>
        </{stop_link_type}>"#,
        stop_link = stop_link,
        stop_link_type = stop_link_type,
        time = scheduled_time.format("%H:%M"),
        min = format_delay(r_01 as i32 / 60),
        min_tooltip = a_01.format("%H:%M:%S"),
        med = format_delay(r_50 as i32 / 60),
        med_tooltip = a_50.format("%H:%M:%S"),
        max = format_delay(r_99 as i32 / 60),
        max_tooltip = a_99.format("%H:%M:%S"),
        stopname = stop_time.stop.name,
        source_area = get_source_area(prediction),
        prob_area = prob_area,
        image_url = image_url,
        scheduled_percent = scheduled_percent,

    )?;
    Ok(())
}

fn format_delay(delay: i32) -> String {
    if delay > 0 {
        format!("+{}", delay)
    } else  {
        format!("{}", delay)
    }
}


fn format_duration(duration: Duration) -> String {
    if duration < Duration::seconds(60) {
        format!("{:.0} Sek.", duration.num_seconds())
    } else  {
        let seconds = duration.num_seconds() as i32;
        format!("{:.0}:{:02.0} Min.", seconds / 60, seconds % 60)
    }
}

#[allow(dead_code)]
pub fn get_transfer_probability(
    arrival_time: DateTime<Local>, 
    arrival_dist: &IrregularDynamicCurve<f32, f32>, 
    departure_time: DateTime<Local>, 
    departure_dist: &IrregularDynamicCurve<f32, f32>
) -> f32 {
    let mut total_miss_prob = 0.0;
    let step_size = 1;
    for percentile in (0..100).step_by(step_size) {
        // compute the absolute time at which the arrival occurs for this percentile
        let arrival_time_abs = arrival_time + Duration::seconds(arrival_dist.x_at_y(percentile as f32 / 100.0) as i64);
        // convert the arrival time into the reference system of the departure
        let arrival_time_rel = arrival_time_abs.signed_duration_since(departure_time);
        // compute the pobability of missing the transfer for this arrival percentile
        let transfer_missed_prob = departure_dist.y_at_x(arrival_time_rel.num_seconds() as f32);
        total_miss_prob += transfer_missed_prob / (100.0 / step_size as f32);
    }
    println!("Computed prob from {} to {} as {} %", arrival_time, departure_time, 1.0 - total_miss_prob);
    1.0 - total_miss_prob 
}

fn generate_png_data_url(time_curve: &TimeCurve, min_time: DateTime<Local>, max_time: DateTime<Local>, width: usize, event_type: EventType) -> FnResult<String> {

    let gradient = match event_type {
        EventType::Arrival => YELLOW_ORANGE_BROWN,
        EventType::Departure => YELLOW_GREEN_BLUE
    };

    let mut buf : Vec<u8> = Vec::new();
    // block for scoped borrow of buf
    {
        let mut encoder = png::Encoder::new(&mut buf, width as u32, 1);
        encoder.set_color(png::ColorType::RGBA);
        encoder.set_depth(png::BitDepth::Eight);
        let mut png = encoder.write_header()?;

        let mut image_data = Vec::<u8>::with_capacity(width * 4);
        let f = (max_time - min_time) / width as i32;
        
        // cumulated probabilities, in image's reference system:
        let probs_cum : Vec<f32> = (0..(width + 1)).map(|x| time_curve.typed_y_at_x(min_time + f * x as i32)).collect();
        // uncumulated ... 
        let probs_uncum : Vec<f32> = probs_cum.iter().tuple_windows().map(|(a,b)| b-a).collect();
        
        let mut max = *probs_uncum.iter().max_by(|a,b| a.partial_cmp(b).unwrap()).unwrap();
        if max < 0.05 {
            max = 0.05;
        }
        for i in 0..width {
            let prob_uncum = probs_uncum[i] / max;
            let prob_cum = probs_cum[i];
            let crop = 0.2;
            let color = if prob_cum > 0.01 && prob_cum < 0.99 { 
                gradient.eval_continuous((crop + (prob_uncum * (1.0 - crop))) as f64)
            } else if prob_cum > 0.0 && prob_cum < 1.0 {
                gradient.eval_continuous(0.0 as f64)
            } else {
                Color{r: 255, g: 255, b: 255}
            };
            image_data.push(color.r);
            image_data.push(color.g);
            image_data.push(color.b);
            image_data.push(255);
        }
        png.write_image_data(&image_data)?; // Save
    }
    let b64_data = base64::encode_config(buf, base64::STANDARD);
    Ok(format!("data:image/png;base64,{}", b64_data))
}

fn generate_info_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, journey: &JourneyData) -> FnResult<()> {
    println!("generate_info_page");
    let trip_data = match journey.get_last_component().unwrap() {
        JourneyComponent::Trip(trip_data) => trip_data,
        _ => bail!("No trip at journey end"),
    };
    let route = monitor.schedule.get_route(&trip_data.route_id)?;
    let trip: &Trip = trip_data.get_trip(&monitor.schedule)?;
    let route_variant = trip.route_variant.as_ref().unwrap();

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>

        {favicon_headers}

    </head>
    <body>
        <h1>Informationen für Linie {route_name} (route_id {route_id}, route_variant {route_variant}) nach {headsign}</h1>
        <h2>Statistische Analysen</h2>"#,
        css = CSS,
        favicon_headers = FAVICON_HEADERS,
        route_name = route.short_name.clone(),
        route_id = trip_data.route_id,
        route_variant = route_variant,
        headsign = utf8_percent_encode(&trip.trip_headsign.as_ref().or_error("trip_headsign is None")?, PATH_ELEMENT_ESCAPE).to_string(),
    )?;

    match monitor.stats.specific.get(&trip_data.route_id) {
        None => { writeln!(&mut w, "        Keine Linien-spezifischen Statistiken vorhanden.")?; },
        Some(route_data) => {
            match route_data.variants.get(&route_variant.parse()?) {
                None =>  { writeln!(&mut w, "        Keine Statistiken für die Linien-Variante {} vorhanden.</li></ul>", route_variant)?;} ,
                Some(route_variant_data) => {
                    for et in &EventType::TYPES {
                        let curve_set_keys = route_variant_data.curve_sets[**et].keys();
                        let general_keys = route_variant_data.general_delay[**et].keys();
                        writeln!(&mut w, "            <h3>Daten ({:?}) für die Linien-Variante: {} Curve Sets, {} General Curves</h3>", **et, curve_set_keys.len(), general_keys.len())?;
                        for ts in TimeSlot::TIME_SLOTS_WITH_DEFAULT.iter() {
                            

                            if route_variant_data.curve_sets[**et].keys().any(|key| key.time_slot == **ts) {
                                write!(&mut w, r#"
                                <h4>Timeslot: {ts_description}</h4>"#, ts_description = ts.description)?;
                                write!(&mut w, r#"
                                    <table>
                                        <tr>
                                            <td></td>"#)?;

                                for s_i in 0..trip.stop_times.len() {
                                    write!(&mut w, "<td><b>{}</b></td>", s_i)?;
                                }
                                write!(&mut w, "</tr>")?;

                                for s_i in 0..trip.stop_times.len() {
                                    write!(&mut w, "<tr>
                                        <td><b>{}</b></td>", s_i)?;
                                    for e_i in 0..trip.stop_times.len() {
                                        if e_i > s_i {
                                            let _count = match route_variant_data.curve_sets[**et].get(&CurveSetKey{
                                                    start_stop_index: s_i as u32, end_stop_index: e_i as u32, time_slot: (**ts).clone()
                                                }) {
                                                Some(csd) => write!(&mut w, "<td><b>{}</b></td>", csd.sample_size)?,
                                                None => write!(&mut w, r#"<td style="color:#666;">0</td>"#)?
                                            };
                                        } else {
                                            write!(&mut w, "<td></td>")?;
                                        }
                                    }
                                    write!(&mut w, "</tr>")?;
                                }
                                write!(&mut w, "</table>")?;
                            } else {
                                //write!(&mut w, ": nix")?;
                            }
                        }    
                    }
                }
            }
        }
    }

    let stats = get_record_pair_statistics(&monitor.clone(), &monitor.source, &trip_data.route_id, &route_variant)?;

    write!(&mut w, r#"<h2>Echtzeitdaten</h2>
                                    <table>
                                        <tr>
                                            <td></td>"#)?;

    for st_e in &trip.stop_times {
        //überschriften: end-haltestellen (zeile)
        write!(&mut w, "<td><b>{}</b></td>", st_e.stop_sequence)?;

    }
    for st_s in &trip.stop_times {
            //start-haltestellen: je 1 zeile

            //header:
            write!(&mut w, "</tr><tr><td><b>{}</b></td>", st_s.stop_sequence)?;
            for st_e in &trip.stop_times {
                //inhalte: je 1 zelle
                if st_e.stop_sequence > st_s.stop_sequence {
                    match stats.iter().filter(|pair| pair.s == st_s.stop_sequence && pair.e == st_e.stop_sequence).next() {
                        Some(pair) => write!(&mut w, "<td><b>{}</b></td>", pair.c),
                        None => write!(&mut w, r#"<td style="color:#666;">0</td>"#)
                    }?;
                } else {
                    write!(&mut w, "<td></td>")?;
                }
            }
            write!(&mut w, "</tr>")?;
    }

    write!(&mut w, r#"</table>
    </body>
</html>"#
    )?;
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

#[derive(Debug, Clone)]
pub struct DbPrediction {
    pub route_id: String,
    pub trip_id: String,
    pub trip_start_date: Date<Local>,
    pub trip_start_time: Duration, // time from midnight, may be outside 0:00 .. 24:00
    pub prediction_min: DateTime<Local>, 
    pub prediction_max: DateTime<Local>,
    pub precision_type: PrecisionType,
    pub origin_type: OriginType,
    pub sample_size: i32,
    pub prediction_curve: IrregularDynamicCurve<f32, f32>,
    pub stop_id: String,
    pub stop_sequence: usize,

    pub meta_data: Option<DbPredictionMetaData>,
}

#[derive(Debug, Clone)]
pub struct DbPredictionMetaData {
    pub route_name : String,
    pub headsign : String,
    pub stop_index : usize,
    pub scheduled_time_seconds : u32,
    pub scheduled_time_absolute : DateTime<Local>,
    pub route_type: RouteType,
}

impl DbPrediction {
    pub fn compute_meta_data(&mut self, monitor: &Arc<Monitor>) -> FnResult<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let trip = monitor.schedule.get_trip(&self.trip_id)?;
        let route = monitor.schedule.get_route(&self.route_id)?;
        let route_name = route.short_name.clone();
        let route_type = route.route_type;
        let headsign = trip.trip_headsign.as_ref().or_error("trip_headsign is None")?.clone();
        let stop_index = trip.get_stop_index_by_stop_sequence(self.stop_sequence as u16).or_error("stop_index is None")?;
        let scheduled_time_seconds = trip.stop_times[stop_index].departure_time.or_error("departure_time is None")?;
        let scheduled_time_absolute = date_and_time_local(&self.trip_start_date, scheduled_time_seconds as i32);

        self.meta_data = Some(DbPredictionMetaData{ 
            route_name,
            headsign,
            stop_index,
            scheduled_time_seconds,
            scheduled_time_absolute,
            route_type,
        });
        
        Ok(())
    }

    pub fn get_time_curve(&self) -> TimeCurve {
        TimeCurve::new(self.prediction_curve.clone(), self.meta_data.as_ref().unwrap().scheduled_time_absolute)
    }

    pub fn get_absolute_time_for_probability(&self, prob: f32) -> FnResult<DateTime<Local>> {
        let x = self.prediction_curve.x_at_y(prob);
        Ok(date_and_time_local(&self.trip_start_date, self.meta_data.as_ref().or_error("Prediction has no meta_data")?.scheduled_time_seconds as i32 + x as i32))
    }

    pub fn get_relative_time_for_probability(&self, prob: f32) -> i32 {
        self.prediction_curve.x_at_y(prob) as i32
    }

    #[allow(dead_code)]
    pub fn get_relative_time(&self, time: DateTime<Local>) -> FnResult<f32> {
        Ok(-self.meta_data.as_ref().or_error("Prediction has no meta_data")?.scheduled_time_absolute.signed_duration_since(time).num_seconds() as f32)
    }

    #[allow(dead_code)]
    pub fn get_probability_for_relative_time(&self, relative_seconds: f32) -> f32 {
        self.prediction_curve.y_at_x(relative_seconds)
    }
}

impl FromRow for DbPrediction {
    fn from_row_opt(row: Row) -> std::result::Result<Self, FromRowError> {
        use chrono::{NaiveDate, NaiveDateTime};
        use chrono::offset::TimeZone;

        let naive_trip_start_date:NaiveDate    = row.get_opt(2).unwrap().unwrap();
        let naive_prediction_min:NaiveDateTime = row.get_opt(4).unwrap().unwrap();
        let naive_prediction_max:NaiveDateTime = row.get_opt(5).unwrap().unwrap();
         // TODO the .single().unwrap() below will fail when daylight saving changes.
        Ok(DbPrediction{
            route_id:           row.get_opt(0).unwrap().unwrap(),
            trip_id:            row.get_opt(1).unwrap().unwrap(),
            trip_start_date:    Local.from_local_date(&naive_trip_start_date).single().unwrap(),
            trip_start_time:    row.get_opt(3).unwrap().unwrap(),
            prediction_min:     Local.from_local_datetime(&naive_prediction_min).single().unwrap(),
            prediction_max:     Local.from_local_datetime(&naive_prediction_max).single().unwrap(),
            precision_type:     PrecisionType::from_int(row.get_opt(6).unwrap().unwrap()),
            origin_type:        OriginType::from_int(row.get_opt(7).unwrap().unwrap()),
            sample_size:        row.get_opt(8).unwrap().unwrap(),
            prediction_curve:   IrregularDynamicCurve::<f32, f32>
                                    ::deserialize_compact(row.get_opt(9).unwrap().unwrap()),
            stop_id:            row.get_opt(10).unwrap().unwrap(),
            stop_sequence:      row.get_opt(11).unwrap().unwrap(),
            meta_data:          None,
        })
    }
}

struct DbStat {
    s: u16, //start: stop_sequence
    e: u16, //end: stop_sequence
    c: u32 // count: number of matching entries
}

fn get_record_pair_statistics(monitor: &Arc<Monitor>, source: &str, route_id: &str, route_variant: &str) -> FnResult<Vec<DbStat>> {
    let mut conn = monitor.pool.get_conn()?;
    let stmt = conn.prep(
        r"SELECT 
            r1.stop_sequence, r2.stop_sequence, COUNT(*) 
        FROM 
            `records` as r1, `records` as r2
        WHERE 
            r1.source = r2.source AND
            r1.route_id = r2.route_id AND
            r1.trip_id = r2.trip_id AND
            r1.trip_start_date = r2.trip_start_date AND
            r1.trip_start_time = r2.trip_start_time AND
            r1.stop_sequence < r2.stop_sequence AND
            r1.source = :source AND
            r1.route_id = :route_id AND
            r1.route_variant = :route_variant
        GROUP BY 
            r1.stop_sequence, r2.stop_sequence")?;

    let mut result = conn.exec_iter(
        &stmt,
        params! {
            "source" => source,
            "route_id" => route_id,
            "route_variant" => route_variant,
        },
    )?;

    let result_set = result.next_set().unwrap()?;

    let db_counts: Vec<_> = result_set
        .map(|row| {
            let item: (usize, usize, usize) = from_row(row.unwrap());
            DbStat{s: item.0 as u16, e: item.1 as u16, c: item.2 as u32}
        })
        .collect();

    Ok(db_counts)
}

fn get_predictions_for_stop(
    monitor: &Arc<Monitor>,
    source: String, 
    event_type: EventType, 
    stop_id: &str, 
    min_time: DateTime<Local>, 
    max_time: DateTime<Local>
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
            `stop_id`,
            `stop_sequence`
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
            "min_time" => min_time.naive_local(),
            "max_time" => max_time.naive_local(),
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

fn get_predictions_for_trip(
    monitor: &Arc<Monitor>,
    source: String, 
    event_type: EventType, 
    vehicle_id: &VehicleIdentifier,
    start_sequence: u16,
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
            `stop_id`,
            `stop_sequence`
        FROM
            `predictions` 
        WHERE 
            `source`=:source AND 
            `event_type`=:event_type AND
            `trip_id`=:trip_id AND
            `trip_start_date`=:trip_start_date AND 
            `trip_start_time`=:trip_start_time AND
            `stop_sequence`>=:start_sequence;",
    )?;

    let mut result = conn.exec_iter(
        &stmt,
        params! {
            "source" => source,
            "event_type" => event_type.to_int(),
            "trip_id" => vehicle_id.trip_id.clone(),
            "trip_start_date" => vehicle_id.start.date().naive_local(),
            "trip_start_time" => vehicle_id.start.duration(),
            "start_sequence" => start_sequence,
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

pub fn route_type_to_str(route_type: RouteType) -> &'static str {
    match route_type {
        RouteType::Tramway    => "Tram",
        RouteType::Subway     => "U-Bahn",
        RouteType::Rail       => "Zug",
        RouteType::Bus        => "Bus",
        RouteType::Ferry      => "Fähre",
        RouteType::CableCar   => "Kabelbahn",
        RouteType::Gondola    => "Seilbahn",
        RouteType::Funicular  => "Standseilbahn",
        RouteType::Coach      => "Reisebus",
        RouteType::Air        => "Flugzeug",
        RouteType::Taxi       => "Taxi",
        RouteType::Other(_u16) => "Fahrzeug",
    }
}