mod journey_data;

use crate::{FnResult, Main, date_and_time, OrError};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc, Duration, Timelike};
use clap::{App, ArgMatches};
use crate::types::*;
use crate::FileCache;
use std::sync::Arc;
use gtfs_structures::{Gtfs, RouteType, StopTime};
use mysql::*;
use mysql::prelude::*;

use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::header::{HeaderValue};
use hyper::service::{make_service_fn, service_fn};
use itertools::Itertools;

use percent_encoding::percent_decode_str;

use dystonse_curves::{IrregularDynamicCurve, Curve};
use std::str::FromStr;
use std::io::Write;

use journey_data::{JourneyData, JourneyComponent, StopData, TripData};

const CSS:&'static str = include_str!("style.css");

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
    match &path_parts_str[..] {
        [] => generate_search_page(&mut response, &monitor),
        ["stop-by-name"] => {
            // an "stop-by-name" URL just redirects to the corresponding "stop" URL. We can't have pretty URLs in the first place because of the way HTML forms work
            let query_params = url::form_urlencoded::parse(req.uri().query().unwrap().as_bytes());
            let stop_name = query_params.filter_map(|(key, value)| if key == "start" { Some(value)} else { None } ).next().unwrap();
            let start_time = Utc::now().naive_local().format("%d.%m.%y %H:%M");
            let new_path = format!("/{}/{}/", start_time, stop_name);
            response.headers_mut().append(hyper::header::LOCATION, HeaderValue::from_str(&new_path).unwrap());
            *response.status_mut() = StatusCode::FOUND;
        },
        ["info", route_id, trip_id, date_text, time_text] => {
            if let Err(e) = generate_info_page(
                &mut response, 
                &monitor, 
                String::from(*route_id), 
                String::from(*trip_id), 
                NaiveDate::from_str(date_text).unwrap(), 
                NaiveTime::from_num_seconds_from_midnight(time_text.parse().unwrap(), 0)
            ) {
                generate_error_page(&mut response, StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());  
            }
        },
        _ => {
            // TODO use https://crates.io/crates/chrono_locale for German day and month names
            let start_time = NaiveDateTime::parse_from_str(&path_parts[0], "%d.%m.%y %H:%M").unwrap();
            let journey = &path_parts[0..]; // we would need half-open pattern matching to get rid of this line, see https://github.com/rust-lang/rust/issues/67264
            // let points = vec![Tup{x: start_time, y: 0.0}, Tup{x: start_time + 1.0, y: 1.0}];
            // let arrival = IrregularDynamicCurve::new(points);
            if let Err(e) = handle_route_with_stop(&mut response, &monitor, start_time, journey) {
                generate_error_page(&mut response, StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
            }
            //generate_error_page(&mut response, StatusCode::NOT_FOUND, &format!("Keine Seite entsprach dem Muster {:?}.", slice));
        },
    };

    Ok(response)
}

fn generate_search_page(response: &mut Response<Body>, monitor: &Arc<Monitor>) {
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
    </head>
    <body>
        <h1>Reiseplaner</h1>
        <p class="official">
            Herzlich willkommen. Hier kannst du deine Reiseroute mit dem ÖPNV im VBN (Verkehrsverbund Bremen/Niedersachsen) planen.
        </p>
        <form method="get" action="/stop-by-name">
            <p class="dropdown" >
                <label for="start">Start-Haltestelle:</label>
                <input list="stop_list" id="start" name="start" />
                <datalist id="stop_list">"#,
    css=CSS);
    for name in monitor.schedule.stops.iter().map(|(_, stop)| stop.name.clone()).sorted().unique() {
        write!(&mut w, r#"
                    <option>{name}</option>"#,
        name=name);
    }
    write!(&mut w, r#"
                </datalist>
            </p>
            <input type="submit" value="Absenden"/>
        </form>
    </body>
</html>"#
    );
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
}

fn handle_route_with_stop(response: &mut Response<Body>, monitor: &Arc<Monitor>, _arrival: NaiveDateTime, journey: &[String]) -> FnResult<()> {
    let journey = JourneyData::new(monitor.schedule.clone(), &journey)?;

    println!("Parsed journey: time: {}\n\nstops: {:?}\n\ntrips: {:?}", journey.start_date_time, journey.stops, journey.trips);
    
    let res = match journey.get_last_component() {
        JourneyComponent::Stop(stop_data) => generate_first_stop_page(response, monitor, stop_data),
        JourneyComponent::Trip(trip_data) => generate_trip_page(response, monitor, trip_data),
        JourneyComponent::None => generate_error_page(response, StatusCode::BAD_REQUEST, &format!("Empty journey."))
    };

    if let Err(e) = res {
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

fn generate_first_stop_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, stop_data: &StopData) -> FnResult<()> {
    println!("Found {} stop_ids for {}: {:?}", stop_data.stop_ids.len(), stop_data.stop_name, stop_data.stop_ids);

    let mut departures : Vec<DbPrediction> = Vec::new();
    let min_time = stop_data.min_time.unwrap() - Duration::minutes(stop_data.min_time.unwrap().time().minute() as i64 % 5); // round to previous nice time
    let max_time = min_time + Duration::minutes(30);
    
    for stop_id in &stop_data.stop_ids {
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

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>
        <meta name=viewport content="width=device-width, initial-scale=1">
    </head>
    <body>
        <h1>Abfahrten für {stop_name}, {date} von {min_time} bis {max_time}</h1>
            <div class="header">
            <div class="timing">
                <div class="head time">Plan</div>
                <div class="head min" title="Früheste Abfahrt, die in 99% der Fälle nicht unterschritten wird">-</div>
                <div class="head med">○</div>
                <div class="head max">+</div>
            </div>
            <div class="head type">Typ</div>
            <div class="head route">Linie</div>
            <div class="head headsign">Ziel</div>
            <div class="head source">Daten</div>
        </div>
        <div class="timeline">"#,
        css = CSS,
        stop_name = stop_data.stop_name,
        date = min_time.format("%A, %e. %B"),
        min_time = min_time.format("%H:%M"),
        max_time = max_time.format("%H:%M")
    );
    for dep in departures {
        write_departure_output(&mut w, &dep)?;
    }
    for m in (0..31).step_by(1) {
        writeln!(&mut w, r#"    <div class="{class}" style="left: {percent:.1}%;"><span>{time}</span></div>"#,
            class = if m % 5 == 0 { "timebar" } else { "timebar-5" },
            percent = m as f32 / 30.0 * 100.0,
            time = (min_time + Duration::minutes(m)).format("%H:%M")
        );
    }
    write!(&mut w, r#"
    </div>
</body>
</html>"#,
    );
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}

fn generate_trip_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, trip_data: &TripData) -> FnResult<()> {
    let trip = monitor.schedule.get_trip(&trip_data.trip_id)?;
    let route = monitor.schedule.get_route(&trip.route_id)?;
    
    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>
        <meta name=viewport content="width=device-width, initial-scale=1">
    </head>
    <body>
        <h1>Halte für Linie {route_name} nach {headsign}</h1>
            <div class="header">
            <div class="timing">
                <div class="head time">Plan</div>
                <div class="head min" title="Früheste Abfahrt, die in 99% der Fälle nicht unterschritten wird">-</div>
                <div class="head med">○</div>
                <div class="head max">+</div>
            </div>
            <div class="head type">Typ</div>
            <div class="head route">Linie</div>
            <div class="head headsign">Ziel</div>
            <div class="head source">Daten</div>
        </div>
        <div class="timeline">"#,
        css = CSS,
        route_name = route.short_name,
        headsign = trip.trip_headsign.as_ref().unwrap(),
    );
    for stop_time in &trip.stop_times {
        write_stop_time_output(&mut w, &stop_time)?;
    }
    for m in (0..31).step_by(1) {
        writeln!(&mut w, r#"    <div class="{class}" style="left: {percent:.1}%;"><span>{time}</span></div>"#,
            class = if m % 5 == 0 { "timebar" } else { "timebar-5" },
            percent = m as f32 / 30.0 * 100.0,
            time = (trip_data.start_departure + Duration::minutes(m)).format("%H:%M")
        );
    }
    write!(&mut w, r#"
    </div>
</body>
</html>"#,
    );
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
}


fn write_departure_output(mut w: &mut Vec<u8>, dep: &DbPrediction) -> FnResult<()> {
    let md = dep.meta_data.as_ref().unwrap();
    let a_01 = dep.get_absolute_time_for_probability(0.01).unwrap();
    let a_50 = dep.get_absolute_time_for_probability(0.50).unwrap();
    let a_99 = dep.get_absolute_time_for_probability(0.99).unwrap();
    let r_01 = dep.get_relative_time_for_probability(0.01) / 60;
    let r_50 = dep.get_relative_time_for_probability(0.50) / 60;
    let r_99 = dep.get_relative_time_for_probability(0.99) / 60;
    
    // let mut fg = Figure::new();
    // let axes = fg.axes2d();
    // let c_plot = dep.prediction_curve.get_values_as_vectors();
    // axes.lines_points(&c_plot.0, &c_plot.1, &[Color("grey")]);
    // // TODO generate a unique name for a temporary file here, 
    // // generate an img-Element with that filename, and then
    // // when the request for the image arrives, wait until the file is written.
    // fg.save_to_svg("data/monitor/tmp.svg", 800, 128)?;

    //let trip_link =  format!("{}/", dep.trip_id);
    let trip_start_date_time = dep.trip_start_date.and_hms(0, 0, 0) + dep.trip_start_time;
    let trip_link =  format!("{} {} nach {} um {}/", route_type_to_str(md.route_type), md.route_name, md.headsign, md.scheduled_time_absolute.format("%H:%M"));
    // let source_link = format!("/info/{}/{}/{}/{}", dep.route_id, dep.trip_id, dep.trip_start_date, dep.trip_start_time.num_seconds());

    let (origin_letter, origin_description) = match (&dep.origin_type, &dep.precision_type) {
        (OriginType::Realtime, PrecisionType::Specific) => ("E","Aktuelle Echtzeitdaten"),
        (OriginType::Realtime, PrecisionType::FallbackSpecific) => ("E","Aktuelle Echtzeitdaten"),
        (OriginType::Realtime, _) => ("U","Ungenutzte Echtzeitdaten"),
        (OriginType::Schedule, _) => ("P","Fahrplandaten"),
        (OriginType::Unknown, _)  => ("?","Unbekannte Datenquelle")
    };

    let (precision_letter, precision_description) = match dep.precision_type {
        PrecisionType::Specific           => ("S+", "Spezifische Prognose für diese Linie, Haltestelle und Tageszeit"),
        PrecisionType::FallbackSpecific   => ("S" , "Spezifische Prognose für diese Linie und Haltestelle"),
        PrecisionType::SemiSpecific       => ("S-", "Spezifische Prognose für diese Linie und Haltestelle, jedoch ohne Echtzeitdaten zu nutzen"),
        PrecisionType::General            => ("G+", "Generelle Prognose für Fahrzeugart, Tageszeit und Routenabschnitt"),
        PrecisionType::FallbackGeneral    => ("G" , "Generelle Prognose für Fahrzeugart"),
        PrecisionType::SuperGeneral       => ("G-", "Standardprognose, sehr ungenau"),
        PrecisionType::Unknown            => ("?" , "Unbekanntes Prognoseverfahren"),
    };

    let (type_letter, type_class) = match md.route_type {
        RouteType::Bus     => ("Bus", "b"),
        RouteType::Rail    => ("Bahn"  , "s"),
        RouteType::Subway  => ("U"  , "u"),
        RouteType::Tramway => ("Tram"  , "m"),
        RouteType::Ferry   => ("F"  , "f"),
        _                  => ("?"  , "d"),
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

    write!(&mut w, r#"
        <a href="{trip_link}" class="outer">
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
                <div class="area source" title="{source_long}"><span class="bubble {source_class}">{source_short}</span></div>
            </div>
            <div class="visu"></div>
        </a>"#,
        trip_link = trip_link,
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
        headsign = md.headsign,
        source_long = format!("{} und {}, basierend auf {} vorherigen Aufnahmen.", origin_description, precision_description, dep.sample_size),
        source_short = format!("{}/{}", origin_letter, precision_letter),
        source_class = source_class,
    );
    Ok(())
}

fn write_stop_time_output(mut w: &mut Vec<u8>, stop_time: &StopTime) -> FnResult<()> {
    let stop_link = format!("{}/", stop_time.stop.name);
    let scheduled_time = NaiveTime::from_num_seconds_from_midnight(stop_time.arrival_time.unwrap(),0);
    let a_01 = scheduled_time;
    let a_50 = scheduled_time;
    let a_99 = scheduled_time;
    let r_01 = 0;
    let r_50 = 0;
    let r_99 = 0;

    write!(&mut w, r#"
        <a href="{stop_link}" class="outer">
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
                <div class="area source" title="{source_long}"><span class="bubble {source_class}">{source_short}</span></div>
            </div>
            <div class="visu"></div>
        </a>"#,
        stop_link = stop_link,
        time = scheduled_time.format("%H:%M"),
        min = format_delay(r_01),
        min_tooltip = a_01.format("%H:%M:%S"),
        med = format_delay(r_50),
        med_tooltip = a_50.format("%H:%M:%S"),
        max = format_delay(r_99),
        max_tooltip = a_99.format("%H:%M:%S"),
        type_letter = "?",
        type_class = "",
        route_name = "",
        headsign = stop_time.stop.name,
        source_long = "",
        source_short = "?",
        source_class = "",
    );
    Ok(())
}

fn format_delay(delay: i32) -> String {
    if delay > 0 {
        format!("+{}", delay)
    } else  {
        format!("{}", delay)
    }
}

fn generate_info_page(response: &mut Response<Body>,  monitor: &Arc<Monitor>, route_id: String, trip_id: String, start_date: NaiveDate, start_time: NaiveTime) -> FnResult<()> {
    let route = monitor.schedule.get_route(&route_id)?;
    let trip = monitor.schedule.get_trip(&trip_id)?;
    let route_variant = trip.route_variant.as_ref().unwrap();

    let mut w = Vec::new();
    write!(&mut w, r#"
<html>
    <head>
        <title>ÖPNV-Reiseplaner</title>
        <style>{css}</style>
    </head>
    <body>
        <h1>Informationen für Linie {route_name} (route_id {route_id}, route_variant {route_variant}) nach {headsign}</h1>
        <h2>Statistische Analysen</h2>"#,
        css = CSS,
        route_name = route.short_name.clone(),
        route_id = route_id,
        route_variant = route_variant,
        headsign = trip.trip_headsign.as_ref().or_error("trip_headsign is None")?.clone()
    );

    match monitor.stats.specific.get(&route_id) {
        None => { writeln!(&mut w, "        Keine Linien-spezifischen Statistiken  vorhanden."); },
        Some(route_data) => {
            match route_data.variants.get(&route_variant.parse()?) {
                None =>  { writeln!(&mut w, "        Keine Statistiken für die Linien-Variante {} vorhanden.</li></ul>", route_variant);} ,
                Some(route_variant_data) => {
                    writeln!(&mut w, "        <ul>"); 
                    for et in &EventType::TYPES {
                        let curve_set_keys = route_variant_data.curve_sets[**et].keys();
                        let general_keys = route_variant_data.general_delay[**et].keys();
                        writeln!(&mut w, "            <li>Daten ({:?}) für die Linien-Variante: {} Curve Sets, {} General Curves.", **et, curve_set_keys.len(), general_keys.len());
                        for ts in TimeSlot::TIME_SLOTS_WITH_DEFAULT.iter() {
                            write!(&mut w, r#"
                            <li>Timeslot: {ts_description}
                                <ul>
                            "#, ts_description = ts.description);
                            for key in route_variant_data.curve_sets[**et].keys() {
                                if key.time_slot.id == ts.id {
                                    let data : &CurveSetData = route_variant_data.curve_sets[**et].get(&key).unwrap();
                                    writeln!(&mut w, "                            <li>Von {} nach {} ({} Samples)</li>", key.start_stop_index, key.end_stop_index, data.sample_size);
                                } 
                            }
                            write!(&mut w, r#"
                                </li>
                            </ul>"#);
                        }
                        writeln!(&mut w, "            </li>");
                    }
                    writeln!(&mut w, "        </ul>");
                }
            }
        }
    }
    write!(&mut w, r#"
        <h2>Echtzeitdaten</h2>
        (Noch nicht implementiert)
    </body>
</html>"#
    );
    *response.body_mut() = Body::from(w);
    response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(())
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
    pub route_type: RouteType,
}

impl DbPrediction {
    pub fn compute_meta_data(&mut self, monitor: &Arc<Monitor>) -> FnResult<()> {
        let trip = monitor.schedule.get_trip(&self.trip_id)?;
        let route = monitor.schedule.get_route(&self.route_id)?;
        let route_name = route.short_name.clone();
        let route_type = route.route_type;
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
            route_type,
        });
        
        Ok(())
    }

    pub fn get_absolute_time_for_probability(&self, prob: f32) -> FnResult<NaiveDateTime> {
        let x = self.prediction_curve.x_at_y(prob);
        Ok(date_and_time(&self.trip_start_date, self.meta_data.as_ref().or_error("Prediction has no meta_data")?.scheduled_time_seconds as i32 + x as i32))
    }

    pub fn get_relative_time_for_probability(&self, prob: f32) -> i32 {
        self.prediction_curve.x_at_y(prob) as i32
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
    stop_id: &str, 
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
        RouteType::Other(u16) => "Fahrzeug",
    }
}