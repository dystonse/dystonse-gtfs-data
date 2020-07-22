use crate::{FnResult, OrError, Main};
use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use crate::types::{PredictionBasis, DefaultCurveKey, PrecisionType, CurveData, CurveSetKey};
use std::sync::Arc;
use gtfs_structures::{Gtfs, Trip};

use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, Method, StatusCode};
use hyper::header::{HeaderName, HeaderValue};
use hyper::service::{make_service_fn, service_fn};
use futures::executor::block_on;

mod css;
use css::CSS;

use typed_html::{html, dom::DOMTree, text};


pub struct Monitor {
    #[allow(dead_code)]
    pub schedule: Arc<Gtfs>
}

impl Monitor {
    pub fn get_subcommand() -> App<'static>{
        App::new("monitor").about("Starts a web server that serves the monitor website.")
    }

    pub fn new(main: &Main, _sub_args: &ArgMatches) -> FnResult<Monitor> {
        Ok(Monitor {
            schedule: main.get_schedule()?,
        })
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Self::serve().await
        });

        Ok(())
    }

    async fn serve() {
        // We'll bind to 127.0.0.1:3000
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

        // A `Service` is needed for every connection, so this
        // creates one from our `hello_dystonse` function.
        let make_svc = make_service_fn(|_conn| async {
            // service_fn converts our function into a `Service`
            Ok::<_, Infallible>(service_fn(Self::hello_dystonse))
        });

        let server = Server::bind(&addr).serve(make_svc);

        println!("Waiting for connections…");
        // Run this server for... forever!
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    }

    async fn hello_dystonse(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut response = Response::new(Body::empty());

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => {
                let author = "Dystonse GbR";

                let mut doc: DOMTree<String> = html!(
                    <html>
                        <head>
                            <title>"Reiseplaner"</title>
                            <meta name="author" content=author/>
                            <style>{ text!("{}", CSS)}</style>
                        </head>
                        <body>
                            <h1>"Reiseplaner"</h1>
                            <p class="official">
                                "Herzlich willkommen. Hier kannst du deine Reiseroute mit dem ÖPNV planen."
                            </p>
                            <p class="dropdown" >
                                <label for="start">"Start-Haltestelle:"</label>
                                <input list="stop_list" id="stops" name="stops" />
                                <datalist id="stop_list">
                                    <option value="2344">"Hamburger Straße"</option>
                                    <option value="5466" label="Hauptbahnhof"/>
                                    <option value="Internet Explorer"/>
                                    <option value="Opera"/>
                                    <option value="Safari"/>
                                </datalist>
                            </p>
                            { (0..3).map(|_| html!(
                                <p class="emphasis">
                                    "Her name is Kitty White."
                                </p>
                            )) }
                        </body>
                    </html>
                );
                let doc_string = doc.to_string();
                *response.body_mut() = Body::from(doc_string);
                response.headers_mut().append(hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
            },
            (&Method::GET, "/kitty") => {
                let author = "Lena and Kiki";

                let mut doc: DOMTree<String> = html!(
                    <html>
                        <head>
                            <title>"Hello Kitty"</title>
                            <meta name="author" content=author/>
                        </head>
                        <body>
                            <h1>"Hello Kitty"</h1>
                            <p class="official">
                                "She is not a cat. She is a human girl."
                            </p>
                            { (0..3).map(|_| html!(
                                <p class="emphasis">
                                    "Her name is Kitty White."
                                </p>
                            )) }
                            <p class="citation-needed">
                                "We still don't know how she eats."
                            </p>
                        </body>
                    </html>
                );
                let doc_str = doc.to_string();

                *response.body_mut() = Body::from(doc_str);
            },
            // TODO: this needs to be adapted!
            (&Method::POST, "/echo") => {
                *response.body_mut() = req.into_body();
            },
            _ => {
                *response.status_mut() = StatusCode::NOT_FOUND;
            },
        };
    
        Ok(response)
    }

    //not used anymore, was used for tutorial
    async fn hello_echo(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut response = Response::new(Body::empty());

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => {
                *response.body_mut() = Body::from("Try POSTing data to /echo");
            },
            (&Method::GET, "/kitty") => {
                let author = "Lena and Kiki";

                let mut doc: DOMTree<String> = html!(
                    <html>
                        <head>
                            <title>"Hello Kitty"</title>
                            <meta name="author" content=author/>
                        </head>
                        <body>
                            <h1>"Hello Kitty"</h1>
                            <p class="official">
                                "She is not a cat. She is a human girl."
                            </p>
                            { (0..3).map(|_| html!(
                                <p class="emphasis">
                                    "Her name is Kitty White."
                                </p>
                            )) }
                            <p class="citation-needed">
                                "We still don't know how she eats."
                            </p>
                        </body>
                    </html>
                );
                let doc_str = doc.to_string();

                *response.body_mut() = Body::from(doc_str);
            },
            (&Method::POST, "/echo") => {
                *response.body_mut() = req.into_body();
            },
            _ => {
                *response.status_mut() = StatusCode::NOT_FOUND;
            },
        };
    
        Ok(response)
    }

    //not used anymore, was used for tutorial
    async fn hello_world(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
        Ok(Response::new("Hello, World".into()))
    }


}