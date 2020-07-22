use crate::{FnResult, OrError, Main};
use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use crate::types::{PredictionBasis, DefaultCurveKey, PrecisionType, CurveData, CurveSetKey};
use std::sync::Arc;
use gtfs_structures::{Gtfs, Trip};

use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use futures::executor::block_on;


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
        // creates one from our `hello_world` function.
        let make_svc = make_service_fn(|_conn| async {
            // service_fn converts our function into a `Service`
            Ok::<_, Infallible>(service_fn(Self::hello_world))
        });

        let server = Server::bind(&addr).serve(make_svc);

        println!("Waiting for connections…");
        // Run this server for... forever!
        if let Err(e) = server.await {
            eprintln!("server error: {}", e);
        }
    }

    async fn hello_world(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
        Ok(Response::new("Hello, World".into()))
    }
}