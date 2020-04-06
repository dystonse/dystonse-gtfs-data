mod importer;
mod analyser;

use std::error::Error;
#[macro_use]
extern crate lazy_static;

use clap::{App, Arg, ArgMatches};
use mysql::*;
use retry::delay::Fibonacci;
use retry::retry;

use importer::Importer;
use analyser::Analyser;

// This is handy, because mysql defines its own Result type and we don't
// want to repeat std::result::Result
type FnResult<R> = std::result::Result<R, Box<dyn Error>>;

pub struct Main {
    verbose: bool,
    pool: Pool,
    args: ArgMatches,
    source: String,
    schedule_dir: Option<String>,
    rt_dir: Option<String>,
}

fn main() -> FnResult<()> {
    let mut instance = Main::new()?;
    instance.run()?;
    Ok(())
}

fn parse_args() -> ArgMatches {
    let matches = App::new("Dystonse GTFS Tool")
        .subcommand(Importer::get_subcommand())
        .subcommand(Analyser::get_subcommand())
        .arg(Arg::with_name("verbose")
            .short('v')
            .long("verbose")
            .help("Output status messages during run.")
        ).arg(Arg::with_name("password")
            .short('p')
            .long("password")
            .env("DB_PASSWORD")
            .takes_value(true)
            .help("Password used to connect to the database.")
            .required_unless("help")
        ).arg(Arg::with_name("user")
            .short('u')
            .long("user")
            .env("DB_USER")
            .takes_value(true)
            .help("User on the database.")
            .default_value("dystonse")
        ).arg(Arg::with_name("host")
            .long("host")
            .env("DB_HOST")
            .takes_value(true)
            .help("Host on which the database can be connected.")
            .default_value("localhost")   
        ).arg(Arg::with_name("port")
            .long("port")
            .env("DB_PORT")
            .takes_value(true)
            .help("Port on which the database can be connected.")
            .default_value("3306")
        ).arg(Arg::with_name("database")
            .short('d')
            .long("database")
            .env("DB_DATABASE")
            .takes_value(true)
            .help("Database name which will be selected.")
            .default_value("dystonse")
        ).arg(Arg::with_name("source")
            .short('s')
            .long("source")
            .env("GTFS_DATA_SOURCE_ID")
            .takes_value(true)
            .help("Source identifier for the data sets. Used to distinguish data sets with non-unique ids.")
            .required_unless("help")
        )
        .get_matches();
    return matches;
}

impl Main {
    /// Constructs a new instance of Main, with parsed arguments and a ready-to-use pool of database connections.
    fn new() -> FnResult<Main> {
        let args = parse_args();
        let verbose = args.is_present("verbose");
        let source = String::from(args.value_of("source").unwrap()); // already validated by clap

        if verbose {
            println!("Connecting to database…");
        }
        let pool = retry(Fibonacci::from_millis(1000), || {
            Main::open_db(&args, verbose)
        })
        .expect("DB connections should succeed eventually.");
        Ok(Main {
            args,
            verbose,
            pool,
            source,
            schedule_dir: None,
            rt_dir: None,
        })
    }

    /// Runs the actions that are selected via the command line args
    fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("import", Some(sub_args)) => {
                let mut importer = Importer::new(&self, sub_args);
                importer.run()
            }
            ("analyse", Some(sub_args)) => {
                let mut analyser = Analyser::new(&self, sub_args);
                analyser.run()
            }
            _ => panic!("Invalid arguments."),
        }
    }

    /// Opens a connection to a database and returns the resulting connection pool.
    /// Takes configuration values from DB_PASSWORD, DB_USER, DB_HOST, DB_PORT and DB_DATABASE
    /// environment variables. For all values except DB_PASSWORD a default is provided.
    fn open_db(args: &ArgMatches, verbose: bool) -> FnResult<Pool> {
        if verbose {
            println!("Trying to connect to the database.");
        }
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            args.value_of("user").unwrap(), // already validated by clap
            args.value_of("password").unwrap(), // already validated by clap
            args.value_of("host").unwrap(), // already validated by clap
            args.value_of("port").unwrap(), // already validated by clap
            args.value_of("database").unwrap()  // already validated by clap
        );
        let pool = Pool::new(url)?;
        Ok(pool)
    }

}
