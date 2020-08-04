mod per_schedule_importer;
mod scheduled_predictions_importer;
mod batched_statements;

use simple_error::bail;
use clap::{App, Arg, ArgMatches, ArgGroup};
use rayon::prelude::*;
use std::fs::DirBuilder;
use std::path::{Path, PathBuf};
use std::{thread, time};
use ureq::get;
use mysql::*;
use mysql::prelude::*;
use chrono::{NaiveDate, NaiveTime, Local, Duration, DateTime, Utc};
use std::sync::Mutex;
use std::collections::HashMap;

use crate::{Main, FileCache, FnResult, read_dir_simple, date_from_filename, OrError};
use crate::types::PredictionBasis;

use per_schedule_importer::PerScheduleImporter;
use scheduled_predictions_importer::ScheduledPredictionsImporter;

lazy_static! {
    static ref MAX_ESTIMATED_TRIP_DURATION: Duration =  Duration::hours(12);
}

const TIME_BETWEEN_DIR_SCANS: time::Duration = time::Duration::from_secs(5);

#[derive(Hash, PartialEq, Eq, Clone)]
struct VehicleIdentifier {
    trip_id: String,
    start_time: NaiveTime,
    start_date: NaiveDate
}

pub struct Importer<'a>  {
    main: &'a Main,
    args: &'a ArgMatches,
    rt_dir: Option<String>,
    schedule_dir: Option<String>,
    target_dir: Option<String>,
    fail_dir: Option<String>,
    verbose: bool,
    perform_cleanup: bool,
    last_ping_time_mutex: Mutex<Option<DateTime<Local>>>,
    current_prediction_basis: Mutex<HashMap<VehicleIdentifier, PredictionBasis>> //used in per_schedule_importer, but declared here for persistence
}


impl<'a> Importer<'a>  {
    pub fn get_subcommand() -> App<'a> {
        App::new("import")
            .about("Processes GTFS realtime files in multiple ways and writes the results into a database. See long help for more information.")
            .long_about("Processes GTFS realtime files in multiple ways and writes the results into a database.
            
            The realtime data is interpreted in relation to a GTFS schedule.
            
            Processing can involve:
             - *record*ing for later analysis
             - creating updated *predict*ions
             - both")
            .arg(Arg::new("record")
                .about("Indicates that realtime data shall be recorded for later analysis.")
                .short('r')
                .long("record")
                .takes_value(false)
            )
            .arg(Arg::new("predict")
                .about("Indicates that realtime data shall be used to update current predictions.")
                .short('p')
                .long("predict")
                .takes_value(false)
            )
            .arg(Arg::new("cleanup")
                .about("Indicates that on each run, outdated predictions shall be deleted.")
                .short('c')
                .long("cleanup")
                .takes_value(false)
            )
            .group(ArgGroup::new("processing")
                .args(&["record", "predict", "cleanup"])
                .required(true)
                .multiple(true)
            )
            .subcommand(App::new("automatic")
                .about("Runs forever, importing all files which are present or become present during the run.")
                .arg(Arg::new("pingurl")
                    .long("pingurl")
                    .env("PING_URL")
                    .takes_value(true)
                    .about("An URL that will be pinged (using HTTP GET) after each iteration.")
                )
            )
            .subcommand(App::new("batch")
                .about("Imports all files which are present at the time it is started.")
                .arg(Arg::new("dir")
                    .index(1)
                    .value_name("DIRECTORY")
                    .required_unless("help")
                    .about("The directory which contains schedules and realtime data")
                    .long_about(
                        "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                        and realtime data (located in a subdirectory named 'rt'). \
                        Successfully processed files are moved to a subdirectory named 'imported'. \
                        The 'imported' subdirectory will be created automatically if it doesn't already exist."
                    )
                )
            )
            .subcommand(App::new("manual")
                .about("Imports all specified realtime files using one specified schedule. Paths to schedule and realtime files have to be given as arguments.")
                .arg(Arg::new("schedule")
                    .index(1)
                    .value_name("SCHEDULE")
                    .about("The static GTFS schedule, as directory or .zip")
                ).arg(Arg::new("rt")
                    .index(2)
                    .multiple(true)
                    .value_name("PBs")
                    .about("One or more files with real time data, as .pb or .zip")
                )
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Importer<'a> {
        Importer {
            main,
            args,
            target_dir: None,
            fail_dir: None,
            schedule_dir: None,
            rt_dir: None,
            verbose: main.verbose,
            perform_cleanup: args.is_present("cleanup"),
            last_ping_time_mutex: Mutex::new(None),
            current_prediction_basis: Mutex::new(HashMap::new()),
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("automatic", Some(_sub_args)) => {
                self.set_dir_paths()?;
                self.run_as_non_manual(true)
            }
            ("batch", Some(_sub_args)) => {
                self.set_dir_paths()?;
                self.run_as_non_manual(false)
            }
            ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }

    /// Handle manual mode
    fn run_as_manual(&self, args: &ArgMatches) -> FnResult<()> {
        if self.perform_cleanup {
            self.run_cleanup()?;
        }
        
        if self.args.is_present("record") || self.args.is_present("predict") {
            let gtfs_schedule_filename = args.value_of("schedule").or_error("The argument <SCHEDULE> is required when --record or --predict is provided.")?;
            let gtfs_realtime_filenames: Vec<String> = args
                .values_of("rt")
                .or_error("The argument <REALTIME> is required when --record or --predict is provided.")? // already validated by clap
                .map(|s| String::from(s))
                .collect();
            if let Err(e) = self.process_schedule_and_realtimes(&gtfs_schedule_filename, &gtfs_realtime_filenames) {
                eprintln!("Error while processing schedule and realtimes: {}.", e);
            }
        }
        Ok(())
    }

    /// Handle cleanup command
    fn run_cleanup(&self) -> FnResult<()> {
        let min = Utc::now().naive_utc() - *MAX_ESTIMATED_TRIP_DURATION;
        let min_start_date = min.date();
        let min_start_time = min.time();
        if self.verbose {
            println!("Deleting all predictions with trip start before {}.", min);
        }
        let mut con = self.main.pool.get_conn()?;
        let statement = con.prep(
            r"DELETE FROM 
                predictions 
            WHERE 
                `source` = :source AND (
                    `trip_start_date` < :min_start_date OR (
                        `trip_start_date` = :min_start_date AND
                        `trip_start_time` < :min_start_time
                    )
                );",
        )?;
        con.exec_drop(statement, params!{
            "source" => self.main.source.clone(),
            "min_start_date" => min_start_date,
            "min_start_time" => min_start_time,
        })?;
        // TODO handle deadlock error here, like we already do in BatchedStatements.

        // Clean up outdated entries from the current_prediction_basis:
        if self.verbose {
            println!("Database prediction cleanup successful. Now deleting old entries from prediction basis cache.");
        }
        { // block for mutex
            let mut cpr = self.current_prediction_basis.lock().unwrap();
            let mut to_remove : Vec<VehicleIdentifier> = Vec::new();
            for key in cpr.keys() {
                if(key.start_date < min_start_date) 
                    ||  (key.start_date == min_start_date && key.start_time < min_start_time) 
                {
                    to_remove.push(key.clone());
                }
            }
            for key in &to_remove {
                cpr.remove(key);
            }
            // TODO: try out if we need to call cpr.shrink_to_fit() here. 
            // It might be useful to prevent unlimited growth of its allocated space.
            // But it might also slow down the predictions because the map would be reallocated more often.
            if self.verbose {
                println!("Deleted {} entries from prediction basis cache", to_remove.len());
            }
        }
        Ok(())
    }

    // this has been used in the past, but the code which was used to create those tuples
    // was *very* ugly and has been deleted. We need a new way to handle success statistics
    // now that there are multiple possible import targets (record and/or predict). 
    fn _output_statistics(&self, statistics: ((u32, u32), (u32, u32), (u32, u32), (u32, u32))) {
        if self.verbose {
            println!("Finished processing files.");
            println!(
                "Schedule files   : {} of {} successful.",
                (statistics.0).1,
                (statistics.0).0
            );
            println!(
                "Realtime files   : {} of {} successful.",
                (statistics.1).1,
                (statistics.1).0
            );
            println!(
                "Trip updates     : {} of {} successful.",
                (statistics.2).1,
                (statistics.2).0
            );
            println!(
                "Stop time updates: {} of {} successful.",
                (statistics.3).1,
                (statistics.3).0
            );
        }
    }

    /// Construct the full directory paths used for storing input files and processed files
    /// needs the dir argument, this means it can only be used when running in non manual modes
    fn set_dir_paths(&mut self) -> FnResult<()> {
        // construct paths of directories
        let dir = &self.main.dir;
        self.target_dir = Some(format!("{}/imported", dir));
        self.fail_dir = Some(format!("{}/failed", dir));
        self.rt_dir = Some(format!("{}/rt", dir));
        self.schedule_dir = Some(format!("{}/schedule", dir));
        Ok(())
    }

    /// makes a request to the configured ping URL if the last ping-attempt was more 
    /// than 1 minute ago (or if there never was a previous attempt)
    fn ping_url(&self) {
        let mut perform_ping = false;
        let url_opt = self.args.subcommand_matches("automatic").unwrap().value_of("pingurl");

        if url_opt.is_some() {
            // Last_ping_time is within a mutex because multiple threads may call this concurrently.
            let mut last_ping_time = self.last_ping_time_mutex.lock().unwrap();
            if last_ping_time.is_none() || last_ping_time.unwrap() < Local::now() - Duration::minutes(1) {
                perform_ping = true;
                *last_ping_time = Some(Local::now());
            } else if self.verbose {
                println!("Last ping less then a minute ago, skip Pinging.");
            }
            // If url_opt is None, perform_ping will be false anyway,
            // so we can perform the ping outside this block to
            // release the mutex before the actual request is made.
        }

        if perform_ping {
            if self.verbose {
                println!("Pinging URL {}", url_opt.unwrap());
            }
            get(url_opt.unwrap()).call();
        }
    }

    /// Handle automatic mode and batch mode, which are very similar to each other
    fn run_as_non_manual(&self, is_automatic: bool) -> FnResult<()> {
        // ensure that the directory exists
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        builder.create(self.target_dir.as_ref().unwrap())?; // if target dir can't be created, there's no good way to continue execution
        builder.create(self.fail_dir.as_ref().unwrap())?; // if fail dir can't be created, there's no good way to continue execution
        if is_automatic {
            loop {
                match self.process_all_files() {
                    Ok(true) => {
                        if self.verbose {
                            println!("Finished one iteration. Sleeping until next directory scan.");
                        }
                    },
                    Ok(false) => {
                        match ScheduledPredictionsImporter::new(&self, self.verbose) {
                            Ok(mut spi) => {
                                if self.verbose {
                                    println!("No realtime data to import. Starting to import predictions from schedule...");
                                }
                                match spi.make_scheduled_predictions() {
                                    Ok(_) => { 
                                        if self.verbose {
                                            println!("Sucessfully imported some schedule-based predictions. Sleeping until next directory scan.");
                                        }
                                    },
                                    Err(e) => {
                                        eprintln!("Error while trying to import schedule-based predictions: {}. Sleeping until next directory scan.", e);
                                    },
                                }
                            },
                            Err(e) => {
                                eprintln!("Could not initialize ScheduledPredictionsImporter: {}", e);
                            }
                        }
                    }
                    Err(e) => eprintln!(
                        "Iteration failed with error: {}. Sleeping until next directory scan.",
                        e
                    ),
                }
                if self.perform_cleanup {
                    if let Err(e) = self.run_cleanup() {
                        println!("Error during cleanup: {}", e);
                    }
                }
                self.ping_url();

                thread::sleep(TIME_BETWEEN_DIR_SCANS);
            }
        } else {
            match self.process_all_files() {
                Ok(_) => {
                    if self.verbose {
                        println!("Finished.");
                    }
                }
                Err(e) => eprintln!("Failed with error: {}.", e),
            }
            if self.perform_cleanup {
                self.run_cleanup()?;
            }
            return Ok(());
        }
    }

    fn process_all_files(&self) -> FnResult<bool> {
        if self.verbose {
            println!("Scan directory");
        }
        // list files in both directories
        let mut schedule_filenames = read_dir_simple(&self.schedule_dir.as_ref().unwrap())?;
        let rt_filenames = read_dir_simple(&self.rt_dir.as_ref().unwrap())?;

        if rt_filenames.is_empty() {
            return Ok(false); //false for "no realtime files imported"
        }

        if schedule_filenames.is_empty() {
            bail!("No schedule data (but real time data is present).");
        }

        // get the date of the earliest schedule, then reverse the list to start searching with the latest schedule
        let oldest_schedule_date = date_from_filename(&schedule_filenames[0])?;
        schedule_filenames.reverse();

        // data structures to collect the files to work on in the current iteration (one schedule and all its corresponding rt files)
        let mut current_schedule_file = String::new();
        let mut realtime_files_for_current_schedule: Vec<String> = Vec::new();

        // Iterate over all rt files (oldest first), collecting all rt files that belong to the same schedule to process them in batch.
        for rt_filename in rt_filenames {
            let rt_date = match date_from_filename(&rt_filename) {
                Ok(date) => date,
                Err(e) => {
                    match &self.fail_dir {
                        Some(d) => {
                            Importer::move_file_to_dir(&rt_filename, &d)?;
                            eprintln!("Rt file {} does not contain a valid date and was moved to {}. (Error was {})", rt_filename, d, e);
                        }
                        None => eprintln!(
                            "Rt file {} does not contain a valid date. (Error was {})",
                            rt_filename, e
                        ),
                    }
                    continue;
                }
            };

            if rt_date < oldest_schedule_date {
                eprintln!(
                    "Realtime data {} is older than any schedule, skipping.",
                    rt_filename
                );
                continue;
            }

            // Look at all schedules (newest first)
            for schedule_filename in &schedule_filenames {
                let schedule_date = match date_from_filename(&schedule_filename) {
                    Ok(date) => date,
                    Err(e) => {
                        match &self.fail_dir {
                            Some(d) => {
                                Importer::move_file_to_dir(schedule_filename, &d)?;
                                eprintln!("Schedule file {} does not contain a valid date and was moved to {}. (Error was {})", schedule_filename, d, e);
                            }
                            None => eprintln!(
                                "Schedule file {} does not contain a valid date. (Error was {})",
                                schedule_filename, e
                            ),
                        }
                        continue;
                    }
                };
                // Assume we found the right schedule if this is the newest schedule that is older than the realtime file:
                if rt_date >= schedule_date {
                    // process the current schedule's collection before going to next schedule
                    if *schedule_filename != current_schedule_file {
                        if !realtime_files_for_current_schedule.is_empty() {
                            if let Err(e) = self.process_schedule_and_realtimes(
                                &current_schedule_file,
                                &realtime_files_for_current_schedule,
                            ) {
                                 eprintln!("Error while working with schedule file {}: {}", current_schedule_file, e);
                            }
                        }
                        // go on with the next schedule
                        current_schedule_file = schedule_filename.clone();
                        realtime_files_for_current_schedule.clear();
                    }
                    realtime_files_for_current_schedule.push(rt_filename.clone());
                    // Correct schedule found for this one, so continue with next realtime file
                    break;
                }
            }
        }

        // process last schedule's collection
        if !realtime_files_for_current_schedule.is_empty() {
            if let Err(e) = self.process_schedule_and_realtimes(&current_schedule_file, &realtime_files_for_current_schedule) {
                eprintln!("Error while working with schedule file {}: {}", current_schedule_file, e);
            };
        }
        Ok(true)
    }

    /// Perform the import of one or more realtime data sets relating to a single schedule
    fn process_schedule_and_realtimes(
        &self,
        gtfs_schedule_filename: &str,
        gtfs_realtime_filenames: &Vec<String>,
    ) -> FnResult<()> {
        if self.verbose {
            println!("Parsing schedule…");
        }

        let schedule = match FileCache::get_cached_simple(&self.main.gtfs_cache, gtfs_schedule_filename) {
            Ok(schedule) => schedule,
            Err(e) => {
                match &self.fail_dir {
                    Some(d) => {
                        Importer::move_file_to_dir(gtfs_schedule_filename, &d)?;
                        eprintln!("Schedule file {} could not be parsed and was moved to {}. (Error was {})", gtfs_schedule_filename, d, e);
                    }
                    None => eprintln!(
                        "Schedule file {} could not be parsed. (Error was {})",
                        gtfs_schedule_filename, e
                    ),
                }
                bail!("Schedule file could not be parsed.");
            }
        };

        if self.verbose {
            println!("Importing realtime data…");
        }

        let short_filename = &gtfs_schedule_filename[gtfs_schedule_filename.rfind('/').unwrap() + 1 ..];

        // create importer for this schedule and iterate over all given realtime files
        let imp = PerScheduleImporter::new(schedule.clone(), &self, self.verbose, short_filename)?;

        let (success, total) = gtfs_realtime_filenames
            .par_iter()
            .map(|gtfs_realtime_filename| {
                match self.process_realtime(&gtfs_realtime_filename, &imp) {
                    Ok(()) => { 
                        // if a realtime file was successfull, send a ping
                        self.ping_url();
                        (1,1)
                    },
                    Err(e) => {
                        eprintln!("Error while reading {}: {}", &gtfs_realtime_filename, e);
                        (0,1)
                    }
                }
            })
            .reduce(
                || (0, 0),
                |(a_s, a_t), (b_s, b_t)| (a_s + b_s, a_t + b_t),
            );
        if self.verbose {
            println!("Done with realtime files, {} of {} successfull!", success, total);
        }
        Ok(())
    }

    /// Process a single realtime file on the given Importer
    fn process_realtime(
        &self,
        gtfs_realtime_filename: &str,
        imp: &PerScheduleImporter,
    ) -> FnResult<()> {
        if let Err(e) = imp.handle_realtime_file(&gtfs_realtime_filename) {
            // Don't print the error itself, because it will be handled by the calling function
            eprintln!("Error in realtime file, moving to fail_dir…");
            if let Some(dir) = &self.fail_dir {
                Importer::move_file_to_dir(gtfs_realtime_filename, &dir)?;
            }
            return Err(e);
        };
        // TODO possibly make an error file per failed file to capture the error in place
        if self.verbose {
            println!("Finished importing file: {}", &gtfs_realtime_filename);
        } else {
            println!("{}", &gtfs_realtime_filename);
        }
        // move file into target_dir if target_dir is defined
        if let Some(dir) = &self.target_dir {
            Importer::move_file_to_dir(gtfs_realtime_filename, &dir)?;
        }
        Ok(())
    }

    fn move_file_to_dir(filename: &str, dir: &String) -> FnResult<()> {
        let mut target_path = PathBuf::from(dir);
        target_path.push(Path::new(&filename).file_name().unwrap()); // assume that the filename does not end in `..` because we got it from a directory listing
        std::fs::rename(filename, target_path)?;
        Ok(())
    }
}