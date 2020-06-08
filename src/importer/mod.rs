mod per_schedule_importer;

use gtfs_structures::Gtfs;
use mysql::*;
use simple_error::SimpleError;
use clap::{App, Arg, ArgMatches};
use chrono::NaiveDate;
use rayon::prelude::*;
use regex::Regex;
use std::fs;
use std::fs::DirBuilder;
use std::path::{Path, PathBuf};
use std::{thread, time};

use crate::FnResult;
use crate::Main;

use per_schedule_importer::PerScheduleImporter;

const TIME_BETWEEN_DIR_SCANS: time::Duration = time::Duration::from_secs(60);

pub struct Importer<'a>  {
    main: &'a Main,
    args: &'a ArgMatches,
    rt_dir: Option<String>,
    schedule_dir: Option<String>,
    target_dir: Option<String>,
    fail_dir: Option<String>,
    verbose: bool
}


impl<'a> Importer<'a>  {
    pub fn get_subcommand() -> App<'a> {
        App::new("import")
            .about("Combines data from GTFS schedules and GTFS realtime files and writes them into a database.")
            .subcommand(App::new("automatic")
                .about("Runs forever, importing all files which are present or become present during the run.")
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
                ).arg(Arg::new("pingurl")
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
                    .required_unless("help")
                ).arg(Arg::new("rt")
                    .index(2)
                    .multiple(true)
                    .value_name("PBs")
                    .about("One or more files with real time data, as .pb or .zip")
                    .required_unless("help")
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
            verbose: main.verbose
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("automatic", Some(sub_args)) => {
                self.set_dir_paths(sub_args)?;
                self.run_as_non_manual(true)
            }
            ("batch", Some(sub_args)) => {
                self.set_dir_paths(sub_args)?;
                self.run_as_non_manual(false)
            }
            ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }

    /// Handle manual mode
    fn run_as_manual(&self, args: &ArgMatches) -> FnResult<()> {
        let gtfs_schedule_filename = args.value_of("schedule").unwrap(); // already validated by clap
        let gtfs_realtime_filenames: Vec<String> = args
            .values_of("rt")
            .unwrap() // already validated by clap
            .map(|s| String::from(s))
            .collect();
        let statistics = match 
            self.process_schedule_and_realtimes(&gtfs_schedule_filename, &gtfs_realtime_filenames) {
                Ok(tuple) => ((1, 1), tuple.0, tuple.1, tuple.2),
                Err(e) => {
                    eprintln!("Error while processing schedule and realtimes: {}.", e);
                    ((1, 0), (0, 0), (0, 0), (0, 0))
                }
            };
        self.output_statistics(statistics);

        Ok(())
    }

    fn output_statistics(&self, statistics: ((u32, u32), (u32, u32), (u32, u32), (u32, u32))) {
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

    /// Reads contents of the given directory and returns an alphabetically sorted list of included files / subdirectories as Vector of Strings.
    pub fn read_dir_simple(path: &str) -> FnResult<Vec<String>> {
        let mut path_list: Vec<String> = fs::read_dir(path)?
            .filter_map(|r| r.ok()) // unwraps Options and ignores any None values
            .map(|d| {
                String::from(d.path().to_str().expect(&format!(
                    "Found file with invalid UTF8 in file name in directory {}.",
                    &path
                )))
            })
            .collect();
        path_list.sort();
        Ok(path_list)
    }

    pub fn date_from_filename(filename: &str) -> FnResult<NaiveDate> {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }
        let date_element_captures =
            FIND_DATE
                .captures(&filename)
                .ok_or(SimpleError::new(format!(
                "File name does not contain a valid date (does not match format YYYY-MM-DD): {}",
                filename
            )))?;
        let date_option = NaiveDate::from_ymd_opt(
            date_element_captures[1].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
            date_element_captures[2].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
            date_element_captures[3].parse().unwrap(), // can't fail because input string is known to be a bunch of decimal digits
        );
        Ok (date_option.ok_or(SimpleError::new(format!("File name does not contain a valid date (format looks ok, but values are out of bounds): {}", filename)))?)
    }

    /// Construct the full directory paths used for storing input files and processed files
    /// needs the dir argument, this means it can only be used when running in non manual modes
    fn set_dir_paths(&mut self, args: &ArgMatches) -> FnResult<()> {
        // construct paths of directories
        let dir = args.value_of("dir").unwrap(); // already validated by clap
        self.target_dir = Some(format!("{}/imported", dir));
        self.fail_dir = Some(format!("{}/failed", dir));
        self.rt_dir = Some(format!("{}/rt", dir));
        self.schedule_dir = Some(format!("{}/schedule", dir));
        Ok(())
    }

    fn ping_url(&self) {
        lazy_static! {
            static ref HTTP_CLIENT: reqwest::blocking::Client = reqwest::blocking::Client::builder()
            .timeout(time::Duration::from_secs(10))
            .build().expect("Error while initializing http client.");
        }

        
        if let Some(url) = self.args.subcommand_matches("automatic").unwrap().value_of("pingurl") {
            if self.verbose {
                println!("Pinging URL {}", url);
            }
            if let Err(e) = HTTP_CLIENT.get(url).send() {
                eprintln!("Error while pinging url {}: {}", url, e);
            }
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
                    Ok(_) => {
                        if self.verbose {
                            println!("Finished one iteration. Sleeping until next directory scan.");
                        }
                    }
                    Err(e) => eprintln!(
                        "Iteration failed with error: {}. Sleeping until next directory scan.",
                        e
                    ),
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

            return Ok(());
        }
    }

    fn process_all_files(&self) -> FnResult<()> {
        if self.verbose {
            println!("Scan directory");
        }
        // list files in both directories
        let mut schedule_filenames = Importer::read_dir_simple(&self.schedule_dir.as_ref().unwrap())?;
        let rt_filenames = Importer::read_dir_simple(&self.rt_dir.as_ref().unwrap())?;

        if rt_filenames.is_empty() {
            return Err(Box::from(SimpleError::new("No realtime data.")));
        }

        if schedule_filenames.is_empty() {
            return Err(Box::from(SimpleError::new(
                "No schedule data (but real time data is present).",
            )));
        }

        // get the date of the earliest schedule, then reverse the list to start searching with the latest schedule
        let oldest_schedule_date = Importer::date_from_filename(&schedule_filenames[0])?;
        schedule_filenames.reverse();

        // data structures to collect the files to work on in the current iteration (one schedule and all its corresponding rt files)
        let mut current_schedule_file = String::new();
        let mut realtime_files_for_current_schedule: Vec<String> = Vec::new();

        let mut schedules_count = 0;
        let mut schedules_success_count = 0;
        let mut real_time_files_count = 0;
        let mut real_time_files_success_count = 0;
        let mut trip_updates_count = 0;
        let mut trip_updates_success_count = 0;
        let mut stop_time_updates_count = 0;
        let mut stop_time_updates_success_count = 0;

        // Iterate over all rt files (oldest first), collecting all rt files that belong to the same schedule to process them in batch.
        for rt_filename in rt_filenames {
            let rt_date = match Importer::date_from_filename(&rt_filename) {
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
                    real_time_files_count += 1;
                    continue;
                }
            };

            if rt_date <= oldest_schedule_date {
                eprintln!(
                    "Realtime data {} is older than any schedule, skipping.",
                    rt_filename
                );
                // Don't increment count because this is not really a problem, 
                // and because we don't move those files into fail_dir,
                // we would count them as an error over and over again.
                // real_time_files_count += 1;
                continue;
            }

            // Look at all schedules (newest first)
            for schedule_filename in &schedule_filenames {
                let schedule_date = match Importer::date_from_filename(&schedule_filename) {
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
                if rt_date > schedule_date {
                    // process the current schedule's collection before going to next schedule
                    if *schedule_filename != current_schedule_file {
                        if !realtime_files_for_current_schedule.is_empty() {
                            schedules_count += 1;
                            match self.process_schedule_and_realtimes(
                                &current_schedule_file,
                                &realtime_files_for_current_schedule,
                            ) {
                                Ok(((rtc, rtsc), (tuc, tusc), (stuc, stusc))) => {
                                    schedules_success_count += 1;
                                    real_time_files_count += rtc;
                                    real_time_files_success_count += rtsc;
                                    trip_updates_count += tuc;
                                    trip_updates_success_count += tusc;
                                    stop_time_updates_count += stuc;
                                    stop_time_updates_success_count += stusc;
                                }
                                Err(e) => eprintln!(
                                    "Error in schedule file {}: {}",
                                    current_schedule_file, e
                                ),
                            };
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
            schedules_count += 1;
            match self.process_schedule_and_realtimes(
                &current_schedule_file,
                &realtime_files_for_current_schedule,
            ) {
                Ok(((rtc, rtsc), (tuc, tusc), (stuc, stusc))) => {
                    schedules_success_count += 1;
                    real_time_files_count += rtc;
                    real_time_files_success_count += rtsc;
                    trip_updates_count += tuc;
                    trip_updates_success_count += tusc;
                    stop_time_updates_count += stuc;
                    stop_time_updates_success_count += stusc;
                }
                Err(e) => eprintln!("Error in schedule file {}: {}", current_schedule_file, e),
            };
        }
        self.output_statistics((
            (schedules_count, schedules_success_count),
            (real_time_files_count, real_time_files_success_count),
            (trip_updates_count, trip_updates_success_count),
            (stop_time_updates_count, stop_time_updates_success_count),
        ));
        Ok(())
    }

    /// Perform the import of one or more realtime data sets relating to a single schedule
    fn process_schedule_and_realtimes(
        &self,
        gtfs_schedule_filename: &str,
        gtfs_realtime_filenames: &Vec<String>,
    ) -> FnResult<((u32, u32), (u32, u32), (u32, u32))> {
        if self.verbose {
            println!("Parsing schedule…");
        }

        let schedule = match Gtfs::new(gtfs_schedule_filename) {
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
                return Err(Box::from(SimpleError::new(
                    "Schedule file could not be parsed.",
                )));
            }
        };

        if self.verbose {
            println!("Importing realtime data…");
        }

        let short_filename = &gtfs_schedule_filename[gtfs_schedule_filename.rfind('/').unwrap() + 1 ..];

        // create importer for this schedule and iterate over all given realtime files
        let imp = PerScheduleImporter::new(&schedule, &self.main.pool, self.verbose, &self.main.source, short_filename);

        let (rt, tu, stu) = gtfs_realtime_filenames
            .par_iter()
            .map(|gtfs_realtime_filename| {
                match self.process_realtime(&gtfs_realtime_filename, &imp) {
                    Ok(tuple) => ((1,1), tuple.0, tuple.1),
                    Err(e) => {
                        eprintln!("Error while reading {}: {}", &gtfs_realtime_filename, e);
                        ((1, 0), (0, 0), (0, 0))
                    }
                }
            })
            .reduce(
                || ((0, 0), (0, 0), (0, 0)),
                |a, b| {
                    (
                        ((a.0).0 + (b.0).0, (a.0).1 + (b.0).1),
                        ((a.1).0 + (b.1).0, (a.1).1 + (b.1).1),
                        ((a.2).0 + (b.2).0, (a.2).1 + (b.2).1),
                    )
                },
            );
        if self.verbose {
            println!("Done!");
        }
        Ok((rt, tu, stu))
    }

    /// Process a single realtime file on the given Importer
    fn process_realtime(
        &self,
        gtfs_realtime_filename: &str,
        imp: &PerScheduleImporter,
    ) -> FnResult<((u32, u32), (u32, u32))> {
        let statistics = match imp.import_realtime_into_database(&gtfs_realtime_filename) {
            Ok(s) => s,
            Err(e) => {
                // Don't print the error, because it will be handled by the calling function
                eprintln!("Error in realtime file, moving to fail_dir…");
                if let Some(dir) = &self.fail_dir {
                    Importer::move_file_to_dir(gtfs_realtime_filename, &dir)?;
                }
                return Err(e);
            }
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
        Ok(statistics)
    }

    fn move_file_to_dir(filename: &str, dir: &String) -> FnResult<()> {
        let mut target_path = PathBuf::from(dir);
        target_path.push(Path::new(&filename).file_name().unwrap()); // assume that the filename does not end in `..` because we got it from a directory listing
        std::fs::rename(filename, target_path)?;
        Ok(())
    }
}