use crate::importer::Importer;
use crate::FnResult;
use crate::Main;
use chrono::NaiveDateTime;
use clap::{App, Arg, ArgMatches};
use mysql::prelude::*;
use mysql::time::Duration;
use regex::Regex;
use simple_error::SimpleError;
use std::fs;
use std::str::FromStr;

pub struct Analyser<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
    data_dir: Option<String>,
}

impl<'a> Analyser<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("analyse")
            .subcommand(App::new("count"))
        .arg(Arg::with_name("dir")
                .index(1)
                .value_name("DIRECTORY")
                .required_unless("help")
                .help("The directory which contains schedules and realtime data")
                .long_help(
                    "The directory that contains the schedules (located in a subdirectory named 'schedules') \
                    and realtime data (located in a subdirectory named 'rt')."
                )
            )
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Analyser<'a> {
        Analyser {
            main,
            args,
            data_dir: Some(String::from(
                args.value_of("dir")
                    .unwrap(),
            )),
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            ("count", Some(_sub_args)) => self.run_count(),
            // ("batch", Some(sub_args)) => {
            //     self.set_dir_paths(sub_args)?;
            //     self.run_as_non_manual(false)
            // }
            // ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }

    pub fn date_time_from_filename(filename: &str) -> FnResult<NaiveDateTime> {
        lazy_static! {
            static ref FIND_DATE: Regex = Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})").unwrap(); // can't fail because our hard-coded regex is known to be ok
        }
        let date_element_captures =
            FIND_DATE
                .captures(&filename)
                .ok_or(SimpleError::new(format!(
                "File name does not contain a valid date (does not match format YYYY-MM-DD): {}",
                filename
            )))?;
        Ok(NaiveDateTime::from_str(&date_element_captures[1])?)
    }

    fn run_count(&self) -> FnResult<()> {
        let imported_dir = format!("{}/imported", &self.data_dir.as_ref().unwrap());
        let rt_filenames = Importer::read_dir_simple(&imported_dir)?;

        if rt_filenames.is_empty() {
            return Err(Box::from(SimpleError::new("No realtime data.")));
        }

        let mut con = self.main.pool.get_conn()?;
        let (start, end): (mysql::chrono::NaiveDateTime, mysql::chrono::NaiveDateTime) = con
            .query_first("SELECT MIN(time_of_recording), MAX(time_of_recording) from realtime")?
            .unwrap();

        let step = Duration::hours(1);
        let mut time_min = start;
        let mut time_max = start + step;
        println!(
            "time_min; time_max; trip update count; average delay; rt file count; rt file size"
        );
        loop {
            let mut rt_file_count = 0;
            let mut rt_file_size = 0;
            let row : mysql::Row = con.exec_first(
                "SELECT COUNT(*), AVG(delay_arrival) 
                FROM realtime 
                WHERE (`time_of_recording` BETWEEN ? AND ?) 
                AND (delay_arrival BETWEEN - 36000 AND 36000) 
                AND source = ?", 
                (time_min, time_max, &self.main.source))?.unwrap();
            let count: i32 = row.get(0).unwrap();
            let delay: f32 = row.get_opt(1).unwrap().unwrap_or(-1.0);
            // println!("Between {} and {} there are {} delay values, average is {} seconds.", time_min, time_max, count, delay);

            for rt_filename in &rt_filenames {
                let rt_date = Analyser::date_time_from_filename(&rt_filename).unwrap();
                if rt_date > time_min && rt_date < time_max {
                    rt_file_count += 1;
                    rt_file_size += fs::metadata(&rt_filename)?.len();
                }
            }

            println!(
                "{}; {}; {}; {}; {}; {}",
                time_min, time_max, count, delay, rt_file_count, rt_file_size
            );
            time_min = time_max;
            time_max = time_min + step;
            if time_max > end {
                break;
            }
        }

        Ok(())
    }
}
