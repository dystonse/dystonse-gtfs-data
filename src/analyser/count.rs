use mysql::*;
use mysql::prelude::*;
use parse_duration::parse;
use simple_error::SimpleError;

use super::Analyser;

use crate::FnResult;
use crate::read_dir_simple;

use std::fs;

pub fn run_count(analyser: &Analyser) -> FnResult<()> {
    let imported_dir = format!("{}/imported", &analyser.main.dir);
    let rt_filenames = read_dir_simple(&imported_dir)?;

    if rt_filenames.is_empty() {
        return Err(Box::from(SimpleError::new("No realtime data.")));
    }

    let mut con = analyser.main.pool.get_conn()?;
    let (start, end): (mysql::chrono::NaiveDateTime, mysql::chrono::NaiveDateTime) = con
        .exec_first("SELECT MIN(time_of_recording), MAX(time_of_recording) FROM records WHERE `source` = ?", (&analyser.main.source,))?
        .unwrap();

    let std_date = parse(
        analyser.args
            .subcommand_matches("count")
            .unwrap()
            .value_of("interval")
            .unwrap(),
    )?;
    let step: chrono::Duration = chrono::Duration::from_std(std_date)?;
    let mut time_min = start;
    let mut time_max = start + step;
    println!(
        "time_min; time_max; stop time update count; average delay; rt file count; rt file size"
    );
    loop {
        let mut rt_file_count = 0;
        let mut rt_file_size = 0;
        let row: mysql::Row = con
            .exec_first(
                "SELECT COUNT(*), AVG(delay_arrival) 
                FROM records 
                WHERE (`time_of_recording` BETWEEN ? AND ?) 
                AND (delay_arrival BETWEEN - 36000 AND 36000) 
                AND source = ?",
                (time_min, time_max, &analyser.main.source),
            )?
            .unwrap();
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