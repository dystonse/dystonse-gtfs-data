use serde::Deserialize;
use std::ffi::OsString;
use std::collections::HashMap;
use std::fs::File;
use std::error::Error;

#[derive(Debug, Deserialize)]
pub struct Route {
    pub route_id: String,
    pub agency_id: u32,
    pub route_short_name: String,
    pub route_type: u32,
    // Don't read fields that we won't need anyway
    // route_long_name: String,
    // route_color: String,
    // route_text_color: String,
    // route_desc: String
}

/// Reads the csv file at the given path and puts each Route it reads into routes, using the route_id as the key.
pub fn read_csv(file_path: OsString, routes: &mut HashMap<String, Route>) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.deserialize() {
        // we need to put this into a var because the compiler needs a type annotation for Route
        let route: Route = result?;

        // we clone route.route_id because we can't pass its ownership to the map, 
        // and references don't work either for some reson 
        routes.insert(route.route_id.clone(), route);
    }

    Ok(())
}
