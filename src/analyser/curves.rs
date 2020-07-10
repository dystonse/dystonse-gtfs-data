use clap::ArgMatches;

use dystonse_curves::tree::{SerdeFormat, NodeData};

use super::Analyser;
use crate::types::DelayStatistics;

use crate::{ FnResult, Main };

use super::{SpecificCurveCreator, DefaultCurveCreator};

pub struct CurveCreator<'a> {
    pub main: &'a Main,
    pub analyser:&'a Analyser<'a>,
    pub args: &'a ArgMatches
}

impl<'a> CurveCreator<'a> {

    pub fn run_curves(&self) -> FnResult<()> {
        let scc = SpecificCurveCreator {
            main: self.main,
            analyser: self.analyser,
            args: self.args, 
        };
        
        let dcc = DefaultCurveCreator {
            main: self.main,
            analyser: self.analyser,
            args: self.args, 
        };
        
        let delay_stats = DelayStatistics {
            specific: scc.get_specific_curves()?,
            general: dcc.get_default_curves()?
        };
       
        delay_stats.save_to_file(&self.analyser.main.dir, "all_curves", &SerdeFormat::MessagePack)?;
        Ok(())
    }

}