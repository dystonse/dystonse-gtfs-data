use clap::{App, ArgMatches};
use crate::FnResult;
use crate::Main;

pub struct Analyser<'a> {
    #[allow(dead_code)]
    main: &'a Main,
    args: &'a ArgMatches,
}

impl<'a> Analyser<'a> {
    pub fn get_subcommand() -> App<'a> {
        App::new("analyse")
    }

    pub fn new(main: &'a Main, args: &'a ArgMatches) -> Analyser<'a> {
        Analyser {
            main,
            args
        }
    }

    /// Runs the actions that are selected via the command line args
    pub fn run(&mut self) -> FnResult<()> {
        match self.args.clone().subcommand() {
            // ("automatic", Some(sub_args)) => {
            //     self.set_dir_paths(sub_args)?;
            //     self.run_as_non_manual(true)
            // }
            // ("batch", Some(sub_args)) => {
            //     self.set_dir_paths(sub_args)?;
            //     self.run_as_non_manual(false)
            // }
            // ("manual", Some(sub_args)) => self.run_as_manual(sub_args),
            _ => panic!("Invalid arguments."),
        }
    }
}