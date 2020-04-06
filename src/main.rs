#![warn(missing_docs)]
#![feature(decl_macro, proc_macro_hygiene)]

#[macro_use] extern crate log;
extern crate env_logger;
extern crate arrow;
extern crate datafusion;

#[macro_use]
extern crate clap;
use std::error::Error;
use clap::{Arg, App, SubCommand};

pub mod coronalib;


fn main() {
    let matches = App::new("Coronaton challenge")
      .version("1.0")
      .author("Gonzalo Aguilar Delgado <gaguilar@level2crm.com>")
      .about("Rust arrow tool for the coronaton challenge")
      .arg(Arg::with_name("datadir")
           .short("d")
           .long("data")
           .value_name("DIR")
           .help("Sets the data directory path"))

      .arg(Arg::with_name("batchsize")
           .short("b")
           .long("batchsize")
           .value_name("1024")
           .help("Sets batch size in bytes"))           
/*
      .arg(Arg::with_name("input_file")
           .help("Sets the input file to use")
           .required(true)
           .index(1))

     .arg(Arg::with_name("v")
           .short("v")
           .multiple(true)
           .help("Sets the level of verbosity"))
      .subcommand(SubCommand::with_name("test")
                  .about("controls testing features")
                  .version("1.3")
                  .author("Someone E. <someone_else@other.com>")
                  .arg(Arg::with_name("debug")
                      .short("d")
                      .help("print debug information verbosely")))
*/
      .get_matches();
    env_logger::init();
    
    let datadir = matches.value_of("datadir").unwrap_or("/data");
    let batchsize = matches.value_of("batchsize").unwrap_or("1024").parse::<usize>().unwrap_or(1024);
    println!("Loading data from [{}]...", datadir);
    
    debug!("System initialized. Loading data from {}...", datadir);


    let mut ctx = coronalib::create_execution_environment(datadir).unwrap();

    coronalib::b70(&mut ctx, batchsize);
    coronalib::b80(&mut ctx, batchsize);
    coronalib::b90(&mut ctx, batchsize);
    coronalib::b00(&mut ctx, batchsize);
    coronalib::race70(&mut ctx, batchsize);
    coronalib::race80(&mut ctx, batchsize);
    coronalib::race90(&mut ctx, batchsize);
    coronalib::race00(&mut ctx, batchsize);
    coronalib::bysex(&mut ctx, batchsize);
    coronalib::weight(&mut ctx, batchsize);
}
