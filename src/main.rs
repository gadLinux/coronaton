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
use arrow::array::{Float64Array, StringArray, UInt16Array, UInt64Array};


fn main() {
    let matches = App::new("coronaton challenge - v1.0")
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
    let buffer_size = matches.value_of("batchsize").unwrap_or("1024").parse::<usize>().unwrap_or(1024);
    println!("Loading data from [{}]...", datadir);
    
    debug!("System initialized. Loading data from {}...", datadir);

    let sql = "SELECT mother_residence_state, year, COUNT(year) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY mother_residence_state";

    let mut ctx = coronalib::create_execution_environment(datadir).unwrap();
    {
        // execute the query
        let results = coronalib::execute_query(sql, &mut ctx, buffer_size).unwrap();
        
        // print the results
       // iterate over the results
        results.iter().for_each(|batch| {
            println!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            batch.schema().fields().iter().for_each(|field| {
            println!("type {:?}", field.data_type());
            });
            

            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let year = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let count = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                println!(
                    "State {}, Year {}, Births: {}",
                    state.value(i),
                    year.value(i),
                    count.value(i),
                );
            }
        });
    }


}
