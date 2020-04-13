extern crate datafusion;
extern crate arrow;

pub mod schema;

use std::io;
use std::vec::Vec;
use std::sync::Arc;
use std::path::Path;
use std::fs::{remove_file,OpenOptions};
use std::collections::HashMap;


use datafusion::execution::context::ExecutionContext;
use datafusion::error::Result;
use datafusion::execution::physical_plan::ExecutionPlan;
use datafusion::utils;
use arrow::record_batch::RecordBatch;
use arrow::array::{Float64Array, StringArray, UInt16Array, UInt64Array};

use std::error::Error;
use csv::Writer;


use log;

#[derive(Debug, Serialize, Deserialize)]
pub struct StateResult {
    Estado: String,
    B70: u64,
    B80: u64,
    B90: u64,
    B00: u64,
    Race70: u64,
    Race80: u64,
    Race90: u64,
    Race00: u64,
    Male: u64,
    Female: u64,
    Weight: f64,
}

/*
Estado (string)
B70: Nacimientos en la decada los 70 en ese estado (number)
B80: Nacimientos en la decada los 80 en ese estado (number)
B90: Nacimientos en la decada los 90 en ese estado (number)
B00: Nacimientos en la decada los 2000 en ese estado (number)
Race70: Raza con mayor número de nacimientos en la decada de los 70 en ese estado (string)
Race80: Raza con mayor número de nacimientos en la decada de los 80 en ese estado (string)
Race90: Raza con mayor número de nacimientos en la decada de los 90 en ese estado(string)
Race00: Raza con mayor número de nacimientos en la decada de los 2000 en ese estado (string)
Male: Numero de nacimientos de hombres en los desde el 70 al 2010 (number)
Female: Numero de nacimientos de hombres en los desde el 70 al 2010 (number)
Weight: peso medio en kilos de todos los niños nacidos en ese estado desde el 70 al 2010 (float)
*/

static FILENAME: &str = "natalidad000000000000";
//static FILENAME: &str = "natalidad";

pub fn create_execution_environment(datadir: &str) -> Result<ExecutionContext> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // register csv file with the execution context
    debug!("Reading file {}", &format!("{}/{}.csv", datadir,FILENAME));
        ctx.register_csv(
        "natalidad",
//        &format!("{}/{}.csv", datadir, FILENAME),
        &format!("{}/", datadir),
        &schema::create_schema(),
        true,
    );
    Ok(ctx)
}

pub fn execute_query(query: &str, ctx: &mut ExecutionContext, batchsize: usize) -> Result<Vec<RecordBatch>> {
    // create the query plan
    let plan = ctx.create_logical_plan(&query)?;
    let plan = ctx.optimize(&plan)?;
    let plan = ctx.create_physical_plan(&plan, batchsize)?;

    ctx.collect(plan.as_ref())
}

pub fn process_objectives( ctx: &mut ExecutionContext, batchsize: usize) -> Result<HashMap<String,StateResult>> {
    let mut objectives: HashMap<String, StateResult> = HashMap::new();
    b70(ctx, batchsize, &mut objectives);
    b80(ctx, batchsize, &mut objectives);
    b90(ctx, batchsize, &mut objectives);
    b00(ctx, batchsize, &mut objectives);

/*
    race70(ctx, batchsize, &mut objectives);
    race80(ctx, batchsize, &mut objectives);
    race90(ctx, batchsize, &mut objectives);
    race00(ctx, batchsize, &mut objectives);
*/

    debug!("Calculate by sex");
    let (male,female) = bysex(ctx, batchsize);
    let avg = weight(ctx, batchsize);
    for (state, stateObjective) in &mut objectives {
        debug!("State {}", state);
        stateObjective.Female = female;
        stateObjective.Male = male;
        stateObjective.Weight = avg;
    }
/*

*/


    /*
        Use derived implementation to write results
    */
    debug!("Writing file");
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&format!("result_{}.csv", "gaguilar"))
            .unwrap();

        let mut wtr = Writer::from_writer(file);
        for (state, stateObjective) in &objectives {
            wtr.serialize(stateObjective).unwrap();
        }
        wtr.flush();
    }
    Ok(objectives)
}

/*
 * Objectives
 */
pub fn b70(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
//    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY mother_residence_state";
    let sql = "SELECT mother_residence_state, COUNT(*) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY mother_residence_state";
    let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult =objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: count.value(i),
                    B80: 0,
                    B90: 0,
                    B00: 0,
                    Race70: 0,
                    Race80: 0,
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.B70 = count.value(i);

            }
        });

}

pub fn b80(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
//    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1980 and year<1990 GROUP BY mother_residence_state";
    let sql = "SELECT mother_residence_state, COUNT(*) FROM natalidad WHERE year>=1980 and year<1990 GROUP BY mother_residence_state";
    let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: count.value(i),
                    B90: 0,
                    B00: 0,
                    Race70: 0,
                    Race80: 0,
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.B80 = count.value(i);

            }
        });
}

pub fn b90(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
//    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1990 and year<2000 GROUP BY mother_residence_state";
    let sql = "SELECT mother_residence_state, COUNT(*) FROM natalidad WHERE year>=1990 and year<2000 GROUP BY mother_residence_state";
//    exec_and_print(sql, ctx, batchsize);
   let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: count.value(i),
                    B00: 0,
                    Race70: 0,
                    Race80: 0,
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.B90 = count.value(i);

            }
        });
}

pub fn b00(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
//    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=2000 and year<2010 GROUP BY mother_residence_state";
    let sql = "SELECT mother_residence_state, COUNT(*) FROM natalidad WHERE year>=2000 and year<2010 GROUP BY mother_residence_state";
//    exec_and_print(sql, ctx, batchsize);
 let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: 0,
                    B00: count.value(i),
                    Race70: 0,
                    Race80: 0,
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.B00 = count.value(i);

            }
        });
}

pub fn race70(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
    let sql = "SELECT child_race, COUNT(child_race) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
    /*
    let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: 0,
                    B00: 0,
                    Race70: count.value(i),
                    Race80: 0,
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.Race70 = count.value(i);

            }
        });
        */
}

pub fn race80(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=1980 and year<1990 GROUP BY child_race";

    exec_and_print(sql, ctx, batchsize);
    /*
        let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: 0,
                    B00: 0,
                    Race70: 0,
                    Race80: count.value(i),
                    Race90: 0,
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.Race80 = count.value(i);

            }
        });*/
}

pub fn race90(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=1990 and year<2000 GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
    /*
     let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: 0,
                    B00: 0,
                    Race70: 0,
                    Race80: 0,
                    Race90: count.value(i),
                    Race00: 0,
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.Race90 = count.value(i);

            }
        });
        */
}

pub fn race00(ctx: &mut ExecutionContext, batchsize: usize, objectives: &mut HashMap<String,StateResult>){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=2000 and year<2010  GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
    /*
     let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let state = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "State {}, Births: {}",
                    state.value(i),
                    count.value(i),
                );
                let stateResult = objectives.entry(state.value(i).to_string()).or_insert(StateResult{
                    Estado: state.value(i).to_string(),
                    B70: 0,
                    B80: 0,
                    B90: 0,
                    B00: 0,
                    Race70: 0,
                    Race80: 0,
                    Race90: 0,
                    Race00: count.value(i),
                    Male: 0,
                    Female: 0,
                    Weight: 0.0,
                });
                stateResult.Race00 = count.value(i);

            }
        });
        */
}

pub fn bysex(ctx: &mut ExecutionContext, batchsize: usize) -> (u64,u64){
    let sql = "SELECT is_male, COUNT(*) FROM natalidad WHERE year>=1970 and year<2010 GROUP BY is_male";
    let mut males=0;
    let mut females=0;
    let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let male = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                debug!(
                    "Male {}, Births: {}",
                    male.value(i),
                    count.value(i),
                );
                if male.value(i).to_string()=="false" {
                    females = count.value(i);
                }else{
                    males = count.value(i);
                }

            }
        });
        (males,females)
}

pub fn weight(ctx: &mut ExecutionContext, batchsize: usize) -> f64 {
    let sql = "SELECT avg(weight_pounds) FROM natalidad WHERE year>=1970 and year<2010";
    let mut avg: f64=0.0;
    let results = execute_query(sql, ctx, batchsize).unwrap();
    results.iter().for_each(|batch| {
            debug!(
                "RecordBatch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            let avgarray = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                avg = avgarray.value(i);
            }
        });
    avg
}


fn exec_and_print(sql: &str, ctx: &mut ExecutionContext, batchsize: usize){
    // execute the query
    let results = execute_query(sql, ctx, batchsize).unwrap();
    utils::print_batches(&results).unwrap();
}





#[test]
fn should_write_csv() -> Result<()> {
    let mut wtr = Writer::from_writer(vec![]);
    wtr.serialize(StateResult {
        Estado: "VA".to_string(),
        B70: 1,
        B80: 2,
        B90: 3,
        B00: 4,
        Race70: 5,
        Race80: 6,
        Race90: 7,
        Race00: 8,
        Male: 9,
        Female: 10,
        Weight: 11.1,
    }).unwrap();
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap_or("Error".to_string());
    debug!("Serialized [{}]", data);
    assert_eq!(data, "Estado,B70,B80,B90,B00,Race70,Race80,Race90,Race00,Male,Female,Weight\nVA,1,2,3,4,5,6,7,8,9,10,11.1\n");
    Ok(())
}

#[test]
fn should_write_csv_file() -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("test.csv")
        .unwrap();

    let mut wtr = Writer::from_writer(file);
    wtr.serialize(StateResult {
        Estado: "VA".to_string(),
        B70: 1,
        B80: 2,
        B90: 3,
        B00: 4,
        Race70: 5,
        Race80: 6,
        Race90: 7,
        Race00: 8,
        Male: 9,
        Female: 10,
        Weight: 11.1,
    }).unwrap();
    if(Path::new("test.csv").exists()){
        remove_file("test.csv").unwrap();
        return Ok(());
    }
    assert!(false);
    Ok(())
}

