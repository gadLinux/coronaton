extern crate datafusion;
extern crate arrow;

pub mod schema;

use std::vec::Vec;
use std::sync::Arc;

use datafusion::execution::context::ExecutionContext;
use datafusion::error::Result;
use datafusion::execution::physical_plan::ExecutionPlan;
use datafusion::utils;
use arrow::record_batch::RecordBatch;
use arrow::array::{Float64Array, StringArray, UInt16Array, UInt64Array};

use log;

//static FILENAME: &str = "natalidad000000000000";
static FILENAME: &str = "natalidad";

pub fn create_execution_environment(datadir: &str) -> Result<ExecutionContext> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // register csv file with the execution context
    debug!("Reading file {}", &format!("{}/{}.csv", datadir,FILENAME));
        ctx.register_csv(
        "natalidad",
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


/*
 * Objectives
 */
pub fn b70(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY mother_residence_state";
    exec_and_print(sql, ctx, batchsize);
}

pub fn b80(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1980 and year<1990 GROUP BY mother_residence_state";
    exec_and_print(sql, ctx, batchsize);
}

pub fn b90(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=1990 and year<2000 GROUP BY mother_residence_state";
    exec_and_print(sql, ctx, batchsize);
}

pub fn b00(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT mother_residence_state, SUM(plurality) FROM natalidad WHERE year>=2000 and year<2010 GROUP BY mother_residence_state";
    exec_and_print(sql, ctx, batchsize);
}

pub fn race70(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=1970 and year<1980 GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
}

pub fn race80(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=1980 and year<1990 GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
}

pub fn race90(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=1990 and year<2000 GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
}

pub fn race00(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT child_race, COUNT(*) FROM natalidad WHERE year>=2000 and year<2010  GROUP BY child_race";
    exec_and_print(sql, ctx, batchsize);
}

pub fn bysex(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT is_male, COUNT(*) FROM natalidad WHERE year>=1970 and year<2010 GROUP BY is_male";
    exec_and_print(sql, ctx, batchsize);
}

pub fn weight(ctx: &mut ExecutionContext, batchsize: usize){
    let sql = "SELECT avg(weight_pounds) FROM natalidad WHERE year>=1970 and year<2010";
    exec_and_print(sql, ctx, batchsize);
}


fn exec_and_print(sql: &str, ctx: &mut ExecutionContext, batchsize: usize){
    // execute the query
    let results = execute_query(sql, ctx, batchsize).unwrap();
    utils::print_batches(&results).unwrap();
}

