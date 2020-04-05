extern crate datafusion;
extern crate arrow;

pub mod schema;

use std::vec::Vec;
use std::sync::Arc;

use datafusion::execution::context::ExecutionContext;
use datafusion::error::Result;
use datafusion::execution::physical_plan::ExecutionPlan;
use arrow::record_batch::RecordBatch;
use log;

pub fn create_execution_environment(datadir: &str) -> Result<ExecutionContext> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // register csv file with the execution context
    debug!("Reading file {}", &format!("{}/natalidad000000000000.csv", datadir));
        ctx.register_csv(
        "natalidad",
        &format!("{}/natalidad000000000000.csv", datadir),
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
