use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

pub fn create_schema() -> Schema {
    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("source_year", DataType::UInt16, false),
        Field::new("year", DataType::UInt16, false),
        Field::new("month", DataType::UInt8, false),
        Field::new("day", DataType::UInt8, false),
        Field::new("wday", DataType::UInt16, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("is_male", DataType::Utf8, false),
        Field::new("child_race", DataType::UInt8, false),
        Field::new("weight_pounds", DataType::Float32, false),
        Field::new("plurality", DataType::UInt8, false),
        Field::new("apgar_1min", DataType::UInt8, false),
        Field::new("apgar_5min", DataType::UInt8, false),
        Field::new("mother_residence_state", DataType::Utf8, false),
        Field::new("mother_race", DataType::UInt8, false),
        Field::new("mother_age", DataType::UInt8, false),
        Field::new("gestation_weeks", DataType::UInt8, false),
        Field::new("lmp", DataType::Utf8, false),
        Field::new("mother_married", DataType::Utf8, false),
        Field::new("mother_birth_state", DataType::Utf8, false),
        Field::new("cigarette_use", DataType::Utf8, false),
        Field::new("cigarette_per_day", DataType::UInt8, false),
        Field::new("alcohol_use", DataType::Utf8, false),
        Field::new("drinks_per_wek", DataType::UInt8, false),
        Field::new("weight_gain_pounds", DataType::UInt8, false),
        Field::new("born_alive_alive", DataType::UInt8, false),
        Field::new("born_alive_dead", DataType::UInt8, false),
        Field::new("born_dead", DataType::UInt8, false),
        Field::new("ever_born", DataType::UInt8, false),
        Field::new("father_race", DataType::UInt8, false),
        Field::new("father_age", DataType::UInt8, false),
        Field::new("record_weight", DataType::UInt8, false)
    ]);
    return schema;
}
