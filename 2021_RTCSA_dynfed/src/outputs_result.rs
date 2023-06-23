use lib::output_log::append_info_to_yaml;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ResultInfo {
    schedule_length: i32,
    hyper_period: i32,
    result: bool,
}

pub fn dump_dynfed_result_to_file(
    file_path: &str,
    schedule_length: i32,
    hyper_period: i32,
    result: bool,
) {
    let result_info = ResultInfo {
        schedule_length,
        hyper_period,
        result,
    };
    let yaml =
        serde_yaml::to_string(&result_info).expect("Failed to serialize federated result to YAML");

    append_info_to_yaml(file_path, &yaml);
}
