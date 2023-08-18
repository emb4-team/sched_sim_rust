use crate::util::create_yaml;
use chrono::{DateTime, Utc};

pub fn create_scheduler_log_yaml(dir_path: &str, alg_name: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
    let file_name = format!("{}-{}-log", date, alg_name);
    create_yaml(dir_path, &file_name)
}
