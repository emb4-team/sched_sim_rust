use chrono::{DateTime, Utc};
use log::{info, warn};
use std::fs::{self, OpenOptions};
use std::io::Write;

fn create_yaml_file_core(folder_path: &str, file_name: &str) -> String {
    if fs::metadata(folder_path).is_err() {
        let _ = fs::create_dir_all(folder_path);
        info!("Created folder: {}", folder_path);
    }
    let file_path = format!("{}/{}.yaml", folder_path, file_name);
    if let Err(err) = fs::File::create(&file_path) {
        warn!("Failed to create file: {}", err);
    }
    file_path
}

pub fn create_scheduler_log_yaml_file(folder_path: &str, sched_name: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
    let file_name = format!("{}-{}-log", date, sched_name);
    create_yaml_file_core(folder_path, &file_name)
}

pub fn append_info_to_yaml(file_path: &str, info: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
    {
        if let Err(err) = file.write_all(info.as_bytes()) {
            eprintln!("Failed to write to file: {}", err);
        }
    } else {
        eprintln!("Failed to open file: {}", file_path);
    }
}
