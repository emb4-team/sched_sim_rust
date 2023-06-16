use chrono::{DateTime, Utc};
use clap::Parser;
mod outputs_result;
mod parallel_provider_consumer;
mod prioritization_cpc_model;

use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::homogeneous::HomogeneousProcessor;
use lib::output_log::*;
use lib::processor::ProcessorBase;
use lib::scheduler::SchedulerBase;
use lib::{dag_creator::*, graph_extension::GraphExtension};
use outputs_result::dump_cpc_result_to_file;

/// Application description and arguments definition using clap crate
#[derive(Parser)]
#[clap()]
/// Application arguments definition using clap crate
struct ArgParser {
    #[clap(short = 'f', long = "dag_file_path", required = true)]
    dag_file_path: String,
    #[clap(short = 'c', long = "number_of_cores", required = true)]
    number_of_cores: usize,
    #[clap(short = 'o', long = "output_dir_path", default_value = "../outputs")]
    output_dir_path: String,
}
fn main() {
    let arg: ArgParser = ArgParser::parse();
    let mut dag = create_dag_from_yaml(&arg.dag_file_path);
    let homogeneous_processor = HomogeneousProcessor::new(arg.number_of_cores);
    prioritization_cpc_model::assign_priority_to_cpc_model(&mut dag);
    let mut fixed_priority_scheduler = FixedPriorityScheduler::new(&dag, &homogeneous_processor);

    let (schedule_length, _) = fixed_priority_scheduler.schedule();
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
    let file_name = format!("{}-cpc-result", date);
    let file_path = create_yaml_file(&arg.output_dir_path, &file_name);
    dump_dag_set_info_to_yaml(&file_path, vec![dag.clone()]);
    dump_processor_info_to_yaml(&file_path, homogeneous_processor);

    let result = schedule_length < dag.get_head_period().unwrap();

    dump_cpc_result_to_file(&file_path, schedule_length, result);
    dump_processor_log_to_yaml(&file_path, fixed_priority_scheduler.processor_log);
    dump_node_logs_to_yaml(&file_path, fixed_priority_scheduler.node_logs);
}
