mod dynfed;
mod outputs_result;

use clap::Parser;
use dynfed::DynamicFederatedScheduler;
use lib::dag_creator::*;
use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::homogeneous::HomogeneousProcessor;
use lib::output_log::*;
use lib::processor::ProcessorBase;
use lib::scheduler::DAGSetSchedulerBase;
use lib::util::{adjust_to_implicit_deadline, get_hyper_period};
use outputs_result::dump_dynfed_result_to_file;

#[derive(Parser)]
#[clap(
    name = "DynFed_Algorithm",
    version = "1.0",
    about = "About:
    DynFed_Algorithm operates on the same assumption of period and end_to_end_deadline.
    Therefore, the period shall be considered as the end_to_end_deadline.
    If there is no period, the end_to_end_deadline shall be obtained."
)]
struct ArgParser {
    ///Path to DAGSet directory.
    #[clap(short = 'd', long = "dag_dir_path", required = true)]
    dag_dir_path: String,
    ///Number of processing cores.
    #[clap(short = 'c', long = "number_of_cores", required = true)]
    number_of_cores: usize,
    ///Path to output directory.
    #[clap(short = 'o', long = "output_dir_path", default_value = "../outputs")]
    output_dir_path: String,
}

fn main() {
    let arg: ArgParser = ArgParser::parse();
    let mut dag_set = create_dag_set_from_dir(&arg.dag_dir_path);

    adjust_to_implicit_deadline(&mut dag_set);

    let homogeneous_processor = HomogeneousProcessor::new(arg.number_of_cores);
    let mut dynfed_scheduler: DynamicFederatedScheduler<
        FixedPriorityScheduler<HomogeneousProcessor>,
    > = DynamicFederatedScheduler::new(&dag_set, &homogeneous_processor);

    let schedule_length = dynfed_scheduler.schedule();

    let file_path = create_scheduler_log_yaml_file(&arg.output_dir_path, "dynfed");

    dump_dynfed_result_to_file(
        &file_path,
        schedule_length,
        get_hyper_period(&dag_set),
        schedule_length < get_hyper_period(&dag_set),
    );

    dump_dag_set_info_to_yaml(&file_path, dag_set);
    dump_processor_info_to_yaml(&file_path, homogeneous_processor);
    dump_dag_set_log_to_yaml(&file_path, dynfed_scheduler.dag_set_log);
    dump_node_set_logs_to_yaml(&file_path, dynfed_scheduler.node_logs);
    dump_processor_log_to_yaml(&file_path, dynfed_scheduler.processor_log);
}
