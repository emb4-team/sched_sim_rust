mod dynfed;

use clap::Parser;
use dynfed::DynamicFederatedScheduler;
use lib::dag_creator::*;
use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::graph_extension::GraphExtension;
use lib::homogeneous::HomogeneousProcessor;
use lib::log::dump_dag_set_scheduler_result_to_yaml;
use lib::processor::ProcessorBase;
use lib::scheduler::DAGSetSchedulerBase;
use lib::util::{adjust_to_implicit_deadline, get_hyper_period, load_yaml};

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

    dynfed_scheduler.schedule();
    let file_path = dynfed_scheduler.dump_log(&arg.output_dir_path, "FixedPriority");

    // Check the result
    let yaml_doc = &load_yaml(&file_path)[0];
    let dag_set_log = &yaml_doc["dag_set_log"];
    let hyper_period = get_hyper_period(&dag_set);
    let mut result = true;
    'outer: for dag_id in 0..dag_set.len() {
        let dag_period = dag_set[dag_id].get_head_period().unwrap();
        let dag_log = &dag_set_log[dag_id]["finish_time"];
        for release_count in 0..(hyper_period / dag_period) {
            let dag_finish_time = dag_log[dag_id]["finish_time"][release_count as usize]
                .as_i64()
                .unwrap();
            result = result && dag_finish_time <= (dag_period * (release_count + 1)) as i64;
            // If result is false, break the outer loop early using the 'outer label
            if !result {
                break 'outer;
            }
        }
    }

    dump_dag_set_scheduler_result_to_yaml(&file_path, result);
}
