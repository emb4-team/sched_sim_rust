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
use lib::util::adjust_to_implicit_deadline;

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

    let file_path = dynfed_scheduler.dump_log(&arg.output_dir_path, "FixedPriority");

    let mut result = true;

    'outer: for dag in dag_set.iter_mut() {
        for dag_schedule_length in schedule_length.iter() {
            for each_schedule_length in dag_schedule_length.iter() {
                if *each_schedule_length < dag.get_head_period().unwrap() {
                    result = false;
                    break 'outer;
                }
            }
        }
    }

    dump_dag_set_scheduler_result_to_yaml(&file_path, result);
}
