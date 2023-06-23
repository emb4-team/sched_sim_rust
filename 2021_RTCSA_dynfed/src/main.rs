mod dynfed;

use clap::Parser;
use dynfed::DynamicFederatedScheduler;
use lib::dag_creator::*;
use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::graph_extension::GraphExtension;
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use lib::scheduler::DAGSetSchedulerBase;

#[derive(Parser)]
#[clap(
    name = "DynFed_Algorithm",
    version = "1.0",
    about = "About:
    DynFed_Algorithm operates on the same assumption of cycles and deadlines.
    Therefore, the period shall be considered as the deadline.
    If there is no period, the deadline shall be obtained."
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

    for dag in dag_set.iter_mut() {
        if let (Some(end_to_end_deadline), Some(period)) =
            (dag.get_end_to_end_deadline(), dag.get_head_period())
        {
            assert_eq!(
                end_to_end_deadline, period,
                "end_to_end_deadline and period must be equal"
            );
        }
    }

    let homogeneous_processor = HomogeneousProcessor::new(arg.number_of_cores);
    let mut dynfed_scheduler: DynamicFederatedScheduler<
        FixedPriorityScheduler<HomogeneousProcessor>,
    > = DynamicFederatedScheduler::new(dag_set, homogeneous_processor);

    let _result = dynfed_scheduler.schedule();
}
