mod dynfed;

use clap::Parser;
use dynfed::DynamicFederatedScheduler;
use lib::dag_creator::*;
use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::graph_extension::GraphExtension;
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use lib::scheduler::DAGSetSchedulerBase;
use log::warn;

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

    for dag in dag_set.iter_mut() {
        if let (Some(period), Some(end_to_end_deadline)) =
            (dag.get_head_period(), dag.get_end_to_end_deadline())
        {
            if end_to_end_deadline != period {
                warn!("In this algorithm, the period and the end-to-end deadline must be equal. Therefore, the end-to-end deadline is overridden by the period.");
            }
            let source_nodes = dag.get_source_nodes();
            let node_i = source_nodes
                .iter()
                .find(|&&node_i| dag[node_i].params.get("end_to_end_deadline").is_some())
                .unwrap();

            dag.update_param(*node_i, "end_to_end_deadline", period)
        }
    }

    let homogeneous_processor = HomogeneousProcessor::new(arg.number_of_cores);
    let mut dynfed_scheduler: DynamicFederatedScheduler<
        FixedPriorityScheduler<HomogeneousProcessor>,
    > = DynamicFederatedScheduler::new(&dag_set, &homogeneous_processor);

    let _result = dynfed_scheduler.schedule(); //TODO: use result
}
