mod dynfed;
mod outputs_result;

use clap::Parser;
use dynfed::DynamicFederatedScheduler;
use lib::dag_creator::*;
use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::output_log::*;
use lib::processor::ProcessorBase;
use lib::scheduler::DAGSetSchedulerBase;
use lib::util::get_hyper_period;
use log::warn;
use outputs_result::dump_dynfed_result_to_file;
use petgraph::graph::NodeIndex;
use petgraph::Graph;

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

pub fn adjust_dag_set(dag_set: &mut [Graph<NodeData, i32>]) {
    for dag in dag_set.iter_mut() {
        adjust_dag(dag);
    }
}

fn adjust_dag(dag: &mut Graph<NodeData, i32>) {
    if let (Some(period), Some(end_to_end_deadline)) =
        (dag.get_head_period(), dag.get_end_to_end_deadline())
    {
        handle_equal_period_and_deadline(dag, period, end_to_end_deadline);
    } else if dag.get_head_period().is_none() {
        handle_missing_period(dag);
    } else if dag.get_end_to_end_deadline().is_none() {
        handle_missing_deadline(dag);
    }
}

fn handle_equal_period_and_deadline(
    dag: &mut Graph<NodeData, i32>,
    period: i32,
    end_to_end_deadline: i32,
) {
    if end_to_end_deadline != period {
        warn!("In this algorithm, the period and the end-to-end deadline must be equal. Therefore, the end-to-end deadline is overridden by the period.");
    }
    let source_nodes = dag.get_source_nodes();
    let node_i = source_nodes
        .iter()
        .find(|&&node_i| dag[node_i].params.get("end_to_end_deadline").is_some())
        .unwrap();
    dag.update_param(*node_i, "end_to_end_deadline", period);
}

fn handle_missing_period(dag: &mut Graph<NodeData, i32>) {
    let end_to_end_deadline = dag
        .get_end_to_end_deadline()
        .expect("Either an end-to-end deadline or period of time is required for the schedule.");
    dag.add_param(NodeIndex::new(0), "period", end_to_end_deadline);
}

fn handle_missing_deadline(dag: &mut Graph<NodeData, i32>) {
    let period = dag
        .get_head_period()
        .expect("Either an end-to-end deadline or period of time is required for the schedule.");
    dag.add_param(
        NodeIndex::new(dag.node_count() - 1),
        "end_to_end_deadline",
        period,
    );
}

fn main() {
    let arg: ArgParser = ArgParser::parse();
    let mut dag_set = create_dag_set_from_dir(&arg.dag_dir_path);

    adjust_dag_set(&mut dag_set);

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
