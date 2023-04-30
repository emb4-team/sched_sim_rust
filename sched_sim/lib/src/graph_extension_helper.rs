use crate::graph_extension::NodeData;
use petgraph::algo::toposort;
use petgraph::graph::Graph;
use petgraph::visit::EdgeRef;
use petgraph::Direction::Incoming;
use petgraph::Direction::Outgoing;

/// Calculate the earliest start times for each node in the DAG.
pub fn calculate_earliest_start_times(dag: &mut Graph<NodeData, f32>) -> Vec<f32> {
    let sorted_nodes = toposort(&*dag, None).unwrap();
    let mut earliest_start_times = vec![0.0; dag.node_count()];

    for node in sorted_nodes.iter() {
        let max_earliest_start_time = dag
            .edges_directed(*node, Incoming)
            .map(|edge| {
                let source_node = edge.source();
                let exe_time = dag[source_node].params["execution_time"];
                earliest_start_times[source_node.index()] + exe_time
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);

        earliest_start_times[node.index()] = max_earliest_start_time;
    }

    earliest_start_times
}
/// Calculate the latest start times for each node in the DAG.
pub fn calculate_latest_start_times(dag: &mut Graph<NodeData, f32>) -> Vec<f32> {
    let earliest_start_times = calculate_earliest_start_times(dag);
    let sorted_nodes = toposort(&*dag, None).unwrap();
    let node_count = dag.node_count();
    let mut latest_start_times = vec![f32::MAX; node_count];
    latest_start_times[node_count - 1] = earliest_start_times[node_count - 1];

    for &node in sorted_nodes.iter().rev() {
        let min_latest_start_time = dag
            .edges_directed(node, Outgoing)
            .map(|edge| {
                let target_node = edge.target();
                let pre_exe_time = dag[node].params["execution_time"];
                latest_start_times[target_node.index()] - pre_exe_time
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(earliest_start_times[node_count - 1]);

        latest_start_times[node.index()] = min_latest_start_time;
    }

    latest_start_times
}
