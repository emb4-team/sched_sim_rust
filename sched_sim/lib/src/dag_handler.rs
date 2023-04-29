use crate::dag_creator::{self, *};
use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f32::MAX;

fn create_dummy_node(id: i32) -> NodeData {
    let mut params = HashMap::new();
    params.insert(String::from("execution_time"), 0.0);

    NodeData { id, params }
}

fn connect_nodes(
    dag: &Graph<NodeData, f32>,
    new_dag: &mut Graph<NodeData, f32>,
    dummy_node_index: NodeIndex,
    direction: petgraph::Direction,
) {
    let nodes = dag.externals(direction).collect::<Vec<_>>();

    for node_index in nodes {
        match direction {
            Incoming => new_dag.add_edge(dummy_node_index, node_index, 0.0),
            Outgoing => new_dag.add_edge(node_index, dummy_node_index, 0.0),
        };
    }
}

fn add_dummy_nodes(dag: &Graph<NodeData, f32>) -> Graph<NodeData, f32> {
    let mut new_dag: Graph<dag_creator::NodeData, f32> = (*dag).clone();

    let start_node_index = new_dag.add_node(create_dummy_node(-1));
    let end_node_index = new_dag.add_node(create_dummy_node(-2));

    connect_nodes(
        dag,
        &mut new_dag,
        start_node_index,
        petgraph::Direction::Incoming,
    );
    connect_nodes(
        dag,
        &mut new_dag,
        end_node_index,
        petgraph::Direction::Outgoing,
    );
    new_dag
}

fn calculate_earliest_start_times(
    dag: &Graph<NodeData, f32>,
    sorted_nodes: &[NodeIndex],
) -> Vec<f32> {
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

fn calculate_latest_start_times(
    dag: &Graph<NodeData, f32>,
    sorted_nodes: &[NodeIndex],
    earliest_start_times: &[f32],
) -> Vec<f32> {
    let node_count = dag.node_count();
    let mut latest_start_times = vec![MAX; node_count];
    latest_start_times[node_count - 1] = earliest_start_times[node_count - 1];

    for &node in sorted_nodes.iter().rev() {
        let min_latest_start_time = dag
            .edges_directed(node, Outgoing)
            .map(|edge| {
                let source_node = edge.source();
                let target_node = edge.target();
                let pre_exe_time = dag[source_node].params["execution_time"];
                latest_start_times[target_node.index()] - pre_exe_time
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(earliest_start_times[node_count - 1]);

        latest_start_times[node.index()] = min_latest_start_time;
    }

    latest_start_times
}

fn find_critical_paths(
    dag: &Graph<NodeData, f32>,
    start_node: NodeIndex,
    earliest_start_times: &[f32],
    latest_start_times: &[f32],
) -> Vec<Vec<NodeIndex>> {
    let mut critical_paths = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back((start_node, vec![start_node]));

    while let Some((node, path)) = queue.pop_front() {
        let outgoing_edges = dag.edges_directed(node, petgraph::Direction::Outgoing);

        if outgoing_edges.clone().count() == 0 {
            critical_paths.push(path);
        } else {
            for edge in outgoing_edges {
                let target_node = edge.target();
                if earliest_start_times[target_node.index()]
                    == latest_start_times[target_node.index()]
                {
                    let mut new_path = path.clone();
                    new_path.push(target_node);
                    queue.push_back((target_node, new_path));
                }
            }
        }
    }

    critical_paths
}

fn _extract_critical_path_nodes(
    sorted_nodes: &[NodeIndex],
    earliest_start_times: &[f32],
    latest_start_times: &[f32],
) -> Vec<NodeIndex> {
    sorted_nodes
        .iter()
        .filter(|node| earliest_start_times[node.index()] == latest_start_times[node.index()])
        .cloned()
        .collect()
}

/// Returns the critical path of a DAG
///
/// # Arguments
///
/// * `dag` - dag object. each node contains execution time information.
///
/// # Returns
///
/// * `critical path` -containing the nodes in the critical path.
///
/// # Example
///
/// ```
/// use petgraph::Graph;
/// use lib::dag_creator::NodeData;
/// use lib::dag_handler::get_critical_paths;
///
/// let mut dag = Graph::<NodeData, f32>::new();
/// let critical_path = get_critical_paths(dag);
/// println!("The critical path is: {:?}", critical_path);
/// ```
pub fn get_critical_paths(dag: Graph<NodeData, f32>) -> Vec<Vec<NodeIndex>> {
    let dag = add_dummy_nodes(&dag);
    let sorted_nodes = toposort(&dag, None).unwrap();

    let earliest_start_times = calculate_earliest_start_times(&dag, &sorted_nodes);
    let latest_start_times =
        calculate_latest_start_times(&dag, &sorted_nodes, &earliest_start_times);

    let start_node = sorted_nodes[0];
    find_critical_paths(&dag, start_node, &earliest_start_times, &latest_start_times)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_critical_paths_multiple() {
        let dag = create_dag_from_yaml("../tests/sample_dags/multiple_critical_paths.yaml");
        let critical_path = get_critical_paths(dag);
        assert_eq!(
            critical_path[0]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![9_usize, 0_usize, 1_usize, 4_usize, 7_usize, 8_usize, 10_usize]
        );
        assert_eq!(
            critical_path[1]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![9_usize, 0_usize, 1_usize, 2_usize, 5_usize, 8_usize, 10_usize]
        );
    }
}
