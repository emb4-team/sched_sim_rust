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

fn connect_dummy_nodes(
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

    connect_dummy_nodes(dag, &mut new_dag, start_node_index, Incoming);
    connect_dummy_nodes(dag, &mut new_dag, end_node_index, Outgoing);
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
    let mut path_search_queue = VecDeque::new();
    path_search_queue.push_back((start_node, vec![start_node]));

    while let Some((node, mut critical_path)) = path_search_queue.pop_front() {
        let outgoing_edges = dag.edges_directed(node, Outgoing);

        if outgoing_edges.clone().count() == 0 {
            critical_path.pop();
            critical_path.remove(0);
            critical_paths.push(critical_path);
        } else {
            for edge in outgoing_edges {
                let target_node = edge.target();
                if earliest_start_times[target_node.index()]
                    == latest_start_times[target_node.index()]
                {
                    let mut current_critical_path = critical_path.clone();
                    current_critical_path.push(target_node);
                    path_search_queue.push_back((target_node, current_critical_path));
                }
            }
        }
    }

    critical_paths
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
/// use std::collections::HashMap;
///
/// let mut dag = Graph::<NodeData, f32>::new();
/// let mut params = HashMap::new();
/// params.insert("execution_time".to_string(), 1.0);
/// let n0 = dag.add_node(NodeData { id: 0, params: params.clone() });
/// let n1 = dag.add_node(NodeData { id: 1, params: params.clone() });
/// dag.add_edge(n0, n1, 1.0);
///
/// let critical_path = get_critical_paths(dag);
/// println!("The critical path is: {:?}", critical_path);
/// ```
pub fn get_critical_paths(dag: Graph<NodeData, f32>) -> Vec<Vec<NodeIndex>> {
    let dag = add_dummy_nodes(&dag);
    let sorted_nodes = toposort(&dag, None).unwrap();
    let start_node = sorted_nodes[0];

    let earliest_start_times = calculate_earliest_start_times(&dag, &sorted_nodes);
    let latest_start_times =
        calculate_latest_start_times(&dag, &sorted_nodes, &earliest_start_times);

    find_critical_paths(&dag, start_node, &earliest_start_times, &latest_start_times)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_critical_paths_multiple() {
        fn create_node(id: i32, key: &str, value: f32) -> NodeData {
            let mut params = HashMap::new();
            params.insert(key.to_string(), value);
            NodeData { id, params }
        }
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 45.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 26.0));
        let n4 = dag.add_node(create_node(4, "execution_time", 44.0));
        let n5 = dag.add_node(create_node(5, "execution_time", 26.0));
        let n6 = dag.add_node(create_node(6, "execution_time", 26.0));
        let n7 = dag.add_node(create_node(7, "execution_time", 27.0));
        let n8 = dag.add_node(create_node(8, "execution_time", 43.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n1, n2, 1.0);
        dag.add_edge(n1, n3, 1.0);
        dag.add_edge(n1, n4, 1.0);
        dag.add_edge(n2, n5, 1.0);
        dag.add_edge(n3, n6, 1.0);
        dag.add_edge(n4, n7, 1.0);
        dag.add_edge(n5, n8, 1.0);
        dag.add_edge(n6, n8, 1.0);
        dag.add_edge(n7, n8, 1.0);

        let critical_path = get_critical_paths(dag);
        assert_eq!(
            critical_path[0]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![0_usize, 1_usize, 4_usize, 7_usize, 8_usize]
        );
        assert_eq!(
            critical_path[1]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![0_usize, 1_usize, 2_usize, 5_usize, 8_usize]
        );
    }
}
