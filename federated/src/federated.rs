//! This module implements the federated scheduling algorithm.
use lib::graph_extension::GraphExtension;
use lib::graph_extension::NodeData;
use log::warn;
use petgraph::graph::Graph;

fn is_high_utilization(volume: f32, end_to_end_deadline: f32) -> bool {
    let utilization_rate = volume / end_to_end_deadline;
    utilization_rate > 1.0
}

fn count_high_utilization_core(
    volume: f32,
    critical_path_wect: f32,
    end_to_end_deadline: f32,
) -> usize {
    ((volume - critical_path_wect) / (end_to_end_deadline - critical_path_wect)).ceil() as usize
}

fn is_task_set_allocatable(remaining_cores: usize, low_utilizations: f32, core_num: usize) -> bool {
    if remaining_cores as f32 > 2.0 * low_utilizations {
        warn!("Can allocate task set to {} cores.", core_num);
        true
    } else {
        warn!("Cannot allocate task set to {} cores.", core_num);
        false
    }
}
/// This function attempts to apply federated scheduling to a set of directed acyclic graphs
/// (DAGs), each representing a task with a certain end-to-end end_to_end_deadline and a worst-case
/// execution time (WCET). It also considers a given number of available processing cores.
///
/// # Arguments
///
/// * `dag_set` - A vector of Graphs. Each Graph represents a task with nodes of type `NodeData`
///   and edges of type `f32`. Each task has an "end_to_end_end_to_end_deadline" parameter and a WCET.
/// * `core_num` - The total number of available processing cores.
///
/// # Returns
///
/// * `bool` - Returns `true` if the tasks can be scheduled according to the federated
///   scheduling algorithm with the given number of cores, and `false` otherwise.
///
/// # Example
///
/// ```
/// use algorithm::federated::federated;
/// use petgraph::graph::Graph;
/// use lib::graph_extension::NodeData;
/// use std::collections::HashMap;
/// fn create_node(id: i32, key: &str, value: f32) -> NodeData {
///  let mut params = HashMap::new();
///  params.insert(key.to_string(), value);
///  NodeData { id, params }
/// }
/// let mut dag = Graph::<NodeData, f32>::new();
/// let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
/// let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
/// let mut params = HashMap::new();
/// params.insert("execution_time".to_owned(), 2.0);
/// params.insert("end_to_end_end_to_end_deadline".to_owned(), 143.0);
/// let n2 = dag.add_node(NodeData { id: 2, params });
/// dag.add_edge(n0, n1, 1.0);
/// dag.add_edge(n1, n2, 1.0);
/// let dag_set = vec![dag];
/// let core_num = 4;
/// let can_schedule = federated(dag_set, core_num);
/// ```
///
pub fn federated(dag_set: Vec<Graph<NodeData, f32>>, core_num: usize) -> bool {
    let mut remaining_cores = core_num;
    let mut low_utilizations = 0.0;

    for mut dag in dag_set {
        let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
        let volume = dag.get_volume();
        let critical_path = dag.get_critical_paths();
        let critical_path_wect = dag.get_total_wcet_from_nodes(&critical_path[0]);

        // Tasks that do not meet the following conditions are inappropriate for Federated
        if critical_path_wect > end_to_end_deadline {
            warn!("unsuited task {:#?} ", dag);
            return false;
        }

        if is_high_utilization(volume, end_to_end_deadline) {
            let using_cores =
                count_high_utilization_core(volume, critical_path_wect, end_to_end_deadline);
            if using_cores > remaining_cores {
                warn!("Insufficient number of cores for the task set.");
                return false;
            } else {
                remaining_cores -= using_cores;
            }
        } else {
            low_utilizations += volume / end_to_end_deadline;
        }
    }
    is_task_set_allocatable(remaining_cores, low_utilizations, core_num)
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_high_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 3.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 4.0));
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 3.0);
        params.insert("end_to_end_end_to_end_deadline".to_owned(), 10.0);
        let n3 = dag.add_node(NodeData { id: 3, params });
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n0, n3, 1.0);
        dag
    }
    fn create_low_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 4.0));
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 3.0);
        params.insert("end_to_end_end_to_end_deadline".to_owned(), 20.0);
        let n2 = dag.add_node(NodeData { id: 2, params });
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag
    }
    fn create_unsuited_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 10.0);
        params.insert("end_to_end_end_to_end_deadline".to_owned(), 10.0);
        let n2 = dag.add_node(NodeData { id: 2, params });
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n1, n2, 1.0);
        dag
    }
    #[test]
    fn test_federated_enough_core() {
        let dag0 = create_high_dag();
        let dag1 = create_high_dag();
        let dag2 = create_low_dag();
        let dag_set = vec![dag0, dag1, dag2];
        let core_num = 40;
        let can_schedule = federated(dag_set, core_num);
        assert!(can_schedule);
    }
    #[test]
    #[should_panic]
    fn test_federated_lack_cores_for_high_tasks() {
        let dag0 = create_high_dag();
        let dag1 = create_high_dag();
        let dag2 = create_low_dag();
        let dag_set = vec![dag0, dag1, dag2];
        let core_num = 1;
        let can_schedule = federated(dag_set, core_num);
        assert!(can_schedule);
    }
    #[test]
    #[should_panic]
    fn test_federated_lack_cores_for_low_tasks() {
        let dag0 = create_high_dag();
        let dag1 = create_low_dag();
        let dag2 = create_low_dag();
        let dag_set = vec![dag0, dag1, dag2];
        let core_num = 2;
        let can_schedule = federated(dag_set, core_num);
        assert!(can_schedule);
    }
    #[test]
    #[should_panic]
    fn test_federated_unsuited_tasks() {
        let dag0 = create_unsuited_dag();
        let dag1 = create_unsuited_dag();
        let dag2 = create_unsuited_dag();
        let dag_set = vec![dag0, dag1, dag2];
        let core_num = 5;
        let can_schedule = federated(dag_set, core_num);
        assert!(can_schedule);
    }
}
