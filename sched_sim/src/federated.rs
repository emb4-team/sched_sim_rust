use lib::graph_extension::GraphExtension;
use lib::graph_extension::NodeData;
use petgraph::graph::Graph;

fn is_high_utilization_task(sum_wect: f32, deadline: f32) -> bool {
    let utilization_rate = sum_wect / deadline;
    utilization_rate > 1.0
}

fn calculate_cores(sum_wect: f32, critical_path_wect: f32, deadline: f32) -> usize {
    ((sum_wect - critical_path_wect) / (deadline - critical_path_wect)).ceil() as usize
}

fn finalize_task_set_allocation(
    remaining_cores: usize,
    low_utilizations: f32,
    core_num: usize,
) -> bool {
    if remaining_cores as f32 > 2.0 * low_utilizations {
        println!("Can allocate task set to {} cores.", core_num);
        true
    } else {
        println!("Cannot allocate task set to {} cores.", core_num);
        false
    }
}
/// This function attempts to apply federated scheduling to a set of directed acyclic graphs
/// (DAGs), each representing a task with a certain end-to-end deadline and a worst-case
/// execution time (WCET). It also considers a given number of available processing cores.
///
/// # Arguments
///
/// * `dag_set` - A vector of Graphs. Each Graph represents a task with nodes of type `NodeData`
///   and edges of type `f32`. Each task has an "end_to_end_deadline" parameter and a WCET.
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
/// params.insert("end_to_end_deadline".to_owned(), 143.0);
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
        let deadline = dag.get_end_to_end_deadline().unwrap();
        let sum_wect = dag.get_volume();
        let critical_path = dag.get_critical_paths();
        let critical_path_wect = dag.get_total_wcet_from_nodes(&critical_path[0]);

        if critical_path_wect > deadline {
            println!("unsuited task {:#?} ", dag);
            return false;
        }

        if is_high_utilization_task(sum_wect, deadline) {
            let using_cores = calculate_cores(sum_wect, critical_path_wect, deadline);
            if using_cores > remaining_cores {
                println!("Insufficient number of cores for the task set.");
                return false;
            } else {
                remaining_cores -= using_cores;
            }
        } else {
            low_utilizations += sum_wect / deadline;
        }
    }
    finalize_task_set_allocation(remaining_cores, low_utilizations, core_num)
}
