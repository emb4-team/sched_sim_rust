//! This module implements the federated scheduling algorithm.
use lib::graph_extension::GraphExtension;
use lib::graph_extension::NodeData;
use petgraph::graph::Graph;
use FederateResult::*;

/// For determination of federates
#[derive(Debug, PartialEq)]
pub enum FederateResult {
    Unschedulable { reason: String },
    Schedulable,
}

/// This function attempts to apply federated scheduling to a set of directed acyclic graphs
/// (DAGs), each representing a task with a certain period and a worst-case
/// execution time (WCET). It also considers a given number of available processing cores.
///
/// # Arguments
///
/// * `dag_set` - A vector of Graphs. Each Graph represents a task with nodes of type `NodeData`
///   and edges of type `f32`. Each task has an "period" parameter and a WCET.
/// * `number_of_cores` - The total number of available processing cores.
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
/// let mut params = HashMap::new();
/// params.insert("execution_time".to_owned(), 2.0);
/// params.insert("period".to_owned(), 143.0);
/// let n0 = dag.add_node(NodeData { id: 2, params });
/// let n1 = dag.add_node(create_node(0, "execution_time", 3.0));
/// let n2 = dag.add_node(create_node(1, "execution_time", 6.0));
/// dag.add_edge(n0, n1, 1.0);
/// dag.add_edge(n1, n2, 1.0);
/// let dag_set = vec![dag];
/// let number_of_cores = 4;
/// let can_schedule = federated(dag_set, number_of_cores);
/// ```
///
pub fn federated(dag_set: Vec<Graph<NodeData, f32>>, number_of_cores: usize) -> FederateResult {
    let mut remaining_cores = number_of_cores;
    let mut low_utilizations = 0.0;

    for mut dag in dag_set {
        let period = dag
            .get_period()
            .unwrap_or_else(|| panic!("Period is not defined for the tasks."));
        // Conforms to the definition in the original paper
        let end_to_end_deadline = period; // implicit deadline
        let volume = dag.get_volume();
        let critical_paths = dag.get_critical_paths();
        let critical_path_wcet = dag.get_total_wcet_from_nodes(&critical_paths[0]);

        // Tasks that do not meet the following conditions are inappropriate for Federated
        if critical_path_wcet > end_to_end_deadline {
            return Unschedulable {
                reason: "Critical path WCET is greater than end_to_end_deadline.".to_string(),
            };
        }

        if volume / period > 1.0 {
            let using_cores = ((volume - critical_path_wcet)
                / (end_to_end_deadline - critical_path_wcet))
                .ceil() as usize;
            if using_cores > remaining_cores {
                return Unschedulable {
                    reason: "Insufficient number of high-utilization cores for the task set."
                        .to_string(),
                };
            } else {
                remaining_cores -= using_cores;
            }
        } else {
            low_utilizations += volume / period;
        }
    }
    if remaining_cores as f32 > 2.0 * low_utilizations {
        Schedulable
    } else {
        Unschedulable {
            reason: "Insufficient number of low-utilization cores for the task set.".to_string(),
        }
    }
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

    fn create_high_utilization_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 3.0);
            params.insert("period".to_owned(), 10.0);
            dag.add_node(NodeData { id: 3, params })
        };
        let n1 = dag.add_node(create_node(1, "execution_time", 3.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 3.0));
        let n3 = dag.add_node(create_node(0, "execution_time", 4.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n0, n3, 1.0);
        dag
    }

    fn create_low_utilization_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 3.0);
            params.insert("period".to_owned(), 30.0);
            dag.add_node(NodeData { id: 2, params })
        };
        let n1 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n2 = dag.add_node(create_node(1, "execution_time", 4.0));

        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag
    }

    fn create_period_exceeding_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 20.0);
        params.insert("period".to_owned(), 10.0);
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    fn create_no_has_period_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 3.0);
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    #[test]
    fn test_federated_enough_core() {
        let dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];

        assert_eq!(federated(dag_set, 40), Schedulable);
    }

    #[test]
    fn test_federated_lack_cores_for_high_tasks() {
        let dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];

        assert_eq!(
            federated(dag_set, 1),
            Unschedulable {
                reason: (String::from("Insufficient number of cores for high-utilization tasks."))
            }
        );
    }

    #[test]
    fn test_federated_lack_cores_for_low_tasks() {
        let dag_set = vec![
            create_high_utilization_dag(),
            create_low_utilization_dag(),
            create_low_utilization_dag(),
        ];

        assert_eq!(
            federated(dag_set, 2),
            Unschedulable {
                reason: (String::from("Insufficient number of cores for low-utilization tasks."))
            }
        );
    }

    #[test]
    fn test_federated_unsuited_tasks() {
        assert_eq!(
            federated(vec![create_period_exceeding_dag()], 5),
            Unschedulable {
                reason: (String::from("Critical path WCET is greater than end_to_end_deadline."))
            }
        );
    }

    #[test]
    #[should_panic]
    fn test_federated_no_has_period() {
        federated(vec![create_no_has_period_dag()], 1);
    }
}
