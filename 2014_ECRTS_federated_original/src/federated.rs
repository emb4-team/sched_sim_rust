//! This module implements the federated scheduling algorithm.
use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::Graph;
use serde_derive::{Deserialize, Serialize};
use FederateResult::{Schedulable, Unschedulable};

/// For determination of federates
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum FederateResult {
    Schedulable {
        high_dedicated_cores: usize,
        low_dedicated_cores: usize,
    },
    Unschedulable {
        reason: String,
        insufficient_cores: usize,
    },
}

/// This function attempts to apply federated scheduling to a set of directed acyclic graphs
/// (DAGs), each representing a task with a certain period and a worst-case
/// execution time (WCET). It also considers a given number of available processing cores.
///
/// # Arguments
///
/// * `dag_set` - A vector of Graphs. Each Graph represents a task with nodes of type `NodeData`
///   and edges of type `i32`. Each task has an "period" parameter and a WCET.
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
/// fn create_node(id: i32, key: &str, value: i32) -> NodeData {
///  let mut params = HashMap::new();
///  params.insert(key.to_string(), value);
///  NodeData { id, params }
/// }
/// let mut dag = Graph::<NodeData, i32>::new();
/// let mut params = HashMap::new();
/// params.insert("execution_time".to_owned(), 2);
/// params.insert("period".to_owned(), 143);
/// let n0 = dag.add_node(NodeData { id: 2, params });
/// let n1 = dag.add_node(create_node(0, "execution_time", 3));
/// let n2 = dag.add_node(create_node(1, "execution_time", 6));
/// dag.add_edge(n0, n1, 1);
/// dag.add_edge(n1, n2, 1);
/// let dag_set = vec![dag];
/// let number_of_cores = 4;
/// let can_schedule = federated(dag_set, number_of_cores);
/// ```
///
pub fn federated(dag_set: &mut [Graph<NodeData, i32>], number_of_cores: usize) -> FederateResult {
    let mut remaining_cores = number_of_cores;
    let mut low_utilizations = 0.0;

    for dag in dag_set {
        let period = dag.get_head_period().unwrap();

        // Conforms to the definition in the original paper
        let end_to_end_deadline = period; // implicit deadline
        let volume = dag.get_volume();
        let critical_path = dag.get_critical_path();
        let critical_path_wcet = dag.get_total_wcet_from_nodes(&critical_path);

        // Tasks that do not meet the following conditions are inappropriate for Federated
        if critical_path_wcet > end_to_end_deadline {
            return Unschedulable {
                reason: "The critical path length is greater than end_to_end_deadline.".to_string(),
                insufficient_cores: 0,
            };
        }

        let utilization = volume as f32 / period as f32;
        if utilization > 1.0 {
            let high_dedicated_cores = ((volume - critical_path_wcet) as f32
                / (end_to_end_deadline - critical_path_wcet) as f32)
                .ceil() as usize;
            if high_dedicated_cores > remaining_cores {
                return Unschedulable {
                    reason: "Insufficient number of cores for high-utilization tasks.".to_string(),
                    insufficient_cores: high_dedicated_cores - remaining_cores,
                };
            } else {
                remaining_cores -= high_dedicated_cores;
            }
        } else {
            low_utilizations += utilization;
        }
    }
    if remaining_cores as f32 > 2.0 * low_utilizations {
        Schedulable {
            high_dedicated_cores: number_of_cores - remaining_cores,
            low_dedicated_cores: remaining_cores,
        }
    } else {
        Unschedulable {
            reason: "Insufficient number of cores for low-utilization tasks.".to_string(),
            insufficient_cores: (2.0 * low_utilizations - remaining_cores as f32).ceil() as usize,
        }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use lib::tests_helper::{
        create_high_utilization_dag, create_low_utilization_dag, create_simple_dag,
    };

    fn test_federated_helper(
        dag_set: Vec<fn() -> Graph<NodeData, i32>>,
        nun_core: usize,
        expected_result: FederateResult,
    ) {
        let mut dag_set: Vec<Graph<NodeData, i32>> = dag_set.into_iter().map(|dag| dag()).collect();
        assert_eq!(federated(&mut dag_set, nun_core), expected_result);
    }

    #[test]
    fn test_federated_enough_core() {
        test_federated_helper(
            vec![
                create_high_utilization_dag,
                create_high_utilization_dag,
                create_low_utilization_dag,
            ],
            40,
            Schedulable {
                high_dedicated_cores: 6,
                low_dedicated_cores: 34,
            },
        );
    }

    #[test]
    fn test_federated_lack_cores_for_high_tasks() {
        test_federated_helper(
            vec![
                create_high_utilization_dag,
                create_high_utilization_dag,
                create_low_utilization_dag,
            ],
            1,
            Unschedulable {
                reason: String::from("Insufficient number of cores for high-utilization tasks."),
                insufficient_cores: 2,
            },
        );
    }

    #[test]
    fn test_federated_lack_cores_for_low_tasks() {
        test_federated_helper(
            vec![
                create_high_utilization_dag,
                create_low_utilization_dag,
                create_low_utilization_dag,
            ],
            3,
            Unschedulable {
                reason: String::from("Insufficient number of cores for low-utilization tasks."),
                insufficient_cores: 2,
            },
        );
    }

    #[test]
    fn test_federated_unsuited_tasks() {
        test_federated_helper(
            vec![|| create_simple_dag(20, Some(10))],
            5,
            Unschedulable {
                reason: String::from(
                    "The critical path length is greater than end_to_end_deadline.",
                ),
                insufficient_cores: 0,
            },
        );
    }

    #[test]
    #[should_panic]
    fn test_federated_no_has_period() {
        federated(&mut [create_simple_dag(20, None)], 1);
    }
}
