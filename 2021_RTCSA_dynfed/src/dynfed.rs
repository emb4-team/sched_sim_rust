use lib::fixed_priority_scheduler::FixedPriorityScheduler;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use lib::scheduler::SchedulerBase;
use petgraph::graph::{Graph, NodeIndex};

/// Calculate the execution order when minimum number of cores required to meet the end-to-end deadline.
///
/// # Arguments
///
/// * `dag` - The DAG to be scheduled.
///
/// # Returns
///
/// * The minimum number of cores required to meet the end-to-end deadline.
/// * A vector of NodeIndex, representing the execution order of the tasks.
///
/// # Description
///
/// This function calculates the minimum number of cores required to meet the end-to-end deadline of the DAG.
/// In addition, it returns the execution order of the tasks when the minimum number of cores are used.
///
/// # Example
///
/// Refer to the examples in the tests code.
///
#[allow(dead_code)] // TODO: remove
pub fn calculate_minimum_cores_and_execution_order(
    dag: &mut Graph<NodeData, i32>,
) -> (usize, Vec<NodeIndex>) {
    let volume = dag.get_volume();
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
    let mut minimum_cores = (volume as f32 / end_to_end_deadline as f32).ceil() as usize;
    let (mut schedule_length, mut execution_order) =
        FixedPriorityScheduler::schedule(dag, HomogeneousProcessor::new(minimum_cores));

    while schedule_length > end_to_end_deadline {
        minimum_cores += 1;
        (schedule_length, execution_order) =
            FixedPriorityScheduler::schedule(dag, HomogeneousProcessor::new(minimum_cores));
    }

    (minimum_cores, execution_order)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        dag.add_param(c0, "priority", 0);
        dag.add_param(c1, "priority", 0);
        dag.add_param(c0, "end_to_end_deadline", 100);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 30));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 30));
        dag.add_param(n0_0, "priority", 2);
        dag.add_param(n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        dag
    }

    #[test]
    fn test_calculate_minimum_cores_and_execution_order_normal() {
        let mut dag = create_sample_dag();
        let (finished_time, execution_order) =
            calculate_minimum_cores_and_execution_order(&mut dag);

        assert_eq!(finished_time, 3);
        assert_eq!(
            execution_order,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }
}
