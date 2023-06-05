use lib::fixed_priority_scheduler::fixed_priority_scheduler;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use petgraph::graph::{Graph, NodeIndex};

/// Calculate the execution order when minimum number of cores required to meet the end-to-end deadline.
///
/// # Arguments
///
/// * `dag` - The DAG to be scheduled.
///
/// # Returns
///
/// * A vector of NodeIndex, representing the execution order of the tasks.
///
/// # Description
///
/// The function first calculates the volume of the DAG and the end-to-end deadline of the DAG.
/// Then, it calculates the minimum number of cores required to meet the end-to-end deadline.
/// Afterward, it creates a HomogeneousProcessor object with the minimum number of cores.
/// Finally, it calls the fixed_priority_scheduler function to get the execution order of the tasks.
///
/// # Example
///
/// Refer to the examples in the tests code.
///
#[allow(dead_code)] // TODO: remove
pub fn get_execution_order(dag: &mut Graph<NodeData, i32>, num_cores: usize) -> Vec<NodeIndex> {
    let mut homogeneous_processor = HomogeneousProcessor::new(num_cores);
    fixed_priority_scheduler(&mut homogeneous_processor, dag).1
}

#[allow(dead_code)] // TODO: remove
pub fn get_min_mum_cores(dag: &mut Graph<NodeData, i32>) -> usize {
    let volume = dag.get_volume();
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
    let mut min_num_cores = (volume as f32 / end_to_end_deadline as f32).ceil() as usize;
    let mut homogeneous_processor = HomogeneousProcessor::new(min_num_cores);
    let mut result = fixed_priority_scheduler(&mut homogeneous_processor, dag);

    while result.0 > end_to_end_deadline {
        min_num_cores += 1;
        homogeneous_processor = HomogeneousProcessor::new(min_num_cores);
        result = fixed_priority_scheduler(&mut homogeneous_processor, dag);
    }

    min_num_cores
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
    fn test_get_min_num_cores_normal() {
        let mut dag = create_sample_dag();
        let result = get_min_mum_cores(&mut dag);

        assert_eq!(result, 3);
    }

    #[test]
    fn test_calculate_execution_order_normal() {
        let mut dag = create_sample_dag();
        let result = get_execution_order(&mut dag, 3);

        assert_eq!(
            result,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }
}
