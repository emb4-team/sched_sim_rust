use lib::fixed_priority_scheduler::fixed_priority_scheduler;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use petgraph::graph::{Graph, NodeIndex};

#[allow(dead_code)] // TODO: remove
pub fn calculate_execution_order_minimum_cores(dag: &mut Graph<NodeData, i32>) -> Vec<NodeIndex> {
    let volume = dag.get_volume();
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
    let mut min_number_of_cores = (volume as f32 / end_to_end_deadline as f32).ceil() as usize;

    #[allow(unused_variables, unused_mut)] // TODO: remove
    let mut processor = HomogeneousProcessor::new(min_number_of_cores);

    let mut result = fixed_priority_scheduler(&mut processor, dag);

    while result.0 > end_to_end_deadline {
        min_number_of_cores += 1;
        processor = HomogeneousProcessor::new(min_number_of_cores);
        result = fixed_priority_scheduler(&mut processor, dag);
    }

    result.1
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

    #[test]
    fn test_calculate_execution_order_minimum_cores_normal() {
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

        let result = calculate_execution_order_minimum_cores(&mut dag);

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
