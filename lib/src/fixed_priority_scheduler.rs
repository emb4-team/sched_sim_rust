use std::{collections::VecDeque, vec};

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::{get_minimum_multiplier_from_dag_set, ProcessorBase},
};
use petgraph::{graph::NodeIndex, Graph};

pub fn fixed_priority_scheduler(
    processor: &mut impl ProcessorBase,
    task_list: &mut Vec<NodeIndex>,
    dag: &mut Graph<NodeData, f32>,
) -> f32 {
    let mut ready_queue: VecDeque<NodeIndex> = VecDeque::new();
    let mut processing_tasks: Vec<NodeIndex> = vec![];
    let mut time = 0.0;

    let minimum_multiplier = get_minimum_multiplier_from_dag_set(&vec![dag.clone()]);
    processor.set_minimum_multiplier(minimum_multiplier);

    while !task_list.is_empty() || !ready_queue.is_empty() || !processing_tasks.is_empty() {
        // Set the ready queue.
        for task in task_list.clone() {
            let pre_nodes = dag.get_pre_nodes(task).unwrap_or_default();
            if pre_nodes.iter().any(|pre_node| {
                task_list.contains(pre_node)
                    || ready_queue.contains(pre_node)
                    || processing_tasks.contains(pre_node)
            }) {
                continue;
            }

            ready_queue.push_back(task);
            task_list.retain(|&node| node != task);
        }

        // Sort the ready queue.
        ready_queue.make_contiguous().sort_by_key(|&task| {
            let node = dag.node_weight(task).unwrap();
            let priority = node.params.get("priority").unwrap_or(&999.0);
            (*priority * 1000.0) as i32
        });

        // Add newly ready processes to the ready queue.
        while let Some(task) = ready_queue.pop_front() {
            if let Some(core_index) = processor.get_idle_core_index() {
                processor.allocate(core_index, dag[task].clone());
                processing_tasks.push(task);
            } else {
                ready_queue.push_front(task);
                break;
            }
        }

        let mut process_result = processor.process();
        time += 1.0;

        while !process_result
            .iter()
            .any(|result| matches!(result, ProcessResult::Done(_)))
        {
            process_result = processor.process();
            time += 1.0;
        }

        processing_tasks.retain(|&task| {
            !process_result.iter().any(
                |result| matches!(result, ProcessResult::Done(id) if *id == task.index() as i32),
            )
        });
    }
    time / minimum_multiplier as f32
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::homogeneous::HomogeneousProcessor;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn add_params(dag: &mut Graph<NodeData, f32>, node: NodeIndex, key: &str, value: f32) {
        let node_added = dag.node_weight_mut(node).unwrap();
        node_added.params.insert(key.to_string(), value);
    }

    #[test]
    fn test_fixed_priority_scheduler_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 5.1));
        let c1 = dag.add_node(create_node(1, "execution_time", 4.0));
        add_params(&mut dag, c0, "priority", 0.0);
        add_params(&mut dag, c1, "priority", 0.0);
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(2, "execution_time", 1.0));
        let n1_2 = dag.add_node(create_node(3, "execution_time", 1.0));
        add_params(&mut dag, n0_2, "priority", 2.0);
        add_params(&mut dag, n1_2, "priority", 1.0);

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1.0);
        dag.add_edge(c0, n1_2, 1.0);

        let mut task_list = vec![c0, c1, n0_2, n1_2];
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        assert_eq!(
            fixed_priority_scheduler(&mut homogeneous_processor, &mut task_list, &mut dag),
            9.1
        );
    }
}
