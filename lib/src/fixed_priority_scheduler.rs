use std::{
    collections::{HashMap, VecDeque},
    vec,
};

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::ProcessorBase,
};
use petgraph::{graph::NodeIndex, Graph};

/// This function implements a fixed priority scheduling algorithm on a DAG (Directed Acyclic Graph).
///
/// # Arguments
///
/// * `processor` - An object that implements `ProcessorBase` trait, representing a CPU or a collection of CPU cores.
/// * `task_list` - A mutable reference to a vector of NodeIndex, representing the tasks to be scheduled.
/// * `dag` - A mutable reference to a Graph object, representing the dependencies among tasks.
///
/// # Returns
///
/// * A floating point number representing the normalized total time taken to finish all tasks.
///
/// # Description
///
/// The function first initializes a ready queue and a list of finished tasks, sets the time to 0,
/// calculates the minimum_multiplier from the dag set and sets it to the processor.
///
/// Then, it enters a loop that continues until all tasks are finished.
/// In each iteration, it finds the tasks whose predecessors have all finished and adds them to the ready queue.
/// Afterward, it sorts the ready queue by the priority of tasks.
///
/// When there is an idle core, it assigns the first task in the ready queue to the idle core.
///
/// Then it processes tasks for one time unit and checks if there are tasks finished.
/// If no tasks finish in this time unit, it continues processing tasks until there are tasks finished.
///
/// When a task finishes, it adds the task to the finished tasks list.
///
/// Finally, the function returns the total time taken to finish all tasks divided by the minimum_multiplier.
///
/// # Example
///
/// Refer to the examples in the tests code.
///
pub fn fixed_priority_scheduler(
    processor: &mut impl ProcessorBase,
    dag: &mut Graph<NodeData, f32>,
) -> f32 {
    let mut ready_queue: VecDeque<NodeIndex> = VecDeque::new();
    let mut finished_nodes: Vec<NodeIndex> = vec![];
    let mut time = 0.0;
    let mut finished_core_and_node_hashmap: HashMap<usize, NodeIndex> = HashMap::new();

    let source_node = dag.add_dummy_source_node();
    dag[source_node]
        .params
        .insert("execution_time".to_string(), 1.0);
    let sink_node = dag.add_dummy_sink_node();
    dag[sink_node]
        .params
        .insert("execution_time".to_string(), 1.0);

    ready_queue.push_back(source_node);

    loop {
        //Sort by priority
        ready_queue.make_contiguous().sort_by_key(|&node| {
            let node = dag.node_weight(node).unwrap();
            let priority = node.params.get("priority").unwrap_or(&999.0);
            *priority as i32
        });

        //Assign the highest priority task first to the first idle core found.
        while let Some(task) = ready_queue.pop_front() {
            if let Some(core_index) = processor.get_idle_core_index() {
                processor.allocate(core_index, dag[task].clone());
                finished_core_and_node_hashmap.insert(core_index, task);
            } else {
                //If everything is working, it is useless to search any further, so the extracted task is returned to the ready queue.
                ready_queue.push_front(task);
                break;
            }
        }

        //Move one unit time so that the core state of the previous loop does not remain.
        let mut process_result = processor.process();
        time += 1.0;

        //Process until there is a task finished.
        while !process_result
            .iter()
            .any(|result| matches!(result, ProcessResult::Done))
        {
            process_result = processor.process();
            time += 1.0;
        }

        let finish_node = process_result
            .iter()
            .enumerate()
            .find(|(_, result)| matches!(result, ProcessResult::Done))
            .and_then(|(core_index, _)| finished_core_and_node_hashmap.remove(&core_index))
            .unwrap();

        finished_nodes.push(finish_node);

        //Executable if all predecessor nodes are done
        let suc_nodes = dag.get_suc_nodes(finish_node).unwrap_or_default();
        if suc_nodes.is_empty() {
            break;
        }
        for suc_node in suc_nodes {
            let pre_nodes = dag.get_pre_nodes(suc_node).unwrap_or_default();
            if pre_nodes
                .iter()
                .any(|pre_node| !finished_nodes.contains(pre_node))
            {
                continue;
            }

            ready_queue.push_back(suc_node);
        }
    }

    //Return the normalized total time taken to finish all tasks.
    time - 2.0
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
        let c0 = dag.add_node(create_node(0, "execution_time", 52.0));
        let c1 = dag.add_node(create_node(1, "execution_time", 40.0));
        add_params(&mut dag, c0, "priority", 0.0);
        add_params(&mut dag, c1, "priority", 0.0);
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(2, "execution_time", 10.0));
        let n1_2 = dag.add_node(create_node(3, "execution_time", 10.0));
        add_params(&mut dag, n0_2, "priority", 2.0);
        add_params(&mut dag, n1_2, "priority", 1.0);

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1.0);
        dag.add_edge(c0, n1_2, 1.0);

        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        assert_eq!(
            fixed_priority_scheduler(&mut homogeneous_processor, &mut dag),
            92.0
        );
    }
}
