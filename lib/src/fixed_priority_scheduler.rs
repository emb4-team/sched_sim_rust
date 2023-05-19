use std::{collections::VecDeque, vec};

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::{get_minimum_multiplier_from_dag_set, ProcessorBase},
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
    task_list: &mut Vec<NodeIndex>,
    dag: &mut Graph<NodeData, f32>,
) -> f32 {
    let mut ready_queue: VecDeque<NodeIndex> = VecDeque::new();
    let mut finished_tasks: Vec<NodeIndex> = vec![];
    let mut time = 0.0;
    let task_list_len = task_list.len();
    let minimum_multiplier = get_minimum_multiplier_from_dag_set(&vec![dag.clone()]);
    processor.set_minimum_multiplier(minimum_multiplier);

    while task_list_len != finished_tasks.len() {
        //Executable if all predecessor nodes are done
        for task in task_list.clone() {
            let pre_nodes = dag.get_pre_nodes(task).unwrap_or_default();
            if pre_nodes
                .iter()
                .any(|pre_node| !finished_tasks.contains(pre_node))
            {
                continue;
            }

            ready_queue.push_back(task);
            //Remove it from the list so it won't run again
            task_list.retain(|&node| node != task);
        }

        //Sort by priority
        ready_queue.make_contiguous().sort_by_key(|&task| {
            let node = dag.node_weight(task).unwrap();
            let priority = node.params.get("priority").unwrap_or(&999.0);
            *priority as i32
        });

        //Assign the highest priority task first to the first idle core found.
        while let Some(task) = ready_queue.pop_front() {
            if let Some(core_index) = processor.get_idle_core_index() {
                processor.allocate(core_index, dag[task].clone());
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
            .any(|result| matches!(result, ProcessResult::Done(_)))
        {
            process_result = processor.process();
            time += 1.0;
        }

        //id is needed to get NodeIndex because the processor takes NodeData as an argument.
        let finish_node = match process_result
            .iter()
            .find(|result| matches!(result, ProcessResult::Done(_)))
        {
            Some(ProcessResult::Done(id)) => *id,
            _ => unreachable!(), //This is unreachable because the loop above ensures that there is a task finished.
        };

        let finish_node = dag
            .node_indices()
            .find(|node_index| dag[*node_index].id == finish_node)
            .unwrap();

        finished_tasks.push(finish_node);
    }

    //Return the normalized total time taken to finish all tasks.
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
        let c0 = dag.add_node(create_node(0, "execution_time", 5.2));
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
            9.2
        );
    }
}
