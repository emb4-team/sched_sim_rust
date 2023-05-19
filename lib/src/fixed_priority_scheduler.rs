use std::{collections::VecDeque, vec};

use crate::{
    core::ProcessResult,
    graph_extension::NodeData,
    processor::{get_minimum_time_unit_from_dag_set, ProcessorBase},
};
use petgraph::{graph::NodeIndex, Direction::Incoming, Graph};

pub fn fixed_priority_scheduler(
    processor: &mut impl ProcessorBase,
    task_list: Vec<NodeIndex>,
    dag: &Graph<NodeData, f32>,
) -> f32 {
    let mut ready_queue: VecDeque<NodeIndex> = VecDeque::new();
    let mut time = 0.0;

    // Set the time unit of the processor.
    let time_unit = get_minimum_time_unit_from_dag_set(&vec![dag.clone()]);
    println!("time_unit: {}", time_unit);
    processor.set_time_unit(time_unit);

    // Set the ready queue.
    for &task in &task_list {
        if dag.neighbors_directed(task, Incoming).count() == 0 {
            ready_queue.push_back(task);
        }
    }

    // Add newly ready processes to the ready queue.
    while let Some(task) = ready_queue.pop_front() {
        println!("task: {}", task.index());
        if let Some(core_index) = processor.get_idle_core_index() {
            println!("core_index: {}", core_index);
            processor.allocate(core_index, dag[task].clone());
        } else {
            // No idle cores available, break the loop
            println!("No idle cores");
            ready_queue.push_front(task);
            break;
        }
    }

    // Run the processes
    let mut process_result = processor.process();
    time += time_unit;

    while !process_result.contains(&ProcessResult::Done) {
        process_result = processor.process();
        time += time_unit;
    }

    println!("time: {}", time);

    // If the running process is done, remove it.

    // If there is no running process, choose one from the ready queue.

    // If there is a running process, run it for one time unit.

    // If there are no processes left, we are done.

    // Advance the time.
    time
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

    #[test]
    fn test_fixed_priority_scheduler_checked() {
        let mut dag = Graph::<NodeData, f32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 5.1));
        let c1 = dag.add_node(create_node(1, "execution_time", 4.0));
        let c2 = dag.add_node(create_node(2, "execution_time", 3.0));
        let c3 = dag.add_node(create_node(3, "execution_time", 2.0));
        let c4 = dag.add_node(create_node(4, "execution_time", 3.0));
        let c5 = dag.add_node(create_node(5, "execution_time", 2.0));
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(6, "execution_time", 1.0));
        let n0_5 = dag.add_node(create_node(7, "execution_time", 1.0));

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);
        dag.add_edge(c1, c2, 1.0);
        dag.add_edge(c2, c3, 1.0);
        dag.add_edge(c3, c4, 1.0);
        dag.add_edge(c4, c5, 1.0);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1.0);
        dag.add_edge(n0_2, c2, 1.0);
        dag.add_edge(c3, n0_5, 1.0);
        dag.add_edge(n0_5, c5, 1.0);

        let task_list = vec![c0, c1, c2, c3, c4, c5, n0_2, n0_5];
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        fixed_priority_scheduler(&mut homogeneous_processor, task_list, &dag);
    }
}
