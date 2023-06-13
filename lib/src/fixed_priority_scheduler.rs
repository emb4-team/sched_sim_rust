use std::collections::VecDeque;

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::ProcessorBase,
    scheduler::SchedulerBase,
};
use petgraph::{graph::NodeIndex, Graph};

const DUMMY_EXECUTION_TIME: i32 = 1;

pub struct FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    pub dag: Graph<NodeData, i32>,
    pub processor: T,
}

impl<T> SchedulerBase<T> for FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self {
        Self {
            dag: dag.clone(),
            processor: processor.clone(),
        }
    }

    fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.dag = dag.clone();
    }

    fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
    }

    /// This function implements a fixed priority scheduling algorithm on a DAG (Directed Acyclic Graph).
    ///
    /// # Arguments
    ///
    /// * `processor` - An object that implements `ProcessorBase` trait, representing a CPU or a collection of CPU cores.
    /// * `dag` - A mutable reference to a Graph object, representing the dependencies among tasks.
    ///
    /// # Returns
    ///
    /// * A floating point number representing the normalized total time taken to finish all tasks.
    /// * A vector of NodeIndex, representing the order of tasks finished.
    ///
    /// # Description
    ///
    /// The function `fixed_priority_scheduler` is responsible for task scheduling based on a Directed Acyclic Graph (DAG).
    /// Specifically, it schedules tasks, associated with priority, on a processor, and continues to do so until all tasks have been executed.
    /// The tasks are scheduled in order of their priority, from highest to lowest (smaller values have higher priority).
    ///
    /// Initially, a processor and a DAG are passed to this function.
    /// Dummy source and sink nodes are added to the DAG. These nodes symbolize the start and end points of the tasks, respectively.
    /// In the main loop of the function, the following operations are carried out:
    ///
    /// 1. Nodes representing tasks that are ready to be scheduled are sorted by their priority.
    /// 2. If there is an idle core available, the task with the highest priority is allocated to it.
    /// 3. The processor processes for a single unit of time. This is repeated until all tasks are completed.
    /// 4. Once all tasks are completed, dummy source and sink nodes are removed from the DAG.
    ///
    /// The function returns the total time taken to complete all tasks (excluding the execution time of the dummy tasks) and the order in which the tasks were executed.
    ///
    /// # Example
    ///
    /// Refer to the examples in the tests code.
    ///
    fn schedule(&mut self) -> (i32, Vec<NodeIndex>) {
        let mut dag = self.dag.clone(); //To avoid adding pre_node_count to the original DAG
        let mut current_time = 0;
        let mut execution_order = Vec::new();
        let mut ready_queue: VecDeque<NodeIndex> = VecDeque::new();
        let source_node = dag.add_dummy_source_node();

        dag[source_node]
            .params
            .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);
        let sink_node = dag.add_dummy_sink_node();
        dag[sink_node]
            .params
            .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);

        ready_queue.push_back(source_node);

        loop {
            //Sort by priority
            ready_queue.make_contiguous().sort_by_key(|&node| {
                dag[node].params.get("priority").unwrap_or_else(|| {
                    eprintln!(
                        "Warning: 'priority' parameter not found for node {:?}",
                        node
                    );
                    &999 //Because sorting cannot be done well without a priority
                })
            });

            //Assign the highest priority task first to the first idle core found.
            while let Some(core_index) = self.processor.get_idle_core_index() {
                if let Some(task) = ready_queue.pop_front() {
                    self.processor.allocate(core_index, &dag[task]);
                    execution_order.push(task);
                } else {
                    break;
                }
            }

            //Move one unit time so that the core state of the previous loop does not remain.
            let mut process_result = self.processor.process();
            current_time += 1;

            //Process until there is a task finished.
            while !process_result
                .iter()
                .any(|result| matches!(result, ProcessResult::Done(_)))
            {
                process_result = self.processor.process();
                current_time += 1;
            }

            let finish_nodes: Vec<NodeIndex> = process_result
                .iter()
                .filter_map(|result| {
                    if let ProcessResult::Done(id) = result {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();

            if finish_nodes.len() == 1 && dag.get_suc_nodes(finish_nodes[0]).is_none() {
                break; // The scheduling has finished because the dummy sink node has completed.
            }

            //Executable if all predecessor nodes are done
            for finish_node in finish_nodes {
                let suc_nodes = dag.get_suc_nodes(finish_node).unwrap_or_default();
                for suc_node in suc_nodes {
                    if let Some(value) = dag[suc_node].params.get_mut("pre_done_count") {
                        *value += 1;
                    } else {
                        dag[suc_node].params.insert("pre_done_count".to_owned(), 1);
                    }
                    let pre_nodes = dag.get_pre_nodes(suc_node).unwrap_or_default();
                    if pre_nodes.len() as i32 == dag[suc_node].params["pre_done_count"] {
                        ready_queue.push_back(suc_node);
                    }
                }
            }
        }

        //remove dummy nodes
        dag.remove_dummy_sink_node();
        dag.remove_dummy_source_node();

        //Remove the dummy source node from the execution order.
        execution_order.remove(0);
        //Remove the dummy sink node from the execution order.
        execution_order.pop();

        /*
        let num_of_cores = self.processor.get_number_of_cores();
        let mut utilization_rate = vec![0.0; num_of_cores];
        for (core_id, rate) in utilization_rate.iter_mut().enumerate().take(num_of_cores) {
            let total_proc_time = self.processor.get_total_proc_time_by_core(core_id);
            *rate = total_proc_time as f32 / (current_time - DUMMY_EXECUTION_TIME * 2) as f32;
        }
        let average_utilization_rate = utilization_rate.iter().sum::<f32>() / num_of_cores as f32;

        let _variance = utilization_rate
            .iter()
            .map(|rate| (rate - average_utilization_rate).powi(2))
            .sum::<f32>()
            / num_of_cores as f32;
        */

        //Return the normalized total time taken to finish all tasks.
        (current_time - DUMMY_EXECUTION_TIME * 2, execution_order)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::homogeneous::HomogeneousProcessor;
    use crate::processor::ProcessorBase;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn add_params(dag: &mut Graph<NodeData, i32>, node: NodeIndex, key: &str, value: i32) {
        let node_added = dag.node_weight_mut(node).unwrap();
        node_added.params.insert(key.to_string(), value);
    }

    #[test]
    fn test_fixed_priority_scheduler_new_normal() {
        let dag = Graph::<NodeData, i32>::new();
        let processor = HomogeneousProcessor::new(1);
        let scheduler = FixedPriorityScheduler::new(&dag, &processor);
        assert_eq!(scheduler.dag.node_count(), 0);
        assert_eq!(scheduler.processor.get_number_of_cores(), 1);
    }

    #[test]
    fn test_fixed_priority_scheduler_set_dag() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 0));
        let processor = HomogeneousProcessor::new(1);
        let mut scheduler = FixedPriorityScheduler::new(&dag, &processor);
        assert_eq!(scheduler.dag.node_count(), 1);
        assert_eq!(scheduler.processor.get_number_of_cores(), 1);
        scheduler.set_dag(&Graph::<NodeData, i32>::new());
        assert_eq!(scheduler.dag.node_count(), 0);
    }

    #[test]
    fn test_fixed_priority_scheduler_set_processor() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 0));
        let processor = HomogeneousProcessor::new(1);
        let mut scheduler = FixedPriorityScheduler::new(&dag, &processor);
        assert_eq!(scheduler.dag.node_count(), 1);
        assert_eq!(scheduler.processor.get_number_of_cores(), 1);
        scheduler.set_processor(&HomogeneousProcessor::new(2));
        assert_eq!(scheduler.processor.get_number_of_cores(), 2);
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        add_params(&mut dag, c0, "priority", 0);
        add_params(&mut dag, c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 12));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        add_params(&mut dag, n0_0, "priority", 2);
        add_params(&mut dag, n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(2));
        let result = fixed_priority_scheduler.schedule();

        assert_eq!(result.0, 92);

        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_concurrent_task() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        add_params(&mut dag, c0, "priority", 0);
        add_params(&mut dag, c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        add_params(&mut dag, n0_0, "priority", 2);
        add_params(&mut dag, n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(3));
        let result = fixed_priority_scheduler.schedule();

        assert_eq!(result.0, 92);
        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_used_twice_for_same_dag() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        dag.add_node(create_node(0, "execution_time", 1));

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule();
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule();
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);
    }
}
