use std::collections::VecDeque;

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::ProcessorBase,
    scheduler::{DAGSchedulerBase, NodeLog, ProcessorLog},
};

use petgraph::{graph::NodeIndex, Graph};

const DUMMY_EXECUTION_TIME: i32 = 1;

#[derive(Clone, Default)]
pub struct NonPreemptiveScheduler<T>
where
    T: ProcessorBase + Clone,
{
    pub dag: Graph<NodeData, i32>,
    pub processor: T,
    pub ready_queue: VecDeque<NodeIndex>,
    pub node_logs: Vec<NodeLog>,
    pub processor_log: ProcessorLog,
}

impl<T> DAGSchedulerBase<T> for NonPreemptiveScheduler<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self {
        Self {
            dag: dag.clone(),
            processor: processor.clone(),
            ready_queue: VecDeque::new(),
            node_logs: dag
                .node_indices()
                .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
                .collect(),
            processor_log: ProcessorLog::new(processor.get_number_of_cores()),
        }
    }

    fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.dag = dag.clone();
        self.node_logs = dag
            .node_indices()
            .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
            .collect();
    }

    fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
        self.processor_log = ProcessorLog::new(processor.get_number_of_cores());
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
    fn schedule<F>(&mut self, func: F) -> (i32, VecDeque<NodeIndex>)
    where
        F: Fn(&Graph<NodeData, i32>, &mut VecDeque<NodeIndex>),
    {
        let mut dag = self.dag.clone(); //To avoid adding pre_node_count to the original DAG
        let mut current_time = 0;
        let mut execution_order = VecDeque::new();

        let source_node_i = dag.add_dummy_source_node();

        dag[source_node_i]
            .params
            .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);
        let sink_node_i = dag.add_dummy_sink_node();
        dag[sink_node_i]
            .params
            .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);

        self.ready_queue.push_back(source_node_i);

        loop {
            func(&dag, &mut self.ready_queue);

            //Assign the highest priority task first to the first idle core found.
            while let Some(core_index) = self.processor.get_idle_core_index() {
                if let Some(node_i) = self.ready_queue.pop_front() {
                    self.processor
                        .allocate_specific_core(core_index, &dag[node_i]);

                    if node_i != source_node_i && node_i != sink_node_i {
                        let task_id = dag[node_i].id as usize;
                        self.node_logs[task_id].core_id = core_index;
                        self.node_logs[task_id].start_time = current_time - DUMMY_EXECUTION_TIME;
                        self.processor_log.core_logs[core_index].total_proc_time +=
                            dag[node_i].params.get("execution_time").unwrap_or(&0);
                    }
                    execution_order.push_back(node_i);
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
                    if let ProcessResult::Done(node_data) = result {
                        let node_id = node_data.id as usize;
                        let node_i = NodeIndex::new(node_id);
                        if node_i != source_node_i && node_i != sink_node_i {
                            self.node_logs[node_id].finish_time =
                                current_time - DUMMY_EXECUTION_TIME;
                        }
                        Some(node_i)
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
                    dag.increment_pre_done_count(suc_node);
                    if dag.is_node_ready(suc_node) {
                        self.ready_queue.push_back(suc_node);
                    }
                }
            }
        }

        //remove dummy nodes
        dag.remove_dummy_sink_node();
        dag.remove_dummy_source_node();

        //Remove the dummy source node from the execution order.
        execution_order.pop_back();
        //Remove the dummy sink node from the execution order.
        execution_order.pop_front();

        let schedule_length = current_time - DUMMY_EXECUTION_TIME * 2;
        self.processor_log
            .calculate_cores_utilization(schedule_length);

        self.processor_log.calculate_average_utilization();

        self.processor_log.calculate_variance_utilization();

        //Return the normalized total time taken to finish all tasks.
        (schedule_length, execution_order)
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
        let scheduler = NonPreemptiveScheduler::new(&dag, &processor);
        assert_eq!(scheduler.dag.node_count(), 0);
        assert_eq!(scheduler.processor.get_number_of_cores(), 1);
    }

    #[test]
    fn test_fixed_priority_scheduler_set_dag() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 0));
        let processor = HomogeneousProcessor::new(1);
        let mut scheduler = NonPreemptiveScheduler::new(&dag, &processor);
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
        let mut scheduler = NonPreemptiveScheduler::new(&dag, &processor);
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
            NonPreemptiveScheduler::new(&dag, &HomogeneousProcessor::new(2));
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
            NonPreemptiveScheduler::new(&dag, &HomogeneousProcessor::new(3));
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
            NonPreemptiveScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule();
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);

        let mut fixed_priority_scheduler =
            NonPreemptiveScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule();
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);
    }

    #[test]
    fn test_fixed_priority_scheduler_log_normal() {
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
            NonPreemptiveScheduler::new(&dag, &HomogeneousProcessor::new(2));
        fixed_priority_scheduler.schedule();

        assert_eq!(
            fixed_priority_scheduler.processor_log.average_utilization,
            0.61956525
        );

        assert_eq!(
            fixed_priority_scheduler.processor_log.variance_utilization,
            0.14473063
        );

        assert_eq!(
            fixed_priority_scheduler.processor_log.core_logs[0].core_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.processor_log.core_logs[0].total_proc_time,
            92
        );
        assert_eq!(
            fixed_priority_scheduler.processor_log.core_logs[0].utilization,
            1.0
        );

        assert_eq!(fixed_priority_scheduler.node_logs[0].core_id, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[0].dag_id, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[0].node_id, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[0].start_time, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[0].finish_time, 52);

        assert_eq!(fixed_priority_scheduler.node_logs[1].core_id, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[1].dag_id, 0);
        assert_eq!(fixed_priority_scheduler.node_logs[1].node_id, 1);
        assert_eq!(fixed_priority_scheduler.node_logs[1].start_time, 52);
        assert_eq!(fixed_priority_scheduler.node_logs[1].finish_time, 92);
    }
}
