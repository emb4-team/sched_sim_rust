use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    log::*,
    processor::ProcessorBase,
    util::{create_scheduler_log_yaml, get_process_core_indices},
};
use petgraph::graph::{Graph, NodeIndex};
use std::collections::VecDeque;

const DUMMY_EXECUTION_TIME: i32 = 1;

pub trait DAGSchedulerBase<T>
where
    T: ProcessorBase + Clone,
{
    // getter, setter
    fn set_dag(&mut self, dag: &Graph<NodeData, i32>);
    fn set_processor(&mut self, processor: &T);
    fn set_log(&mut self, log: DAGSchedulerLog);
    fn get_dag(&self) -> Graph<NodeData, i32>;
    fn get_processor(&self) -> T;
    fn get_log(&self) -> DAGSchedulerLog;
    // method definition
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self
    where
        Self: Sized;
    fn sort_ready_queue(ready_queue: &mut VecDeque<NodeData>);
    // method implementation
    fn schedule(&mut self) -> (i32, VecDeque<NodeIndex>) {
        {
            let mut dag = self.get_dag(); //To avoid adding pre_node_count to the original DAG
            let mut processor = self.get_processor();
            let mut ready_queue = VecDeque::new();
            let mut log = self.get_log();
            let mut execution_order = VecDeque::new();
            let source_node_i = dag.add_dummy_source_node();

            dag[source_node_i]
                .params
                .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);
            let sink_node_i = dag.add_dummy_sink_node();
            dag[sink_node_i]
                .params
                .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);

            ready_queue.push_back(dag[source_node_i].clone());

            let mut current_time = 0;
            loop {
                Self::sort_ready_queue(&mut ready_queue);

                // Assign the highest priority task first to the first idle core found.
                while let Some(core_index) = processor.get_idle_core_index() {
                    if let Some(node_d) = ready_queue.pop_front() {
                        processor.allocate_specific_core(core_index, &node_d);

                        if node_d.id != dag[source_node_i].id && node_d.id != dag[sink_node_i].id {
                            log.write_allocating_job(
                                &node_d,
                                core_index,
                                current_time - DUMMY_EXECUTION_TIME,
                            );
                        }
                        execution_order.push_back(NodeIndex::new(node_d.id as usize));
                    } else {
                        break;
                    }
                }

                // Move one unit time so that the core state of the previous loop does not remain.
                let mut process_result = processor.process();
                current_time += 1;
                // Write the processing time of the core to the log.
                let indices: Vec<usize> = get_process_core_indices(&process_result);
                log.write_processing_time(&indices);

                // Process until there is a task finished.
                while !process_result
                    .iter()
                    .any(|result| matches!(result, ProcessResult::Done(_)))
                {
                    process_result = processor.process();
                    current_time += 1;

                    // Write the processing time of the core to the log.
                    let indices: Vec<usize> = get_process_core_indices(&process_result);
                    log.write_processing_time(&indices)
                }

                let finish_nodes: Vec<NodeIndex> = process_result
                    .iter()
                    .enumerate()
                    .filter_map(|(core_id, result)| {
                        if let ProcessResult::Done(node_data) = result {
                            let node_id = node_data.id as usize;
                            let node_i = NodeIndex::new(node_id);
                            if node_i != source_node_i && node_i != sink_node_i {
                                log.write_finishing_job(
                                    node_data,
                                    core_id,
                                    current_time - DUMMY_EXECUTION_TIME,
                                );
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

                // Executable if all predecessor nodes are done
                for finish_node in finish_nodes {
                    let suc_nodes = dag.get_suc_nodes(finish_node).unwrap_or_default();
                    for suc_node in suc_nodes {
                        if dag[suc_node].params.contains_key("pre_done_count") {
                            dag.update_param(
                                suc_node,
                                "pre_done_count",
                                dag[suc_node].get_params_value("pre_done_count") + 1,
                            );
                        } else {
                            dag.add_param(suc_node, "pre_done_count", 1);
                        }
                        if dag.is_node_ready(suc_node) {
                            ready_queue.push_back(dag[suc_node].clone());
                        }
                    }
                }
            }

            // Remove dummy nodes
            dag.remove_dummy_sink_node();
            dag.remove_dummy_source_node();

            // Remove the dummy node from the execution order.
            execution_order.pop_back();
            execution_order.pop_front();

            let schedule_length = current_time - DUMMY_EXECUTION_TIME * 2;
            log.calculate_utilization(schedule_length);

            self.set_log(log);

            // Return the normalized total time taken to finish all tasks.
            (schedule_length, execution_order)
        }
    }

    fn dump_log(&self, dir_path: &str, alg_name: &str) -> String {
        let file_path = create_scheduler_log_yaml(dir_path, alg_name);
        self.get_log().dump_log_to_yaml(&file_path);

        file_path
    }
}
