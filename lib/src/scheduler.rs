use std::collections::VecDeque;

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    log::*,
    processor::ProcessorBase,
    util::create_yaml,
};
use chrono::{DateTime, Utc};
use petgraph::graph::{Graph, NodeIndex};

const DUMMY_EXECUTION_TIME: i32 = 1;

pub fn create_scheduler_log_yaml(dir_path: &str, alg_name: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
    let file_name = format!("{}-{}-log", date, alg_name);
    create_yaml(dir_path, &file_name)
}

pub trait DAGSchedulerBase<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self
    where
        Self: Sized;
    fn set_dag(&mut self, dag: &Graph<NodeData, i32>);
    fn set_processor(&mut self, processor: &T);
    fn set_log(&mut self, log: DAGSchedulerLog);
    fn get_dag(&self) -> Graph<NodeData, i32>;
    fn get_processor(&self) -> T;
    fn get_log(&self) -> DAGSchedulerLog;
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

                //Assign the highest priority task first to the first idle core found.
                while let Some(core_index) = processor.get_idle_core_index() {
                    if let Some(node_d) = ready_queue.pop_front() {
                        processor.allocate_specific_core(core_index, &node_d);

                        if node_d.id != dag[source_node_i].id && node_d.id != dag[sink_node_i].id {
                            log.write_allocating_node(
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

                //Move one unit time so that the core state of the previous loop does not remain.
                let mut process_result = processor.process();
                current_time += 1;

                //Process until there is a task finished.
                while !process_result
                    .iter()
                    .any(|result| matches!(result, ProcessResult::Done(_)))
                {
                    process_result = processor.process();
                    current_time += 1;
                }

                let finish_nodes: Vec<NodeIndex> = process_result
                    .iter()
                    .filter_map(|result| {
                        if let ProcessResult::Done(node_data) = result {
                            let node_id = node_data.id as usize;
                            let node_i = NodeIndex::new(node_id);
                            if node_i != source_node_i && node_i != sink_node_i {
                                log.write_finishing_node(
                                    node_data,
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

                //Executable if all predecessor nodes are done
                for finish_node in finish_nodes {
                    let suc_nodes = dag.get_suc_nodes(finish_node).unwrap_or_default();
                    for suc_node in suc_nodes {
                        dag.increment_pre_done_count(suc_node);
                        if dag.is_node_ready(suc_node) {
                            ready_queue.push_back(dag[suc_node].clone());
                        }
                    }
                }
            }

            //remove dummy nodes
            dag.remove_dummy_sink_node();
            dag.remove_dummy_source_node();

            //Remove the dummy node from the execution order.
            execution_order.pop_back();
            execution_order.pop_front();

            let schedule_length = current_time - DUMMY_EXECUTION_TIME * 2;
            log.calculate_utilization(schedule_length);

            self.set_log(log);

            //Return the normalized total time taken to finish all tasks.
            (schedule_length, execution_order)
        }
    }
    fn sort_ready_queue(ready_queue: &mut VecDeque<NodeData>);
    fn dump_log(&self, dir_path: &str, alg_name: &str) -> String {
        let file_path = create_scheduler_log_yaml(dir_path, alg_name);
        self.get_log().dump_log_to_yaml(&file_path);

        file_path
    }
}

// Define a new wrapper type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDataWrapper(pub NodeData);

impl NodeDataWrapper {
    pub fn convert_node_data(&self) -> NodeData {
        self.0.clone()
    }
}

#[derive(Clone, Default)]
pub struct DAGStateManager {
    pub release_count: i32,
    pub is_started: bool,
    pub is_released: bool,
}

impl DAGStateManager {
    pub fn new() -> Self {
        Self {
            release_count: Default::default(),
            is_started: Default::default(),
            is_released: Default::default(),
        }
    }

    pub fn get_release_count(&self) -> i32 {
        self.release_count
    }

    pub fn set_release_count(&mut self, release_count: i32) {
        self.release_count = release_count;
    }

    pub fn start(&mut self) {
        self.is_started = true;
    }

    pub fn get_is_started(&self) -> bool {
        self.is_started
    }

    pub fn can_start(&self) -> bool {
        !self.is_started && self.is_released
    }

    pub fn get_is_released(&self) -> bool {
        self.is_released
    }

    pub fn set_is_released(&mut self, is_released: bool) {
        self.is_released = is_released;
    }

    pub fn increment_release_count(&mut self) {
        self.release_count += 1;
    }

    pub fn reset_state(&mut self) {
        self.is_started = false;
        self.is_released = false;
    }

    pub fn release(&mut self) {
        self.is_released = true;
    }
}

pub trait DAGSetStateManagerBase {
    fn new(dag_set_len: usize) -> Self;
    fn get_release_count(&self, index: usize) -> i32;
    fn get_is_released(&self, index: usize) -> bool;
    fn get_is_started(&self, index: usize) -> bool;
    fn set_release_count(&mut self, index: usize, release_count: i32);
    fn set_is_released(&mut self, index: usize, is_released: bool);
    fn increment_release_count(&mut self, index: usize);
    fn start(&mut self, index: usize);
    fn can_start(&self, index: usize, idle_core_num: i32) -> bool;
    fn release(&mut self, index: usize);
    fn reset_state(&mut self, index: usize);
}

pub trait DAGSetSchedulerBase<T: ProcessorBase + Clone> {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &T) -> Self;
    fn get_log(&self) -> DAGSetSchedulerLog;
    fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>>;
    fn get_current_time(&self) -> i32;
    fn get_processor(&self) -> T;
    fn get_managers(&self) -> Vec<DAGStateManager>;
    fn set_log(&mut self, log: DAGSetSchedulerLog);
    fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>);
    fn set_current_time(&mut self, current_time: i32);
    fn set_processor(&mut self, processors: T);
    fn set_managers(&mut self, managers: Vec<DAGStateManager>);
    fn initialize(&mut self);
    fn start_dag(&mut self);
    fn release_dag(&mut self) {
        let mut dag_set = self.get_dag_set();
        let current_time = self.get_current_time();
        let mut managers = self.get_managers();
        let mut log = self.get_log();
        for dag in dag_set.iter_mut() {
            let dag_id = dag.get_dag_id();
            if current_time
                == dag.get_head_offset()
                    + dag.get_head_period().unwrap() * managers[dag_id].get_release_count()
            {
                managers[dag_id].release();
                managers[dag_id].increment_release_count();
                log.write_dag_release_time(dag_id, current_time);
            }
        }
        self.set_log(log);
        self.set_managers(managers);
    }
    fn calculate_idle_core_mun(&self) -> i32;
    fn allocate_node(&mut self);
    fn process_unit_time(&mut self) -> Vec<ProcessResult> {
        let mut current_time = self.get_current_time();
        current_time += 1;
        self.set_current_time(current_time);
        let mut processor = self.get_processor();
        let process_result = processor.process();
        self.set_processor(processor);
        process_result
    }
    fn handle_done_result(&mut self, process_result: &[ProcessResult]);
    fn handle_successor_nodes(&mut self, dag_id: usize, node_data: &NodeData) {
        let mut dag_set = self.get_dag_set();
        let mut log = self.get_log();
        let mut managers = self.get_managers();
        let dag = &mut dag_set[dag_id];
        let suc_nodes = dag
            .get_suc_nodes(NodeIndex::new(node_data.get_id() as usize))
            .unwrap_or_default();
        if suc_nodes.is_empty() {
            log.write_dag_finish_time(dag_id, self.get_current_time());
            // Reset the state of the DAG
            dag.reset_pre_done_count();
            managers[dag_id].reset_state();
        } else {
            for suc_node in suc_nodes {
                dag.increment_pre_done_count(suc_node);
            }
        }
        self.set_dag_set(dag_set);
        self.set_log(log);
        self.set_managers(managers);
    }
    fn reset_state(&mut self, dag_id: usize);
    fn calculate_log(&mut self) {
        let mut log = self.get_log();
        log.calculate_utilization(self.get_current_time());
        log.calculate_response_time();
        self.set_log(log);
    }
    fn schedule(&mut self) -> i32;
    fn dump_log(&self, dir_path: &str, alg_name: &str) -> String {
        let file_path = create_scheduler_log_yaml(dir_path, alg_name);
        self.get_log().dump_log_to_yaml(&file_path);
        file_path
    }
}
