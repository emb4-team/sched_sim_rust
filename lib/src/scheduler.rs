use std::collections::{BTreeSet, VecDeque};

use crate::util::get_hyper_period;
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

pub trait DAGStateManagerBase {
    // getter, setter
    fn get_is_started(&self) -> bool;
    fn set_is_started(&mut self, is_started: bool);
    fn get_is_released(&self) -> bool;
    fn set_is_released(&mut self, is_released: bool);
    fn get_release_count(&self) -> i32;
    fn set_release_count(&mut self, release_count: i32);

    // method implementation
    fn start(&mut self) {
        self.set_is_started(true);
    }

    fn can_start(&self) -> bool {
        !self.get_is_started() && self.get_is_released()
    }

    fn increment_release_count(&mut self) {
        self.set_release_count(self.get_release_count() + 1);
    }

    fn reset_state(&mut self) {
        self.set_is_started(false);
        self.set_is_released(false);
    }

    fn release(&mut self) {
        self.set_is_released(true);
    }
}

#[macro_export]
macro_rules! getset_dag_state_manager {
    () => {
        fn get_is_started(&self) -> bool {
            self.is_started
        }
        fn set_is_started(&mut self, is_started: bool) {
            self.is_started = is_started;
        }
        fn get_is_released(&self) -> bool {
            self.is_released
        }
        fn set_is_released(&mut self, is_released: bool) {
            self.is_released = is_released;
        }
        fn get_release_count(&self) -> i32 {
            self.release_count
        }
        fn set_release_count(&mut self, release_count: i32) {
            self.release_count = release_count;
        }
    };
}

#[derive(Clone, Default)]
pub struct DAGStateManager {
    release_count: i32,
    is_started: bool,
    is_released: bool,
}

impl DAGStateManagerBase for DAGStateManager {
    getset_dag_state_manager!();
}

pub trait DAGSetSchedulerBase<T: ProcessorBase + Clone> {
    // getter, setter
    fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>>;
    fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>);
    fn get_processor(&self) -> T;
    fn set_processor(&mut self, processor: T);
    fn get_log(&self) -> DAGSetSchedulerLog;
    fn set_log(&mut self, log: DAGSetSchedulerLog);

    // method definition
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &T) -> Self;

    // method implementation
    fn schedule(&mut self) -> i32 {
        let mut dag_set = self.get_dag_set();
        let mut processor = self.get_processor();
        let mut current_time = 0;
        let mut managers = vec![DAGStateManager::default(); dag_set.len()];
        let mut log = self.get_log();
        let mut ready_queue = BTreeSet::new();

        // Initialize DAGSet and DAGStateManagers
        for (dag_id, dag) in dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            dag.set_dag_period(dag.get_head_period().unwrap());
        }

        // Start scheduling
        let hyper_period = get_hyper_period(&dag_set);
        while current_time < hyper_period {
            // Release DAGs
            for dag in dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap() * managers[dag_id].release_count
                {
                    managers[dag_id].release();
                    managers[dag_id].increment_release_count();
                    log.write_dag_release_time(dag_id, current_time);
                }
            }

            // Start DAGs if there are free cores
            let mut idle_core_num = processor.get_idle_core_num();
            for (dag_id, manager) in managers.iter_mut().enumerate() {
                if manager.can_start() && idle_core_num > 0 {
                    manager.start();
                    idle_core_num -= 1;
                    let dag = &dag_set[dag_id];
                    let source_node = &dag[dag.get_source_nodes()[0]];
                    ready_queue.insert(NodeDataWrapper(source_node.clone()));
                    log.write_dag_start_time(dag_id, current_time);
                }
            }

            // Allocate the nodes of ready_queue to idle cores
            while !ready_queue.is_empty() {
                match processor.get_idle_core_index() {
                    Some(idle_core_index) => {
                        let ready_node_data = ready_queue.pop_first().unwrap().convert_node_data();
                        processor.allocate_specific_core(idle_core_index, &ready_node_data);
                        log.write_allocating_node(
                            ready_node_data.get_params_value("dag_id") as usize,
                            ready_node_data.get_id() as usize,
                            idle_core_index,
                            current_time,
                            ready_node_data.get_params_value("execution_time"),
                        );
                    }
                    None => break,
                };
            }

            // Process unit time
            current_time += 1;
            let process_result = processor.process();

            // Post-process on completion of node execution
            for result in process_result.clone() {
                if let ProcessResult::Done(node_data) = result {
                    log.write_finishing_node(&node_data, current_time);
                    let dag_id = node_data.get_params_value("dag_id") as usize;
                    // Increase pre_done_count of successor nodes
                    let dag = &mut dag_set[dag_id];
                    let suc_nodes = dag
                        .get_suc_nodes(NodeIndex::new(node_data.get_id() as usize))
                        .unwrap_or_default();
                    if suc_nodes.is_empty() {
                        log.write_dag_finish_time(dag_id, current_time);
                        // Reset the state of the DAG
                        dag.reset_pre_done_count();
                        managers[dag_id].reset_state();
                    } else {
                        for suc_node in suc_nodes {
                            dag.increment_pre_done_count(suc_node);
                        }
                    }
                }
            }

            // Add the node to the ready queue when all preceding nodes have finished
            for result in process_result {
                if let ProcessResult::Done(node_data) = result {
                    let dag_id = node_data.get_params_value("dag_id") as usize;
                    let dag = &mut dag_set[dag_id];
                    let suc_nodes = dag
                        .get_suc_nodes(NodeIndex::new(node_data.get_id() as usize))
                        .unwrap_or_default();

                    // If all preceding nodes have finished, add the node to the ready queue
                    for suc_node in suc_nodes {
                        if dag.is_node_ready(suc_node) {
                            ready_queue.insert(NodeDataWrapper(dag[suc_node].clone()));
                        }
                    }
                }
            }
        }

        log.calculate_utilization(current_time);
        log.calculate_response_time();
        self.set_log(log);
        current_time
    }

    fn dump_log(&self, dir_path: &str, alg_name: &str) -> String {
        let file_path = create_scheduler_log_yaml(dir_path, alg_name);
        self.get_log().dump_log_to_yaml(&file_path);

        file_path
    }
}

#[macro_export]
macro_rules! getset_dag_set_scheduler {
    { $t:ty } => {
        fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>>{
            self.dag_set.clone()
        }
        fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>){
            self.dag_set = dag_set;
        }
        fn get_processor(&self) -> $t{
            self.processor.clone()
        }
        fn set_processor(&mut self, processor: $t){
            self.processor = processor;
        }
        fn get_log(&self) -> DAGSetSchedulerLog{
            self.log.clone()
        }
        fn set_log(&mut self, log: DAGSetSchedulerLog){
            self.log = log;
        }
    }
}
