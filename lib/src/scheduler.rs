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

#[derive(Clone, Default, PartialEq)]
pub enum DAGState {
    #[default]
    Waiting,
    Ready,
    Running,
}

pub trait DAGStateManagerBase {
    // getter, setter
    fn get_release_count(&self) -> i32;
    fn set_release_count(&mut self, release_count: i32);
    fn get_dag_state(&self) -> DAGState;
    fn set_dag_state(&mut self, dag_state: DAGState);

    // method definition
    fn complete_execution(&mut self);

    // method implementation
    fn release(&mut self) {
        self.set_release_count(self.get_release_count() + 1);
        self.set_dag_state(DAGState::Ready);
    }
}

#[macro_export]
macro_rules! getset_dag_state_manager {
    () => {
        fn get_release_count(&self) -> i32 {
            self.release_count
        }
        fn set_release_count(&mut self, release_count: i32) {
            self.release_count = release_count;
        }
        fn get_dag_state(&self) -> DAGState {
            self.dag_state.clone()
        }
        fn set_dag_state(&mut self, dag_state: DAGState) {
            self.dag_state = dag_state;
        }
    };
}

#[derive(Clone, Default)]
pub struct DAGStateManager {
    dag_state: DAGState,
    release_count: i32,
}

impl DAGStateManagerBase for DAGStateManager {
    getset_dag_state_manager!();

    fn complete_execution(&mut self) {
        self.set_dag_state(DAGState::Waiting);
    }
}

pub trait DAGSetSchedulerBase<T: ProcessorBase + Clone> {
    // getter, setter
    fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>>;
    fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>);
    fn get_processor_mut(&mut self) -> &mut T;
    fn get_log_mut(&mut self) -> &mut DAGSetSchedulerLog;
    fn get_current_time(&self) -> i32;
    fn set_current_time(&mut self, current_time: i32);

    // method definition
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &T) -> Self;

    // method implementation
    fn release_dags(&mut self, managers: &mut [impl DAGStateManagerBase]) -> Vec<NodeData> {
        let current_time = self.get_current_time();
        let mut ready_nodes = Vec::new();

        for dag in self.get_dag_set().iter() {
            let dag_id = dag.get_dag_id();
            if (managers[dag_id].get_dag_state() == DAGState::Waiting)
                && (current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap() * managers[dag_id].get_release_count())
            {
                ready_nodes.push(dag[dag.get_source_nodes()[0]].clone());
                managers[dag_id].release();
                self.get_log_mut()
                    .write_dag_release_time(dag_id, current_time);
            }
        }

        ready_nodes
    }

    fn allocate_node(&mut self, node: &NodeData, core_i: usize) {
        self.get_processor_mut()
            .allocate_specific_core(core_i, node);
        let current_time = self.get_current_time();
        self.get_log_mut().write_allocating_node(
            node.get_params_value("dag_id") as usize,
            node.get_id() as usize,
            core_i,
            current_time,
            node.get_params_value("execution_time"),
        );
    }

    fn process_unit_time(&mut self) -> Vec<ProcessResult> {
        self.set_current_time(self.get_current_time() + 1);
        self.get_processor_mut().process()
    }

    fn post_process_on_node_completion(
        &mut self,
        node: &NodeData,
        managers: &mut [impl DAGStateManagerBase],
    ) -> Vec<NodeData> {
        let mut dag_set = self.get_dag_set();
        let current_time = self.get_current_time();
        let log = self.get_log_mut();

        log.write_finishing_node(node, current_time);
        let dag_id = node.get_params_value("dag_id") as usize;
        let dag = &mut dag_set[dag_id];

        let mut ready_nodes = Vec::new();
        if let Some(suc_nodes) = dag.get_suc_nodes(NodeIndex::new(node.get_id() as usize)) {
            for suc_node in suc_nodes {
                dag.increment_pre_done_count(suc_node);
                if dag.is_node_ready(suc_node) {
                    ready_nodes.push(dag[suc_node].clone());
                }
            }
        } else {
            log.write_dag_finish_time(dag_id, current_time);
            dag.reset_pre_done_count();
            managers[dag_id].complete_execution();
        }

        self.set_dag_set(dag_set);

        ready_nodes
    }

    fn calculate_log(&mut self) {
        let current_time = self.get_current_time();
        let log = self.get_log_mut();
        log.calculate_utilization(current_time);
        log.calculate_response_time();
    }

    fn schedule(&mut self) -> i32 {
        // Start scheduling
        let mut managers = vec![DAGStateManager::default(); self.get_dag_set().len()];
        let mut ready_queue = BTreeSet::new();
        let hyper_period = get_hyper_period(&self.get_dag_set());
        while self.get_current_time() < hyper_period {
            // Release DAGs
            let ready_nodes = self.release_dags(&mut managers);
            for ready_node in ready_nodes {
                ready_queue.insert(NodeDataWrapper(ready_node));
            }

            // Allocate the nodes of ready_queue to idle cores
            while !ready_queue.is_empty() {
                match self.get_processor_mut().get_idle_core_index() {
                    Some(idle_core_index) => {
                        let ready_node_data = ready_queue.pop_first().unwrap().convert_node_data();
                        self.allocate_node(&ready_node_data, idle_core_index);
                    }
                    None => break,
                };
            }

            // Process unit time
            let process_result = self.process_unit_time();

            // Post-process on completion of node execution
            for result in process_result {
                if let ProcessResult::Done(node_data) = result {
                    let ready_nodes =
                        self.post_process_on_node_completion(&node_data, &mut managers);
                    for ready_node in ready_nodes {
                        ready_queue.insert(NodeDataWrapper(ready_node));
                    }
                }
            }
        }

        self.calculate_log();
        self.get_current_time()
    }

    fn dump_log(&mut self, dir_path: &str, alg_name: &str) -> String {
        let file_path = create_scheduler_log_yaml(dir_path, alg_name);
        self.get_log_mut().dump_log_to_yaml(&file_path);

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
        fn get_processor_mut(&mut self) -> &mut $t{
            &mut self.processor
        }
        fn get_log_mut(&mut self) -> &mut DAGSetSchedulerLog{
            &mut self.log
        }
        fn get_current_time(&self) -> i32{
            self.current_time
        }
        fn set_current_time(&mut self, current_time: i32){
            self.current_time = current_time;
        }
    }
}
