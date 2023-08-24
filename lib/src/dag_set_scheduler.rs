use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    log::*,
    processor::ProcessorBase,
    util::{create_scheduler_log_yaml, get_hyper_period, get_process_core_indices},
};
use petgraph::graph::{Graph, NodeIndex};
use std::{cmp::Ordering, collections::BTreeSet};

// Define a new wrapper type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDataWrapper {
    pub node_data: NodeData,
}

impl Ord for NodeDataWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl NodeDataWrapper {
    pub fn convert_node_data(&self) -> NodeData {
        self.node_data.clone()
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
    // method implementation
    fn complete_execution(&mut self) {
        self.set_dag_state(DAGState::Waiting);
    }

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
}

pub enum PreemptiveType {
    NonPreemptive,
    Preemptive { key: String },
}

pub trait DAGSetSchedulerBase<T: ProcessorBase + Clone> {
    // getter, setter
    fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>>;
    fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>);
    fn get_processor_mut(&mut self) -> &mut T;
    fn get_processor(&self) -> &T;
    fn get_log_mut(&mut self) -> &mut DAGSetSchedulerLog;
    fn get_current_time(&self) -> i32;
    fn set_current_time(&mut self, current_time: i32);
    // method definition
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &T) -> Self;
    // method implementation
    fn release_dags(&mut self, managers: &mut [impl DAGStateManagerBase]) -> Vec<NodeData> {
        let current_time = self.get_current_time();
        let mut ready_nodes = Vec::new();
        let mut dag_set = self.get_dag_set();

        for dag in dag_set.iter_mut() {
            let dag_id = dag.get_dag_param("dag_id") as usize;
            if (managers[dag_id].get_dag_state() == DAGState::Waiting)
                && (current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap() * managers[dag_id].get_release_count())
            {
                managers[dag_id].release();
                dag.set_dag_param(
                    "absolute_deadline",
                    dag.get_head_period().unwrap() * managers[dag_id].get_release_count(),
                );
                ready_nodes.push(dag[dag.get_source_nodes()[0]].clone());
                self.get_log_mut()
                    .write_dag_release_time(dag_id, current_time);
            }
        }
        self.set_dag_set(dag_set);
        ready_nodes
    }

    fn allocate_node(&mut self, node: &NodeData, core_i: usize, job_i: usize) {
        self.get_processor_mut()
            .allocate_specific_core(core_i, node);
        let current_time = self.get_current_time();
        if node.params.contains_key("is_preempt") {
            self.get_log_mut().write_job_event(
                node,
                core_i,
                job_i - 1,
                JobEventTimes::ResumeTime(current_time),
            )
        } else {
            self.get_log_mut().write_job_event(
                node,
                core_i,
                job_i - 1,
                JobEventTimes::StartTime(current_time),
            )
        }
    }

    fn process_unit_time(&mut self) -> Vec<ProcessResult> {
        self.set_current_time(self.get_current_time() + 1);
        self.get_processor_mut().process()
    }

    fn post_process_on_node_completion(
        &mut self,
        node: &NodeData,
        core_id: usize,
        managers: &mut [impl DAGStateManagerBase],
    ) -> Vec<NodeData> {
        let mut dag_set = self.get_dag_set();
        let current_time = self.get_current_time();
        let log = self.get_log_mut();

        log.write_job_event(
            node,
            core_id,
            (managers[node.get_params_value("dag_id") as usize].get_release_count() - 1) as usize,
            JobEventTimes::FinishTime(current_time),
        );
        let dag_id = node.get_params_value("dag_id") as usize;
        let dag = &mut dag_set[dag_id];

        let mut ready_nodes = Vec::new();
        if let Some(suc_nodes) = dag.get_suc_nodes(NodeIndex::new(node.get_id() as usize)) {
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
                    ready_nodes.push(dag[suc_node].clone());
                }
            }
        } else {
            log.write_dag_finish_time(dag_id, current_time);
            dag.set_dag_param("pre_done_count", 0);
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

    fn can_preempt(
        &self,
        preemptive_type: &PreemptiveType,
        ready_head_node: &NodeDataWrapper,
    ) -> Option<usize> {
        if let PreemptiveType::Preemptive {
            key: preemptive_key,
        } = &preemptive_type
        {
            let (max_value, core_i) = self
                .get_processor()
                .get_max_value_and_index(preemptive_key)
                .unwrap();

            if max_value
                > ready_head_node
                    .convert_node_data()
                    .get_params_value(preemptive_key)
            {
                return Some(core_i);
            }
        }

        None
    }

    fn schedule(&mut self, preemptive_type: PreemptiveType) -> i32 {
        // Start scheduling
        let mut managers = vec![DAGStateManager::default(); self.get_dag_set().len()];
        let mut ready_queue = BTreeSet::new();
        let hyper_period = get_hyper_period(&self.get_dag_set());
        while self.get_current_time() < hyper_period {
            // Release DAGs
            let ready_nodes = self.release_dags(&mut managers);
            for ready_node in ready_nodes {
                ready_queue.insert(NodeDataWrapper {
                    node_data: ready_node,
                });
            }

            // Allocate nodes as long as there are idle cores, and attempt to preempt when all cores are busy.
            while !ready_queue.is_empty() {
                if let Some(idle_core_i) = self.get_processor().get_idle_core_index() {
                    // Allocate the node to the idle core
                    let node_data = ready_queue.pop_first().unwrap().convert_node_data();
                    self.allocate_node(
                        &node_data,
                        idle_core_i,
                        managers[node_data.get_params_value("dag_id") as usize].get_release_count()
                            as usize,
                    );
                } else if let Some(core_i) =
                    self.can_preempt(&preemptive_type, ready_queue.first().unwrap())
                {
                    // Preempt the node with the lowest priority
                    let current_time = self.get_current_time();
                    let processor = self.get_processor_mut();
                    // Preempted node data
                    let preempted_node_data = processor.preempt_execution(core_i).unwrap();
                    self.get_log_mut().write_job_event(
                        &preempted_node_data,
                        core_i,
                        (managers[preempted_node_data.get_params_value("dag_id") as usize]
                            .get_release_count() as usize)
                            - 1,
                        JobEventTimes::PreemptedTime(current_time),
                    );
                    // Allocate the preempted node
                    let allocate_node_data = &ready_queue.pop_first().unwrap().convert_node_data();
                    self.allocate_node(
                        allocate_node_data,
                        core_i,
                        managers[allocate_node_data.get_params_value("dag_id") as usize]
                            .get_release_count() as usize,
                    );
                    // Insert the preempted node into the ready queue
                    ready_queue.insert(NodeDataWrapper {
                        node_data: preempted_node_data,
                    });
                } else {
                    break; // No core is idle and can not preempt. Exit the loop.
                }
            }

            // Process unit time
            let process_result = self.process_unit_time();
            // TODO: Will be refactoring the core structure to have a core log.
            // Write the processing time of the core to the log.
            let log = self.get_log_mut();
            let indices: Vec<usize> = get_process_core_indices(&process_result);
            log.write_processing_time(&indices);

            // Post-process on completion of node execution
            for (core_id, result) in process_result.iter().enumerate() {
                if let ProcessResult::Done(node_data) = result {
                    let ready_nodes =
                        self.post_process_on_node_completion(node_data, core_id, &mut managers);
                    for ready_node in ready_nodes {
                        ready_queue.insert(NodeDataWrapper {
                            node_data: ready_node,
                        });
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
        fn get_processor(&self) -> &$t{
            &self.processor
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
