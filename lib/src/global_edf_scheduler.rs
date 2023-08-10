use petgraph::graph::{Graph, NodeIndex};

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::core::ProcessResult;
use crate::scheduler::{DAGSetStateManagerBase, NodeDataWrapper};
use crate::util::get_hyper_period;
use crate::{
    graph_extension::{GraphExtension, NodeData},
    homogeneous::HomogeneousProcessor,
    log::DAGSetSchedulerLog,
    processor::ProcessorBase,
    scheduler::DAGSetSchedulerBase,
};

impl PartialOrd for NodeDataWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Define the keys to compare
        let key1 = "period";
        let key2 = "dag_id";
        match (self.0.params.get(key1), other.0.params.get(key1)) {
            (Some(self_val), Some(other_val)) => match self_val.cmp(other_val) {
                // If the keys are equal, compare by id
                Ordering::Equal => match self.0.id.partial_cmp(&other.0.id) {
                    // If the ids are also equal, compare by dag_id
                    Some(Ordering::Equal) => {
                        match (self.0.params.get(key2), other.0.params.get(key2)) {
                            (Some(self_dag), Some(other_dag)) => Some(self_dag.cmp(other_dag)),
                            (None, None) => Some(Ordering::Equal),
                            (Some(_), None) => Some(Ordering::Greater),
                            (None, Some(_)) => Some(Ordering::Less),
                        }
                    }
                    other => other,
                },
                other => Some(other),
            },
            // If neither of the keys exists, compare by id
            (None, None) => match self.0.id.partial_cmp(&other.0.id) {
                // If the ids are equal, compare by dag_id
                Some(Ordering::Equal) => {
                    match (self.0.params.get(key2), other.0.params.get(key2)) {
                        (Some(self_dag), Some(other_dag)) => Some(self_dag.cmp(other_dag)),
                        (None, None) => Some(Ordering::Equal),
                        (Some(_), None) => Some(Ordering::Greater),
                        (None, Some(_)) => Some(Ordering::Less),
                    }
                }
                other => other,
            },
            // If only one of the keys exists, the one with the key is greater
            (Some(_), None) => Some(Ordering::Greater),
            (None, Some(_)) => Some(Ordering::Less),
        }
    }
}

impl Ord for NodeDataWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[derive(Clone)]
pub struct DAGSetStateManager {
    managers: Vec<DAGStateManager>,
}

impl DAGSetStateManagerBase for DAGSetStateManager {
    fn new(dag_set_len: usize) -> Self {
        let managers = vec![DAGStateManager::new(); dag_set_len];
        Self { managers }
    }

    fn get_release_count(&self, index: usize) -> i32 {
        self.managers[index].get_release_count()
    }

    fn set_release_count(&mut self, index: usize, release_count: i32) {
        self.managers[index].set_release_count(release_count);
    }

    fn start(&mut self, index: usize) {
        self.managers[index].start();
    }

    fn get_is_started(&self, index: usize) -> bool {
        self.managers[index].get_is_started()
    }

    fn can_start(&self, index: usize, idle_core_num: i32) -> bool {
        self.managers[index].can_start(idle_core_num)
    }

    fn get_is_released(&self, index: usize) -> bool {
        self.managers[index].get_is_released()
    }

    fn set_is_released(&mut self, index: usize, is_released: bool) {
        self.managers[index].set_is_released(is_released);
    }

    fn increment_release_count(&mut self, index: usize) {
        self.managers[index].increment_release_count();
    }

    fn reset_state(&mut self, index: usize) {
        self.managers[index].reset_state();
    }

    fn release(&mut self, index: usize) {
        self.managers[index].release();
    }
}

#[derive(Clone)]
struct DAGStateManager {
    release_count: i32,
    is_started: bool,
    is_released: bool,
}

impl DAGStateManager {
    fn new() -> Self {
        Self {
            release_count: 0,
            is_started: false,
            is_released: false,
        }
    }

    fn get_release_count(&self) -> i32 {
        self.release_count
    }

    fn set_release_count(&mut self, release_count: i32) {
        self.release_count = release_count;
    }

    fn start(&mut self) {
        self.is_started = true;
    }

    fn get_is_started(&self) -> bool {
        self.is_started
    }

    fn can_start(&self, idle_core_num: i32) -> bool {
        !self.is_started && self.is_released && idle_core_num > 0
    }

    fn get_is_released(&self) -> bool {
        self.is_released
    }

    fn set_is_released(&mut self, is_released: bool) {
        self.is_released = is_released;
    }

    fn increment_release_count(&mut self) {
        self.release_count += 1;
    }

    fn reset_state(&mut self) {
        self.is_started = false;
        self.is_released = false;
    }

    fn release(&mut self) {
        self.is_released = true;
    }
}

pub struct GlobalEDFScheduler {
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    current_time: i32,
    log: DAGSetSchedulerLog,
    managers: DAGSetStateManager,
    ready_queue: BTreeSet<NodeDataWrapper>,
}

impl DAGSetSchedulerBase<HomogeneousProcessor, DAGSetStateManager> for GlobalEDFScheduler {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            current_time: 0,
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
            managers: DAGSetStateManager::new(dag_set.len()),
            ready_queue: BTreeSet::new(),
        }
    }

    fn get_log(&self) -> DAGSetSchedulerLog {
        self.log.clone()
    }

    fn get_dag_set(&self) -> Vec<Graph<NodeData, i32>> {
        self.dag_set.clone()
    }

    fn get_current_time(&self) -> i32 {
        self.current_time
    }

    fn get_processor(&self) -> HomogeneousProcessor {
        self.processor.clone()
    }

    fn get_managers(&self) -> DAGSetStateManager {
        self.managers.clone()
    }

    fn set_log(&mut self, log: DAGSetSchedulerLog) {
        self.log = log;
    }

    fn set_dag_set(&mut self, dag_set: Vec<Graph<NodeData, i32>>) {
        self.dag_set = dag_set;
    }

    fn set_managers(&mut self, managers: DAGSetStateManager) {
        self.managers = managers;
    }

    fn set_processor(&mut self, processor: HomogeneousProcessor) {
        self.processor = processor;
    }

    fn set_current_time(&mut self, current_time: i32) {
        self.current_time = current_time;
    }

    fn initialize(&mut self) {
        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            dag.set_dag_period(dag.get_head_period().unwrap());
        }
    }

    fn start_dag(&mut self) {
        let mut idle_core_num = self.calculate_idle_core_mun();
        for (dag_id, manager) in self.managers.managers.iter_mut().enumerate() {
            if manager.can_start(idle_core_num) {
                manager.start();
                idle_core_num -= 1;
                let dag = &self.dag_set[dag_id];
                let source_node = &dag[dag.get_source_nodes()[0]];
                self.ready_queue
                    .insert(NodeDataWrapper(source_node.clone()));
                self.log.write_dag_start_time(dag_id, self.current_time);
            }
        }
    }

    fn calculate_idle_core_mun(&self) -> i32 {
        self.processor.get_idle_core_num() as i32
    }

    fn allocate_node(&mut self) {
        while !self.ready_queue.is_empty() {
            match self.processor.get_idle_core_index() {
                Some(idle_core_index) => {
                    let ready_node_data = self.ready_queue.pop_first().unwrap().convert_node_data();
                    self.processor
                        .allocate_specific_core(idle_core_index, &ready_node_data);
                    self.log.write_allocating_node(
                        ready_node_data.get_params_value("dag_id") as usize,
                        ready_node_data.get_id() as usize,
                        idle_core_index,
                        self.current_time,
                        ready_node_data.get_params_value("execution_time"),
                    );
                }
                None => break,
            };
        }
    }

    fn handle_done_result(&mut self, process_result: &[ProcessResult]) {
        for result in process_result.clone() {
            if let ProcessResult::Done(node_data) = result {
                self.log.write_finishing_node(node_data, self.current_time);
                let dag_id = node_data.get_params_value("dag_id") as usize;
                // Increase pre_done_count of successor nodes
                self.handle_successor_nodes(dag_id, node_data);
            }
        }
    }

    fn schedule(&mut self) -> i32 {
        // Initialize DAGSet and DAGStateManagers
        self.initialize();

        // Start scheduling
        let hyper_period = get_hyper_period(&self.dag_set);

        while self.current_time < hyper_period {
            // Release DAGs
            self.release_dag();

            // Start DAGs if there are free cores
            self.start_dag();
            // Allocate the nodes of ready_queue to idle cores
            self.allocate_node();
            // Process unit time
            let process_result = self.process_unit_time();
            // Post-process on completion of node execution
            self.handle_done_result(&process_result);
            // Add the node to the ready queue when all preceding nodes have finished
            self.insert_ready_node(&process_result);
        }
        self.calculate_log();
        self.current_time
    }
}

impl GlobalEDFScheduler {
    fn insert_ready_node(&mut self, process_result: &[ProcessResult]) {
        for result in process_result {
            if let ProcessResult::Done(node_data) = result {
                let dag_id = node_data.get_params_value("dag_id") as usize;
                let dag = &mut self.dag_set[dag_id];
                let suc_nodes = dag
                    .get_suc_nodes(NodeIndex::new(node_data.get_id() as usize))
                    .unwrap_or_default();

                // If all preceding nodes have finished, add the node to the ready queue
                for suc_node in suc_nodes {
                    if dag.is_node_ready(suc_node) {
                        self.ready_queue
                            .insert(NodeDataWrapper(dag[suc_node].clone()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::load_yaml;
    use std::{collections::BTreeMap, fs::remove_file};

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        // cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 150);
        dag.add_param(c2, "end_to_end_deadline", 50);
        // nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 10));

        // Create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        // Create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);
        dag.add_edge(n0_0, c2, 1);
        dag.add_edge(n1_0, c2, 1);

        dag
    }

    fn create_sample_dag2() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        // cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 100);
        dag.add_param(c2, "end_to_end_deadline", 60);
        // nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));

        // Create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        // Create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(n0_0, c2, 1);

        dag
    }

    #[test]
    fn test_global_edf_normal() {
        let dag = create_sample_dag();
        let dag2 = create_sample_dag2();
        let dag_set = vec![dag, dag2];

        let processor = HomogeneousProcessor::new(4);

        let mut global_edf_scheduler = GlobalEDFScheduler::new(&dag_set, &processor);
        let time = global_edf_scheduler.schedule();

        assert_eq!(time, 300);

        let file_path = global_edf_scheduler.dump_log("../lib/tests", "edf_test");
        let yaml_docs = load_yaml(&file_path);
        let yaml_doc = &yaml_docs[0];

        // Check the value of total_utilization
        assert_eq!(
            yaml_doc["dag_set_info"]["total_utilization"]
                .as_f64()
                .unwrap(),
            3.8095236
        );

        // Check the value of each_dag_info
        let each_dag_info = &yaml_doc["dag_set_info"]["each_dag_info"][0];
        assert_eq!(each_dag_info["critical_path_length"].as_i64().unwrap(), 50);
        assert_eq!(each_dag_info["period"].as_i64().unwrap(), 150);
        assert_eq!(each_dag_info["end_to_end_deadline"].as_i64().unwrap(), 50);
        assert_eq!(each_dag_info["volume"].as_i64().unwrap(), 70);
        assert_eq!(each_dag_info["utilization"].as_f64().unwrap(), 2.142857);

        // Check the value of processor_info
        assert_eq!(
            yaml_doc["processor_info"]["number_of_cores"]
                .as_i64()
                .unwrap(),
            4
        );

        // Check the value of dag_set_log
        let dag_set_log = &yaml_doc["dag_set_log"][0];
        assert_eq!(dag_set_log["dag_id"].as_i64().unwrap(), 0);
        let release_time = &dag_set_log["release_time"];
        assert_eq!(release_time[0].as_i64().unwrap(), 0);
        assert_eq!(release_time[1].as_i64().unwrap(), 150);
        let start_time = &dag_set_log["start_time"];
        assert_eq!(start_time[0].as_i64().unwrap(), 0);
        assert_eq!(start_time[1].as_i64().unwrap(), 150);
        let finish_time = &dag_set_log["finish_time"];
        assert_eq!(finish_time[0].as_i64().unwrap(), 50);
        assert_eq!(finish_time[1].as_i64().unwrap(), 200);

        // Check the value of node_set_logs
        let node_set_logs = &yaml_doc["node_set_logs"][0][0];
        let core_id = &node_set_logs["core_id"];
        assert_eq!(core_id[0].as_i64().unwrap(), 1);
        assert_eq!(core_id[1].as_i64().unwrap(), 0);
        assert_eq!(node_set_logs["dag_id"].as_i64().unwrap(), 0);
        assert_eq!(node_set_logs["node_id"].as_i64().unwrap(), 0);
        let start_time = &node_set_logs["start_time"];
        assert_eq!(start_time[0].as_i64().unwrap(), 0);
        assert_eq!(start_time[1].as_i64().unwrap(), 150);
        let finish_time = &node_set_logs["finish_time"];
        assert_eq!(finish_time[0].as_i64().unwrap(), 10);
        assert_eq!(finish_time[1].as_i64().unwrap(), 160);

        // Check the value of processor_log
        let processor_log = &yaml_doc["processor_log"];
        assert_eq!(
            processor_log["average_utilization"].as_f64().unwrap(),
            0.26666668
        );
        assert_eq!(
            processor_log["variance_utilization"].as_f64().unwrap(),
            0.06055556
        );

        // Check the value of core_logs
        let core_logs = &processor_log["core_logs"][0];
        assert_eq!(core_logs["core_id"].as_i64().unwrap(), 0);
        assert_eq!(core_logs["total_proc_time"].as_i64().unwrap(), 200);
        assert_eq!(core_logs["utilization"].as_f64().unwrap(), 0.6666667);

        remove_file(file_path).unwrap();
    }
}
