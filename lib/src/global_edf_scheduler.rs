use petgraph::graph::{Graph, NodeIndex};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::core::ProcessResult;
use crate::{
    graph_extension::{GraphExtension, NodeData},
    homogeneous::HomogeneousProcessor,
    log::DAGSetSchedulerLog,
    processor::ProcessorBase,
    scheduler::DAGSetSchedulerBase,
    util::get_hyper_period,
};

// Define a new wrapper type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDataWrapper(NodeData);

impl NodeDataWrapper {
    fn new(id: i32, params: BTreeMap<String, i32>) -> Self {
        Self(NodeData::new(id, params))
    }

    fn get_node_data(&self) -> NodeData {
        self.0.clone()
    }
}

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

#[derive(Clone, Debug)]
struct DAGStateManager {
    is_released: bool,
    is_started: bool,
    release_count: i32,
}

impl DAGStateManager {
    fn new() -> Self {
        Self {
            is_released: false,
            is_started: false,
            release_count: 0,
        }
    }

    fn release(&mut self) {
        self.is_released = true;
    }

    fn get_is_released(&self) -> bool {
        self.is_released
    }

    fn start(&mut self) {
        self.is_started = true;
    }

    fn get_is_started(&self) -> bool {
        self.is_started
    }

    fn reset_state(&mut self) {
        self.is_started = false;
        self.is_released = false;
    }

    fn get_release_count(&self) -> i32 {
        self.release_count
    }

    fn increment_release_count(&mut self) {
        self.release_count += 1;
    }
}

pub struct GlobalEDFScheduler {
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    log: DAGSetSchedulerLog,
}

impl DAGSetSchedulerBase<HomogeneousProcessor> for GlobalEDFScheduler {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
        }
    }

    fn schedule(&mut self) -> i32 {
        // Initialize DAGStateManagers
        let mut dag_state_managers = vec![DAGStateManager::new(); self.dag_set.len()];

        // Initialize DAG id
        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            // TODO : Establish indicators of priority
            let period = dag.get_head_period().unwrap();
            for node in dag.node_indices() {
                dag.add_param(node, "period", period);
            }
        }

        // Start scheduling
        let mut current_time = 0;
        let mut ready_queue: BTreeSet<NodeDataWrapper> = BTreeSet::new();
        let mut log = DAGSetSchedulerLog::new(&self.dag_set, self.processor.get_number_of_cores());
        let hyper_period = get_hyper_period(&self.dag_set);
        while current_time < hyper_period {
            // release DAGs
            for dag in self.dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap()
                            * dag_state_managers[dag_id].get_release_count()
                {
                    dag_state_managers[dag_id].release();
                    dag_state_managers[dag_id].increment_release_count();
                    log.write_dag_release_time(dag_id, current_time);
                }
            }

            // Start DAGs if there are free cores
            let mut idle_core_num = self.processor.get_idle_core_num();
            for (dag_id, manager) in dag_state_managers.iter_mut().enumerate() {
                if idle_core_num > 0 && !manager.get_is_started() && manager.get_is_released() {
                    manager.start();
                    idle_core_num -= 1;
                    // Add the source node to the ready queue
                    let dag = &self.dag_set[dag_id];
                    let source_node = &dag[dag.get_source_nodes()[0]];
                    ready_queue.insert(NodeDataWrapper::new(
                        source_node.id,
                        source_node.params.clone(),
                    ));
                    log.write_dag_start_time(dag_id, current_time);
                };
            }

            // Allocate the nodes of ready_queue to idle cores
            while !ready_queue.is_empty() {
                match self.processor.get_idle_core_index() {
                    Some(idle_core_index) => {
                        let ready_node_data = ready_queue.pop_first().unwrap().get_node_data();
                        self.processor
                            .allocate_specific_core(idle_core_index, &ready_node_data);
                        log.write_allocating_node(
                            ready_node_data.get_params_value("dag_id") as usize,
                            ready_node_data.id.try_into().unwrap(),
                            idle_core_index,
                            current_time,
                            ready_node_data.get_params_value("execution_time"),
                        );
                        self.processor
                            .allocate_specific_core(idle_core_index, &ready_node_data);
                    }
                    None => break,
                };
            }

            // Process unit time
            let process_result = self.processor.process();
            current_time += 1;

            // Post-process on completion of node execution
            for result in process_result.clone() {
                if let ProcessResult::Done(node_data) = result {
                    log.write_finishing_node(&node_data, current_time);
                    let dag_id = node_data.get_params_value("dag_id") as usize;

                    // Increase pre_done_count of successor nodes
                    let dag = &mut self.dag_set[dag_id];
                    let suc_nodes = dag
                        .get_suc_nodes(NodeIndex::new(node_data.id as usize))
                        .unwrap_or_default();
                    if suc_nodes.is_empty() {
                        log.write_dag_finish_time(dag_id, current_time);
                        // Reset the state of the DAG
                        for node_i in dag.node_indices() {
                            dag.update_param(node_i, "pre_done_count", 0);
                        }
                        dag_state_managers[dag_id].reset_state();
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
                    let dag = &mut self.dag_set[dag_id];
                    let suc_nodes = dag
                        .get_suc_nodes(NodeIndex::new(node_data.id as usize))
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
        self.set_log(log);

        current_time
    }

    fn get_log(&self) -> DAGSetSchedulerLog {
        self.log.clone()
    }

    fn set_log(&mut self, log: DAGSetSchedulerLog) {
        self.log = log;
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use crate::util::load_yaml;

    use super::*;

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

        let file_path = global_edf_scheduler.dump_log("../lib/tests", "test");
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
