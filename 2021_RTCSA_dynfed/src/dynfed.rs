//! dynfed.
//! Paper Information
//! -----------------
//! Title: Timing-Anomaly Free Dynamic Scheduling of Periodic DAG Tasks with Non-Preemptive
//! Authors: Gaoyang Dai, Morteza Mohaqeqi, and Wang Yi
//! Conference: RTCSA 2021
//! -----------------

use std::collections::VecDeque;

use lib::core::ProcessResult;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::log::*;
use lib::processor::ProcessorBase;
use lib::scheduler::*;
use lib::util::get_hyper_period;
use petgraph::{graph::NodeIndex, Graph};

/// Calculate the execution order when minimum number of cores required to meet the end-to-end deadline.
///
/// # Arguments
///
/// * `dag` - The DAG to be scheduled.
///
/// # Returns
///
/// * The minimum number of cores required to meet the end-to-end deadline.
/// * A vector of NodeIndex, representing the execution order of the tasks.
///
/// # Description
///
/// This function calculates the minimum number of cores required to meet the end-to-end deadline of the DAG.
/// In addition, it returns the execution order of the tasks when the minimum number of cores are used.
///
/// # Example
///
/// Refer to the examples in the tests code.
///
fn calculate_minimum_cores_and_execution_order<T>(
    dag: &mut Graph<NodeData, i32>,
    scheduler: &mut impl DAGSchedulerBase<T>,
) -> (usize, VecDeque<NodeIndex>)
where
    T: ProcessorBase + Clone,
{
    let volume = dag.get_volume();
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
    let mut minimum_cores = (volume as f32 / end_to_end_deadline as f32).ceil() as usize;

    scheduler.set_dag(dag);
    scheduler.set_processor(&T::new(minimum_cores));

    let (mut schedule_length, mut execution_order) = scheduler.schedule();

    while schedule_length > end_to_end_deadline {
        minimum_cores += 1;
        scheduler.set_processor(&T::new(minimum_cores));
        (schedule_length, execution_order) = scheduler.schedule();
    }

    (minimum_cores, execution_order)
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

impl DAGSetStateManager {
    fn set_minimum_cores(&mut self, index: usize, minimum_cores: i32) {
        self.managers[index].set_minimum_cores(minimum_cores);
    }

    fn set_execution_order(&mut self, index: usize, execution_order: VecDeque<NodeIndex>) {
        self.managers[index].set_execution_order(execution_order);
    }

    fn get_total_allocated_cores(&self) -> i32 {
        let mut total_allocated_cores = 0;
        for manager in self.managers.iter() {
            total_allocated_cores += manager.num_allocated_cores;
        }
        total_allocated_cores
    }

    fn get_execution_order_head(&self, index: usize) -> Option<&NodeIndex> {
        self.managers[index].get_execution_order_head()
    }

    fn get_unused_cores(&self, index: usize) -> i32 {
        self.managers[index].get_unused_cores()
    }

    fn allocate_head(&mut self, index: usize) -> NodeIndex {
        self.managers[index].allocate_head()
    }

    fn decrement_num_using_cores(&mut self, index: usize) {
        self.managers[index].decrement_num_using_cores();
    }
}

#[derive(Clone)]
struct DAGStateManager {
    release_count: i32,
    is_started: bool,
    is_released: bool,
    num_using_cores: i32,
    num_allocated_cores: i32,
    minimum_cores: i32,
    execution_order: VecDeque<NodeIndex>,
    initial_execution_order: VecDeque<NodeIndex>,
}

impl DAGStateManager {
    fn new() -> Self {
        Self {
            release_count: 0,
            is_started: false,
            is_released: false,
            num_using_cores: 0,
            num_allocated_cores: 0,
            minimum_cores: 0,
            execution_order: VecDeque::new(),
            initial_execution_order: VecDeque::new(),
        }
    }

    fn get_release_count(&self) -> i32 {
        self.release_count
    }

    fn set_release_count(&mut self, release_count: i32) {
        self.release_count = release_count;
    }

    fn get_is_started(&self) -> bool {
        self.is_started
    }

    fn start(&mut self) {
        self.is_started = true;
        self.num_allocated_cores = self.minimum_cores;
    }

    fn can_start(&self, idle_core_num: i32) -> bool {
        !self.is_started && self.is_released && (idle_core_num >= self.minimum_cores)
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
        self.set_execution_order(self.initial_execution_order.clone());
        self.free_allocated_cores(); //When the last node is finished, the core allocated to dag is released.
    }

    fn set_execution_order(&mut self, initial_execution_order: VecDeque<NodeIndex>) {
        self.initial_execution_order = initial_execution_order.clone();
        self.execution_order = initial_execution_order;
    }

    fn release(&mut self) {
        self.is_released = true;
    }

    fn free_allocated_cores(&mut self) {
        self.num_allocated_cores = 0;
    }

    fn get_minimum_cores(&self) -> i32 {
        self.minimum_cores
    }

    fn set_minimum_cores(&mut self, minimum_cores: i32) {
        self.minimum_cores = minimum_cores;
    }

    fn get_execution_order_head(&self) -> Option<&NodeIndex> {
        self.execution_order.front()
    }

    fn get_unused_cores(&self) -> i32 {
        self.num_allocated_cores - self.num_using_cores
    }

    fn allocate_head(&mut self) -> NodeIndex {
        self.num_using_cores += 1;
        self.execution_order.pop_front().unwrap()
    }

    fn decrement_num_using_cores(&mut self) {
        self.num_using_cores -= 1;
    }
}

pub struct DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    scheduler: T,
    current_time: i32,
    log: DAGSetSchedulerLog,
    managers: DAGSetStateManager,
}

impl<T> DAGSetSchedulerBase<HomogeneousProcessor, DAGSetStateManager>
    for DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            scheduler: T::new(&Graph::<NodeData, i32>::new(), processor),
            current_time: 0,
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
            managers: DAGSetStateManager::new(dag_set.len()),
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
            let (minimum_cores, execution_order) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);

            self.managers
                .set_minimum_cores(dag_id, minimum_cores as i32);
            self.managers.set_execution_order(dag_id, execution_order);
        }
    }

    fn start_dag(&mut self) {
        let mut idle_core_num = self.calculate_idle_core_mun();
        for (dag_id, manager) in self.managers.managers.iter_mut().enumerate() {
            if manager.can_start(idle_core_num) {
                manager.start();
                idle_core_num -= manager.get_minimum_cores();
                self.log.write_dag_start_time(dag_id, self.current_time);
            }
        }
    }

    fn calculate_idle_core_mun(&self) -> i32 {
        self.processor.get_number_of_cores() as i32 - self.managers.get_total_allocated_cores()
    }

    fn allocate_node(&mut self) {
        for dag in self.dag_set.iter() {
            let dag_id = dag.get_dag_id();
            if !self.managers.get_is_started(dag_id) {
                continue;
            }

            while let Some(node_i) = self.managers.get_execution_order_head(dag_id) {
                if dag.is_node_ready(*node_i) && self.managers.get_unused_cores(dag_id) > 0 {
                    let core_id = self.processor.get_idle_core_index().unwrap();
                    self.log.write_allocating_node(
                        dag_id,
                        node_i.index(),
                        core_id,
                        self.current_time,
                        *dag[*node_i].params.get("execution_time").unwrap(),
                    );
                    self.processor
                        .allocate_specific_core(core_id, &dag[self.managers.allocate_head(dag_id)]);
                } else {
                    break;
                }
            }
        }
    }

    fn handle_done_result(&mut self, process_result: &[ProcessResult]) {
        for result in process_result.clone() {
            if let ProcessResult::Done(node_data) = result {
                self.log.write_finishing_node(node_data, self.current_time);
                let dag_id = node_data.get_params_value("dag_id") as usize;
                self.managers.decrement_num_using_cores(dag_id);

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
        }
        self.calculate_log();

        self.current_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::fixed_priority_scheduler::FixedPriorityScheduler;
    use lib::homogeneous::HomogeneousProcessor;
    use lib::processor::ProcessorBase;
    use lib::util::load_yaml;
    use std::collections::BTreeMap;
    use std::fs::remove_file;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 150);
        dag.add_param(c2, "end_to_end_deadline", 50);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 10));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);
        dag.add_edge(n0_0, c2, 1);
        dag.add_edge(n1_0, c2, 1);

        dag
    }

    fn create_sample_dag2() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 11));
        let c1 = dag.add_node(create_node(1, "execution_time", 21));
        let c2 = dag.add_node(create_node(2, "execution_time", 21));
        dag.add_param(c0, "period", 100);
        dag.add_param(c2, "end_to_end_deadline", 60);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 11));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(n0_0, c2, 1);

        dag
    }

    #[test]
    fn test_dynfed_normal() {
        let dag = create_sample_dag();
        let dag2 = create_sample_dag2();
        let dag_set = vec![dag, dag2];

        let mut dynfed: DynamicFederatedScheduler<FixedPriorityScheduler<HomogeneousProcessor>> =
            DynamicFederatedScheduler::new(&dag_set, &HomogeneousProcessor::new(5));
        let time = dynfed.schedule();
        assert_eq!(time, 300);

        let file_path = dynfed.dump_log("../lib/tests", "dyn_test");
        let yaml_docs = load_yaml(&file_path);
        let yaml_doc = &yaml_docs[0];

        assert_eq!(
            yaml_doc["dag_set_info"]["total_utilization"]
                .as_f64()
                .unwrap(),
            3.705357
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][0]["critical_path_length"]
                .as_i64()
                .unwrap(),
            50
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][0]["period"]
                .as_i64()
                .unwrap(),
            150
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][0]["end_to_end_deadline"]
                .as_i64()
                .unwrap(),
            50
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][0]["volume"]
                .as_i64()
                .unwrap(),
            70
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][0]["utilization"]
                .as_f64()
                .unwrap(),
            2.142857
        );
        assert_eq!(
            yaml_doc["dag_set_info"]["each_dag_info"][1]["utilization"]
                .as_f64()
                .unwrap(),
            1.5625
        );

        assert_eq!(
            yaml_doc["processor_info"]["number_of_cores"]
                .as_i64()
                .unwrap(),
            5
        );

        assert_eq!(yaml_doc["dag_set_log"][1]["dag_id"].as_i64().unwrap(), 1);
        assert_eq!(
            yaml_doc["dag_set_log"][1]["release_time"][0]
                .as_i64()
                .unwrap(),
            0
        );
        assert_eq!(
            yaml_doc["dag_set_log"][1]["start_time"][0]
                .as_i64()
                .unwrap(),
            0
        );
        assert_eq!(
            yaml_doc["dag_set_log"][1]["finish_time"][0]
                .as_i64()
                .unwrap(),
            53
        );
        assert_eq!(
            yaml_doc["dag_set_log"][1]["response_time"][0]
                .as_i64()
                .unwrap(),
            53
        );
        assert_eq!(
            yaml_doc["dag_set_log"][1]["average_response_time"]
                .as_f64()
                .unwrap(),
            53.0
        );
        assert_eq!(
            yaml_doc["dag_set_log"][1]["worst_response_time"]
                .as_i64()
                .unwrap(),
            53
        );

        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["core_id"][0]
                .as_i64()
                .unwrap(),
            1
        );
        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["dag_id"].as_i64().unwrap(),
            1
        );
        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["node_id"].as_i64().unwrap(),
            3
        );
        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["start_time"][0]
                .as_i64()
                .unwrap(),
            11
        );
        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["finish_time"][0]
                .as_i64()
                .unwrap(),
            22
        );

        assert_eq!(
            yaml_doc["processor_log"]["average_utilization"]
                .as_f64()
                .unwrap(),
            0.22133335
        );
        assert_eq!(
            yaml_doc["processor_log"]["variance_utilization"]
                .as_f64()
                .unwrap(),
            0.033460446
        );

        assert_eq!(
            yaml_doc["processor_log"]["core_logs"][0]["core_id"]
                .as_i64()
                .unwrap(),
            0
        );
        assert_eq!(
            yaml_doc["processor_log"]["core_logs"][0]["total_proc_time"]
                .as_i64()
                .unwrap(),
            156
        );
        assert_eq!(
            yaml_doc["processor_log"]["core_logs"][0]["utilization"]
                .as_f64()
                .unwrap(),
            0.52
        );

        remove_file(file_path).unwrap();
    }
}
