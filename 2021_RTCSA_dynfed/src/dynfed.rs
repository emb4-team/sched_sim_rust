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

pub struct DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    scheduler: T,
    log: DAGSetSchedulerLog,
    managers: Vec<DAGStateManager>,
}

impl<T> DAGSetSchedulerBase<HomogeneousProcessor> for DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            scheduler: T::new(&Graph::<NodeData, i32>::new(), processor),
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
            managers: vec![DAGStateManager::new_expended(); dag_set.len()],
        }
    }

    fn initialize(&mut self) {
        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            let (minimum_cores, execution_order) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);
            self.managers[dag_id].set_minimum_cores(minimum_cores as i32);
            self.managers[dag_id].set_execution_order(Some(execution_order));
        }
    }

    fn schedule(&mut self) -> i32 {
        // Initialize DAGStateManagers
        //let mut dag_state_managers = vec![DAGStateManager::new(); self.dag_set.len()];

        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            let (minimum_cores, execution_order) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);
            self.managers[dag_id].set_minimum_cores(minimum_cores as i32);
            self.managers[dag_id].set_execution_order(Some(execution_order));
        }

        // Start scheduling
        let mut current_time = 0;
        let mut ready_dag_queue: VecDeque<Graph<NodeData, i32>> = VecDeque::new();
        let mut log = self.get_log();
        let hyper_period = get_hyper_period(&self.dag_set);
        while current_time < hyper_period {
            // Release DAGs
            for dag in self.dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap() * self.managers[dag_id].get_release_count()
                {
                    ready_dag_queue.push_back(dag.clone());
                    self.managers[dag_id].increment_release_count();
                    log.write_dag_release_time(dag_id, current_time);
                }
            }

            // Start DAG if there are enough free core
            while let Some(dag) = ready_dag_queue.front() {
                let dag_id = dag.get_dag_id();
                if self.managers[dag_id].can_start(
                    self.processor.get_number_of_cores() as i32,
                    get_total_allocated_cores(&self.managers),
                ) {
                    ready_dag_queue.pop_front();
                    self.managers[dag_id].start();
                    log.write_dag_start_time(dag_id, current_time);
                } else {
                    break;
                }
            }

            // Allocate the nodes of each DAG
            for dag in self.dag_set.iter() {
                let dag_id = dag.get_dag_id();
                if !self.managers[dag_id].get_is_started() {
                    continue;
                }

                while let Some(node_i) = self.managers[dag_id].get_execution_order_head() {
                    if dag.is_node_ready(*node_i) && self.managers[dag_id].get_unused_cores() > 0 {
                        let core_id = self.processor.get_idle_core_index().unwrap();
                        log.write_allocating_node(
                            dag_id,
                            node_i.index(),
                            core_id,
                            current_time,
                            *dag[*node_i].params.get("execution_time").unwrap(),
                        );
                        self.processor.allocate_specific_core(
                            core_id,
                            &dag[self.managers[dag_id].allocate_head()],
                        );
                    } else {
                        break;
                    }
                }
            }

            // Process unit time
            let process_result = self.processor.process();
            current_time += 1;

            // Post-process on completion of node execution
            for result in process_result {
                if let ProcessResult::Done(node_data) = result {
                    log.write_finishing_node(&node_data, current_time);
                    let dag_id = node_data.params["dag_id"] as usize;
                    self.managers[dag_id].decrement_num_using_cores();

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
                        self.managers[dag_id].reset_state();
                    } else {
                        for suc_node in suc_nodes {
                            dag.increment_pre_done_count(suc_node);
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

    fn get_log(&self) -> DAGSetSchedulerLog {
        self.log.clone()
    }

    fn set_log(&mut self, log: DAGSetSchedulerLog) {
        self.log = log;
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

        let file_path = dynfed.dump_log("../lib/tests", "test");
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
