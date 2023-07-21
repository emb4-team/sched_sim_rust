//! dynfed.
//! Paper Information
//! -----------------
//! Title: Timing-Anomaly Free Dynamic Scheduling of Periodic DAG Tasks with Non-Preemptive
//! Authors: Gaoyang Dai, Morteza Mohaqeqi, and Wang Yi
//! Conference: RTCSA 2021
//! -----------------
use std::collections::HashSet;
use std::collections::VecDeque;

use lib::core::ProcessResult;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::log::*;
use lib::processor::ProcessorBase;
use lib::scheduler::*;
use lib::util::get_hyper_period;
use petgraph::{graph::NodeIndex, Graph};

#[derive(Clone, Debug)]
pub struct DAGStateManager {
    is_started: bool,
    num_using_cores: i32,
    num_allocated_cores: i32,
    minimum_cores: i32,
    execution_order: VecDeque<NodeIndex>,
}

impl DAGStateManager {
    fn new() -> Self {
        Self {
            is_started: false,
            num_using_cores: 0,
            num_allocated_cores: 0,
            minimum_cores: 0,
            execution_order: VecDeque::new(),
        }
    }

    fn get_is_started(&self) -> bool {
        self.is_started
    }

    fn start(&mut self) {
        self.is_started = true;
        self.num_allocated_cores = self.minimum_cores;
    }

    fn can_start(&self, total_processor_cores: i32, total_allocated_cores: i32) -> bool {
        self.minimum_cores <= total_processor_cores - total_allocated_cores
    }

    fn decrement_num_using_cores(&mut self) {
        self.num_using_cores -= 1;
    }

    fn get_unused_cores(&self) -> i32 {
        self.num_allocated_cores - self.num_using_cores
    }

    fn free_allocated_cores(&mut self) {
        self.num_allocated_cores = 0;
    }

    fn set_minimum_cores(&mut self, minimum_cores: i32) {
        self.minimum_cores = minimum_cores;
    }

    fn get_execution_order_head(&self) -> Option<&NodeIndex> {
        self.execution_order.front()
    }

    fn set_execution_order(&mut self, execution_order: VecDeque<NodeIndex>) {
        self.execution_order = execution_order;
    }

    fn allocate_head(&mut self) -> NodeIndex {
        if self.execution_order.is_empty() {
            panic!("Execution order is empty!");
        }
        self.num_using_cores += 1;
        self.execution_order.pop_front().unwrap()
    }
}

fn get_total_allocated_cores(dyn_feds: &[DAGStateManager]) -> i32 {
    let mut total_allocated_cores = 0;
    for dyn_fed in dyn_feds {
        total_allocated_cores += dyn_fed.num_allocated_cores;
    }
    total_allocated_cores
}

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
        }
    }

    fn schedule(&mut self) -> i32 {
        let mut current_time = 0;
        let dag_set_length = self.dag_set.len();
        let mut dag_state_managers = vec![DAGStateManager::new(); dag_set_length];
        let mut ready_dag_queue: VecDeque<Graph<NodeData, i32>> = VecDeque::new();
        let mut log = self.get_log();

        let mut execution_order: Vec<VecDeque<NodeIndex>> = vec![VecDeque::new(); dag_set_length];
        let mut minimum_cores: Vec<usize> = vec![0; dag_set_length];

        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            (minimum_cores[dag_id], execution_order[dag_id]) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);
            dag_state_managers[dag_id].set_minimum_cores(minimum_cores[dag_id] as i32);
            dag_state_managers[dag_id].set_execution_order(execution_order[dag_id].clone());
        }

        let mut head_offsets: Vec<i32> = self
            .dag_set
            .iter()
            .map(|dag| dag.get_head_offset())
            .collect::<HashSet<i32>>()
            .into_iter()
            .collect::<Vec<i32>>();

        head_offsets.sort();
        let mut head_offsets: VecDeque<i32> = head_offsets.into_iter().collect();
        let hyper_period = get_hyper_period(&self.dag_set);

        let mut release_count = vec![0; dag_set_length];

        while current_time < hyper_period {
            if !head_offsets.is_empty() && *head_offsets.front().unwrap() == current_time {
                self.dag_set
                    .iter_mut()
                    .filter(|dag| current_time == dag.get_head_offset())
                    .for_each(|dag| {
                        ready_dag_queue.push_back(dag.clone());
                        release_count[dag.get_dag_id()] += 1;
                        log.write_dag_release_time(dag.get_dag_id(), current_time);
                    });
                head_offsets.pop_front();
            }

            //release dag
            for dag in self.dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap() * release_count[dag_id] as i32
                {
                    ready_dag_queue.push_back(dag.clone());
                    release_count[dag.get_dag_id()] += 1;
                    log.write_dag_release_time(dag.get_dag_id(), current_time);
                }
            }

            //Start DAG if there are enough free core
            while let Some(dag) = ready_dag_queue.front() {
                let dag_id = dag.get_dag_id();
                let num_processor_cores = self.processor.get_number_of_cores() as i32;
                let total_allocated_cores = get_total_allocated_cores(&dag_state_managers);
                if dag_state_managers[dag_id].can_start(num_processor_cores, total_allocated_cores)
                {
                    ready_dag_queue.pop_front();
                    dag_state_managers[dag_id].start();
                    log.write_dag_start_time(dag_id, current_time);
                } else {
                    break;
                }
            }

            //Allocate the nodes of each DAG
            for dag in self.dag_set.iter() {
                let dag_id = dag.get_dag_id();
                if !dag_state_managers[dag_id].get_is_started() {
                    continue;
                }

                while let Some(node_i) = dag_state_managers[dag_id].get_execution_order_head() {
                    let unused_cores = dag_state_managers[dag_id].get_unused_cores();
                    if dag.is_node_ready(*node_i) && unused_cores > 0 {
                        let node_id = dag[*node_i].id as usize;
                        let core_id = self.processor.get_idle_core_index().unwrap();
                        let proc_time = dag[*node_i].params.get("execution_time").unwrap_or(&0);
                        log.write_allocating_node(
                            dag_id,
                            node_id,
                            core_id,
                            current_time,
                            *proc_time,
                        );
                        self.processor.allocate_specific_core(
                            core_id,
                            &dag[dag_state_managers[dag_id].allocate_head()],
                        );
                    } else {
                        break;
                    }
                }
            }

            let process_result = self.processor.process();
            current_time += 1;

            let finish_nodes: Vec<NodeData> = process_result
                .iter()
                .filter_map(|result| {
                    if let ProcessResult::Done(node_data) = result {
                        log.write_finishing_node(node_data, current_time);
                        Some(node_data.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for finish_node_data in finish_nodes {
                let dag_id = finish_node_data.params["dag_id"] as usize;
                dag_state_managers[dag_id].decrement_num_using_cores();

                let dag = &mut self.dag_set[dag_id];

                let suc_nodes = dag
                    .get_suc_nodes(NodeIndex::new(finish_node_data.id as usize))
                    .unwrap_or_default();

                if suc_nodes.is_empty() {
                    log.write_dag_finish_time(dag_id, current_time);
                    for node_i in dag.node_indices() {
                        dag.update_param(node_i, "pre_done_count", 0);
                    }
                    dag_state_managers[dag_id].is_started = false;
                    dag_state_managers[dag_id].set_execution_order(execution_order[dag_id].clone());
                    dag_state_managers[dag_id].free_allocated_cores(); //When the last node is finished, the core allocated to dag is released.
                }

                for suc_node in suc_nodes {
                    dag.increment_pre_done_count(suc_node);
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
    use super::*;
    use lib::fixed_priority_scheduler::FixedPriorityScheduler;
    use lib::homogeneous::HomogeneousProcessor;
    use lib::processor::ProcessorBase;
    use lib::util::load_yaml;
    use std::collections::HashMap;
    use std::fs::remove_file;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
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

        /*  assert_eq!(
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
            yaml_doc["node_set_logs"][1][3]["core_id"].as_i64().unwrap(),
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
            yaml_doc["node_set_logs"][1][3]["start_time"]
                .as_i64()
                .unwrap(),
            11
        );
        assert_eq!(
            yaml_doc["node_set_logs"][1][3]["finish_time"]
                .as_i64()
                .unwrap(),
            22
        );

        assert_eq!(
            yaml_doc["processor_log"]["average_utilization"]
                .as_f64()
                .unwrap(),
            0.08933333
        );
        assert_eq!(
            yaml_doc["processor_log"]["variance_utilization"]
                .as_f64()
                .unwrap(),
            0.0017751111
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
            40
        );
        assert_eq!(
            yaml_doc["processor_log"]["core_logs"][0]["utilization"]
                .as_f64()
                .unwrap(),
            0.13333334
        );*/

        println!("yaml_doc: {:#?}", yaml_doc);

        remove_file(file_path).unwrap();
    }
}
