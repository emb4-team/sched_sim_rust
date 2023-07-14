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
use petgraph::{graph::NodeIndex, Graph};

#[derive(Clone, Debug)]
pub struct DAGStateManager {
    pub is_started: bool,
    pub num_using_cores: i32,
    pub num_allocated_cores: i32,
    pub minimum_cores: i32,
    pub execution_order: VecDeque<NodeIndex>,
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

    fn set_minimum_cores(&mut self, minimum_cores: i32) {
        self.minimum_cores = minimum_cores;
    }

    fn set_execution_order(&mut self, execution_order: VecDeque<NodeIndex>) {
        self.execution_order = execution_order;
    }

    fn start(&mut self) {
        self.is_started = true;
        self.num_allocated_cores = self.minimum_cores;
    }

    fn can_start(&self, total_processor_cores: i32, total_allocated_cores: i32) -> bool {
        self.minimum_cores <= total_processor_cores - total_allocated_cores
    }

    fn allocate_head(&mut self) -> NodeIndex {
        if self.execution_order.is_empty() {
            panic!("Execution order is empty!");
        }
        self.num_using_cores += 1;
        self.execution_order.pop_front().unwrap()
    }

    fn get_unused_cores(&self) -> i32 {
        self.num_allocated_cores - self.num_using_cores
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
    fn new(dag_set: &mut [Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            scheduler: T::new(&mut Graph::<NodeData, i32>::new(), processor),
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
        }
    }

    fn schedule(&mut self) -> i32 {
        let mut current_time = 0;
        let dag_set_length = self.dag_set.len();
        let mut dag_state_managers = vec![DAGStateManager::new(); dag_set_length];
        let mut ready_dag_queue: VecDeque<Graph<NodeData, i32>> = VecDeque::new();
        let mut finished_dags_count = 0;
        let mut log = self.get_log();

        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
            let (minimum_cores, execution_order) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);
            dag_state_managers[dag_id].set_minimum_cores(minimum_cores as i32);
            dag_state_managers[dag_id].set_execution_order(execution_order);
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

        while finished_dags_count < dag_set_length {
            if !head_offsets.is_empty() && *head_offsets.front().unwrap() == current_time {
                self.dag_set
                    .iter_mut()
                    .filter(|dag| current_time == dag.get_head_offset())
                    .for_each(|dag| {
                        ready_dag_queue.push_back(dag.clone());
                        log.write_dag_release_time_log(dag.get_dag_id(), current_time);
                    });
                head_offsets.pop_front();
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
                    log.write_dag_start_time_log(dag_id, current_time);
                } else {
                    break;
                }
            }

            //Allocate the nodes of each DAG
            for dag in self.dag_set.iter() {
                let dag_id = dag.get_dag_id();
                if !dag_state_managers[dag_id].is_started {
                    continue;
                }

                while let Some(node_i) = dag_state_managers[dag_id].execution_order.front() {
                    let unused_cores = dag_state_managers[dag_id].get_unused_cores();
                    if dag.is_node_ready(*node_i) && unused_cores > 0 {
                        let node_id = dag[*node_i].id as usize;
                        let core_id = self.processor.get_idle_core_index().unwrap();
                        let proc_time = dag[*node_i].params.get("execution_time").unwrap_or(&0);
                        log.write_allocating_log(
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
                        log.write_finishing_node_log(node_data, current_time);
                        Some(node_data.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for finish_node_data in finish_nodes {
                let dag_id = finish_node_data.params["dag_id"] as usize;
                dag_state_managers[dag_id].num_using_cores -= 1;

                let dag = &mut self.dag_set[dag_id];

                let suc_nodes = dag
                    .get_suc_nodes(NodeIndex::new(finish_node_data.id as usize))
                    .unwrap_or_default();

                if suc_nodes.is_empty() {
                    finished_dags_count += 1; //Source node is terminated, and its DAG is terminated
                    log.write_dag_finish_time_log(dag_id, current_time);
                    dag_state_managers[dag_id].num_allocated_cores = 0; //When the last node is finished, the core allocated to dag is released.
                }

                for suc_node in suc_nodes {
                    dag.increment_pre_done_count(suc_node);
                }
            }
        }

        log.write_scheduling_log(current_time);

        self.set_log(log);
        current_time
    }

    fn get_name(&self) -> String {
        "DynFed".to_string()
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
    use std::collections::HashMap;

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
        dag.add_param(c0, "period", 100);
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
        dag.add_param(c2, "end_to_end_deadline", 53);
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
        let mut dag_set = vec![dag, dag2];
        let mut dynfed: DynamicFederatedScheduler<FixedPriorityScheduler<HomogeneousProcessor>> =
            DynamicFederatedScheduler::new(&mut dag_set, &HomogeneousProcessor::new(4));
        let time = dynfed.schedule();

        assert_eq!(time, 103);

        assert_eq!(dynfed.log.dag_set_log.get_dag_set_log()[1].get_dag_id(), 1);
        assert_eq!(
            dynfed.log.dag_set_log.get_dag_set_log()[1].get_release_time(),
            0
        );
        assert_eq!(
            dynfed.log.dag_set_log.get_dag_set_log()[1].get_start_time(),
            50
        );
        assert_eq!(
            dynfed.log.dag_set_log.get_dag_set_log()[1].get_finish_time(),
            103
        );

        assert_eq!(
            dynfed.log.node_set_logs.node_set_logs[1][3].get_core_id(),
            0
        );
        assert_eq!(dynfed.log.node_set_logs.node_set_logs[1][3].get_dag_id(), 1);
        assert_eq!(
            dynfed.log.node_set_logs.node_set_logs[1][3].get_node_id(),
            3
        );
        assert_eq!(
            dynfed.log.node_set_logs.node_set_logs[1][3].get_start_time(),
            61
        );
        assert_eq!(
            dynfed.log.node_set_logs.node_set_logs[1][3].get_finish_time(),
            72
        );

        assert_eq!(dynfed.log.processor_log.average_utilization, 0.32524273);
        assert_eq!(dynfed.log.processor_log.variance_utilization, 0.08862758);

        assert_eq!(dynfed.log.processor_log.core_logs[0].core_id, 0);
        assert_eq!(dynfed.log.processor_log.core_logs[0].total_proc_time, 83);
        assert_eq!(
            dynfed.log.processor_log.core_logs[0].utilization,
            0.80582523
        );

        assert_eq!(
            dynfed.log.dag_set_info.get_each_dag_info()[0].get_critical_path_length(),
            50
        );
        assert_eq!(
            dynfed.log.dag_set_info.get_each_dag_info()[0].get_period(),
            100
        );
        assert_eq!(
            dynfed.log.dag_set_info.get_each_dag_info()[0].get_volume(),
            70
        );

        assert_eq!(dynfed.log.processor_info.get_number_of_cores(), 4);
    }
}
