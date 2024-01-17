//! dynfed.
//! Paper Information
//! -----------------
//! Title: Timing-Anomaly Free Dynamic Scheduling of Periodic DAG Tasks with Non-Preemptive
//! Authors: Gaoyang Dai, Morteza Mohaqeqi, and Wang Yi
//! Conference: RTCSA 2021
//! -----------------
use getset::{CopyGetters, Setters};
use lib::{
    core::ProcessResult,
    dag_scheduler::DAGSchedulerBase,
    dag_set_scheduler::{DAGSetSchedulerBase, DAGState, DAGStateManagerBase, PreemptiveType},
    getset_dag_set_scheduler, getset_dag_state_manager,
    graph_extension::{GraphExtension, NodeData},
    homogeneous::HomogeneousProcessor,
    log::DAGSetSchedulerLog,
    processor::ProcessorBase,
    util::{get_hyper_period, get_process_core_indices},
};
use petgraph::{graph::NodeIndex, Graph};
use std::collections::VecDeque;

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
    dag: &Graph<NodeData, i32>,
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

#[derive(Clone, Default, CopyGetters, Setters)]
pub struct DynFedDAGStateManager {
    #[getset(get_copy = "pub with_prefix", set = "pub")]
    minimum_cores: i32,
    num_using_cores: i32,
    num_allocated_cores: i32,
    execution_order: VecDeque<NodeIndex>,
    initial_execution_order: VecDeque<NodeIndex>,
    release_count: i32,
    dag_state: DAGState,
}

impl DAGStateManagerBase for DynFedDAGStateManager {
    getset_dag_state_manager!();

    fn complete_execution(&mut self) {
        self.execution_order = self.initial_execution_order.clone();
        self.num_allocated_cores = 0; // When the last node is finished, the core allocated to dag is freed.
        self.set_dag_state(DAGState::Waiting);
    }
}

impl DynFedDAGStateManager {
    fn start(&mut self) {
        self.num_allocated_cores = self.minimum_cores;
        self.set_dag_state(DAGState::Running);
    }

    fn can_start(&self, idle_core_num: i32) -> bool {
        (self.dag_state == DAGState::Ready) && (self.minimum_cores <= idle_core_num)
    }

    fn set_execution_order(&mut self, execution_order: VecDeque<NodeIndex>) {
        self.initial_execution_order = execution_order.clone();
        self.execution_order = execution_order;
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

fn get_total_allocated_cores(expansion_managers: &[DynFedDAGStateManager]) -> i32 {
    let mut total_allocated_cores = 0;
    for expansion_manager in expansion_managers {
        total_allocated_cores += expansion_manager.num_allocated_cores;
    }
    total_allocated_cores
}

pub struct DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    scheduler: T,
    log: DAGSetSchedulerLog,
    current_time: i32,
}

impl<T> DAGSetSchedulerBase<HomogeneousProcessor> for DynamicFederatedScheduler<T>
where
    T: DAGSchedulerBase<HomogeneousProcessor>,
{
    getset_dag_set_scheduler!(HomogeneousProcessor);

    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            scheduler: T::new(&Graph::<NodeData, i32>::new(), processor),
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
            current_time: 0,
        }
    }

    fn schedule(&mut self, _: PreemptiveType) -> i32 {
        // Initialize DAGStateManagers
        let mut managers = vec![DynFedDAGStateManager::default(); self.dag_set.len()];
        for dag in self.dag_set.iter() {
            let dag_id = dag.get_dag_param("dag_id") as usize;
            let (minimum_cores, execution_order) =
                calculate_minimum_cores_and_execution_order(dag, &mut self.scheduler);
            managers[dag_id].set_minimum_cores(minimum_cores as i32);
            managers[dag_id].set_execution_order(execution_order);
        }

        // Start scheduling
        let hyper_period = get_hyper_period(&self.dag_set);
        while self.get_current_time() < hyper_period {
            // Release DAGs
            self.release_dags(&mut managers);
            // Start DAGs if there are free cores
            let mut idle_core_num =
                self.processor.get_number_of_cores() as i32 - get_total_allocated_cores(&managers);
            for manager in managers.iter_mut() {
                if manager.can_start(idle_core_num) {
                    manager.start();
                    idle_core_num -= manager.get_minimum_cores();
                }
            }

            // Allocate the nodes of ready_queue to idle cores
            for dag in self.get_dag_set().iter() {
                let dag_id = dag.get_dag_param("dag_id") as usize;
                if managers[dag_id].get_dag_state() != DAGState::Running {
                    continue;
                }

                while let Some(node_i) = managers[dag_id].get_execution_order_head() {
                    if dag.is_node_ready(*node_i) && managers[dag_id].get_unused_cores() > 0 {
                        let core_id = self.processor.get_idle_core_index().unwrap();
                        let node = &dag[managers[dag_id].allocate_head()];
                        self.allocate_node(
                            node,
                            core_id,
                            managers[dag_id].get_release_count() as usize,
                        );
                    } else {
                        break;
                    }
                }
            }
            // Process unit time
            let process_result = self.process_unit_time();
            let indices: Vec<usize> = get_process_core_indices(&process_result);
            self.log.write_processing_time(&indices);

            // Post-process on completion of node execution
            for (core_id, result) in process_result.iter().enumerate() {
                if let ProcessResult::Done(node_data) = result {
                    managers[node_data.get_params_value("dag_id") as usize]
                        .decrement_num_using_cores();
                    self.post_process_on_node_completion(node_data, core_id, &mut managers);
                }
            }
        }

        self.calculate_log();
        self.get_current_time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::fixed_priority_scheduler::FixedPriorityScheduler;
    use lib::homogeneous::HomogeneousProcessor;
    use lib::processor::ProcessorBase;
    use lib::tests_helper::{common_sched_dump_test, create_dag_for_dynfed};
    use lib::{assert_yaml_value, assert_yaml_values_for_prefix};
    use std::path::PathBuf;

    #[test]
    fn test_dynfed_normal() {
        common_sched_dump_test(
            || {
                let mut dag_set = vec![
                    create_dag_for_dynfed(0, 150, 50, true),
                    create_dag_for_dynfed(1, 100, 60, false),
                ];

                for (dag_id, dag) in dag_set.iter_mut().enumerate() {
                    dag.set_dag_param("dag_id", dag_id as i32);
                }

                let mut dynfed: DynamicFederatedScheduler<
                    FixedPriorityScheduler<HomogeneousProcessor>,
                > = DynamicFederatedScheduler::new(&dag_set, &HomogeneousProcessor::new(5));
                dynfed.schedule(PreemptiveType::NonPreemptive);

                PathBuf::from(dynfed.dump_log("../lib/tests", "dyn_test"))
            },
            |yaml_doc| {
                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "dag_set_info",
                    [
                        ("total_utilization", f64, 3.705357),
                        ("each_dag_info[0].critical_path_length", i64, 50),
                        ("each_dag_info[0].period", i64, 150),
                        ("each_dag_info[0].end_to_end_deadline", i64, 50),
                        ("each_dag_info[0].volume", i64, 70),
                        ("each_dag_info[0].utilization", f64, 2.142857),
                        ("each_dag_info[1].utilization", f64, 1.5625)
                    ]
                );

                assert_yaml_value!(yaml_doc, "processor_info.number_of_cores", i64, 5);

                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "dag_set_log[1]",
                    [
                        ("dag_id", i64, 1),
                        ("release_time[0]", i64, 0),
                        ("finish_time[0]", i64, 53),
                        ("response_time[0]", i64, 53),
                        ("average_response_time", f64, 53.0),
                        ("worst_response_time", i64, 53)
                    ]
                );

                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "node_set_logs[1][2]",
                    [
                        ("core_id", i64, 1),
                        ("dag_id", i64, 1),
                        ("node_id", i64, 3),
                        ("job_id", i64, 0),
                        ("event_time", str, "11")
                    ]
                );

                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "processor_log",
                    [
                        ("average_utilization", f64, 0.22133335),
                        ("variance_utilization", f64, 0.033460446)
                    ]
                );

                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "processor_log.core_logs[0]",
                    [
                        ("core_id", i64, 0),
                        ("total_proc_time", i64, 156),
                        ("utilization", f64, 0.52)
                    ]
                );
            },
        );
    }
}
