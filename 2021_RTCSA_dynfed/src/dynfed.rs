//! dynfed.
//! Paper Information
//! -----------------
//! Title: Timing-Anomaly Free Dynamic Scheduling of Periodic DAG Tasks with Non-Preemptive
//! Authors: Gaoyang Dai, Morteza Mohaqeqi, and Wang Yi
//! Conference: RTCSA 2021
//! -----------------
use lib::core::ProcessResult;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::processor::ProcessorBase;
use lib::scheduler::SchedulerBase;
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
    scheduler: &mut impl SchedulerBase<T>,
) -> (usize, Vec<NodeIndex>)
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

fn get_dag_id(dag: &Graph<NodeData, i32>) -> usize {
    dag[NodeIndex::new(0)].params["dag_id"] as usize
}

#[allow(dead_code)]
pub fn dynamic_federated<T>(
    dag_set: &mut [Graph<NodeData, i32>],
    processor: &mut T,
    scheduler: &mut impl SchedulerBase<T>,
) -> i32
where
    T: ProcessorBase + Clone,
{
    let mut current_time = 0;
    let processor_cores = processor.get_number_of_cores() as i32;

    let mut dag_queue: Vec<Graph<NodeData, i32>> = Vec::new();
    let mut finished_dags_count = 0;
    let num_of_dags = dag_set.len();

    //Variables per dag respectively
    //DISCUSS: 可読性を上げるために後ろに_by_dagをつける？
    //DISCUSS: もしくは、structにまとめる？
    let mut finished_nodes = vec![Vec::new(); num_of_dags];
    let mut using_cores: Vec<i32> = vec![0; num_of_dags];
    let mut allocated_cores: Vec<i32> = vec![0; num_of_dags];
    let mut execution_orders: Vec<Vec<NodeIndex>> = vec![Vec::new(); num_of_dags];
    let mut min_cores_for_sched: Vec<i32> = vec![0; num_of_dags];
    let mut is_started: Vec<bool> = vec![false; num_of_dags];

    //Variables per core respectively
    //DISCUSS: 可読性を上げるために後ろに_by_coreをつける？
    let mut assigned_dag_id: Vec<i32> = vec![-1; processor_cores.try_into().unwrap()];

    scheduler.set_processor(processor);

    for (dag_id, dag) in dag_set.iter_mut().enumerate() {
        //Managing index of dag with param because Hash cannot be used for key of Hash.
        dag.add_param(NodeIndex::new(0), "dag_id", dag_id as i32);
        scheduler.set_dag(dag);
        let (required_core, execution_order) =
            calculate_minimum_cores_and_execution_order(dag, scheduler);
        min_cores_for_sched[dag_id] = required_core as i32;
        execution_orders[dag_id] = execution_order;
    }

    while finished_dags_count < num_of_dags {
        //Queue the dag when it is released.
        for dag in &mut *dag_set {
            if current_time == dag.get_head_offset() {
                dag_queue.push(dag.clone());
            }
        }

        //Operations on finished jobs
        for dag in &mut *dag_set {
            let dag_id = get_dag_id(dag);
            using_cores[dag_id] -= finished_nodes[dag_id].len() as i32;
            finished_nodes[dag_id].clear();
        }

        //Start DAG if there are enough free cores
        while let Some(dag) = dag_queue.first() {
            let dag_id = get_dag_id(dag);
            if min_cores_for_sched[dag_id] > processor_cores - allocated_cores.iter().sum::<i32>() {
                break;
            }
            dag_queue.remove(0);
            is_started[dag_id] = true;
            allocated_cores[dag_id] = min_cores_for_sched[dag_id];
        }

        //Assign a node per DAG
        for dag in &mut *dag_set {
            let dag_id = get_dag_id(dag);
            if !is_started[dag_id] {
                continue;
            }
            let dag_execution_order = &mut execution_orders[dag_id];

            while let Some(node) = dag_execution_order.first() {
                let pre_nodes_count = dag.get_pre_nodes(*node).unwrap_or_default().len() as i32;
                let pre_done_nodes_count = *dag[*node].params.get("pre_done_count").unwrap_or(&0);
                let unused_cores = allocated_cores[dag_id] - using_cores[dag_id];

                //DISCUSS: 可読性を上げるためにifを分割するべきか
                //DISCUSS: pre_nodes_count == pre_done_nodes_countは実行可能になった場合
                //DISCUSS: unused_cores > 0はDAGに割り当てられるコアで使用されていないコアがある場合
                if pre_nodes_count == pre_done_nodes_count && unused_cores > 0 {
                    let core_i = processor.get_idle_core_index().unwrap();
                    processor.allocate(core_i, &dag[*node]);
                    using_cores[dag_id] += 1;
                    assigned_dag_id[core_i] = dag_id as i32;
                    dag_execution_order.remove(0);
                } else {
                    break;
                }
            }
        }

        let process_result = processor.process();
        current_time += 1;

        let finish_nodes: Vec<(usize, NodeIndex)> = process_result
            .iter()
            .enumerate()
            .filter_map(|(core_i, result)| {
                if let ProcessResult::Done(id) = result {
                    Some((core_i, *id))
                } else {
                    None
                }
            })
            .collect();

        for (core_i, node_i) in finish_nodes {
            let dag_id = assigned_dag_id[core_i] as usize;
            let dag = &mut dag_set[dag_id];

            finished_nodes[dag_id].push(node_i);

            let suc_nodes = dag.get_suc_nodes(node_i).unwrap_or_default();

            if suc_nodes.is_empty() {
                finished_dags_count += 1; //Source node is terminated, and its DAG is terminated
                allocated_cores[dag_id] = 0; //When the last node is finished, the core allocated to dag is released.
            }

            for suc_node in suc_nodes {
                if let Some(value) = dag[suc_node].params.get_mut("pre_done_count") {
                    *value += 1;
                } else {
                    dag[suc_node].params.insert("pre_done_count".to_owned(), 1);
                }
            }
        }
    }
    current_time
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::homogeneous::HomogeneousProcessor;
    use lib::{fixed_priority_scheduler::FixedPriorityScheduler, processor::ProcessorBase};
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
        dag.add_param(c0, "end_to_end_deadline", 50);
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
        dag.add_param(c0, "end_to_end_deadline", 53);
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
        let mut scheduler = FixedPriorityScheduler::new(
            &Graph::<NodeData, i32>::new(),
            &HomogeneousProcessor::new(1),
        );
        let time = dynamic_federated(
            &mut dag_set,
            &mut HomogeneousProcessor::new(5),
            &mut scheduler,
        );

        assert_eq!(time, 53);
    }
}
