//! dynfed.
//! Paper Information
//! -----------------
//! Title: Timing-Anomaly Free Dynamic Scheduling of Periodic DAG Tasks with Non-Preemptive
//! Authors: Gaoyang Dai, Morteza Mohaqeqi, and Wang Yi
//! Conference: RTCSA 2021
//! -----------------

use lib::core::ProcessResult;
use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use lib::scheduler::SchedulerBase;
use lib::util::get_hyper_period;
use petgraph::{graph::NodeIndex, Graph};

fn calculate_minimum_cores_and_execution_order(
    dag: &mut Graph<NodeData, i32>,
    scheduler: &mut impl SchedulerBase<HomogeneousProcessor>,
) -> (usize, Vec<NodeIndex>) {
    let volume = dag.get_volume();
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap();
    let mut minimum_cores = (volume as f32 / end_to_end_deadline as f32).ceil() as usize;
    scheduler.set_processor(&HomogeneousProcessor::new(minimum_cores));
    let (mut schedule_length, mut execution_order) = scheduler.schedule();

    while schedule_length > end_to_end_deadline {
        minimum_cores += 1;
        scheduler.set_processor(&HomogeneousProcessor::new(minimum_cores));
        (schedule_length, execution_order) = scheduler.schedule();
    }

    (minimum_cores, execution_order)
}

fn get_dag_id(dag: &Graph<NodeData, i32>) -> usize {
    dag[NodeIndex::new(0)].params["dag_id"] as usize
}

#[allow(dead_code)]
pub fn dynfed(
    dag_set: &mut Vec<Graph<NodeData, i32>>,
    processor: &mut impl ProcessorBase,
    scheduler: &mut impl SchedulerBase<HomogeneousProcessor>,
) {
    let mut current_time = 0;
    let processor_cores = processor.get_number_of_cores() as i32;
    let hyper_period = get_hyper_period(dag_set);
    let mut dag_queue: Vec<Graph<NodeData, i32>> = Vec::new();
    let mut finished_dag_nodes = Vec::<Vec<NodeIndex>>::new();
    let mut number_nodes_cores: Vec<i32> = Vec::new();
    let mut number_dag_cores: Vec<i32> = Vec::new();
    let mut execution_order: Vec<Vec<NodeIndex>> = Vec::new();
    let mut required_cores: Vec<i32> = Vec::new();
    let mut is_dag_started: Vec<bool> = Vec::new();
    let mut core_dag_id: Vec<i32> = Vec::new();

    for (i, dag) in dag_set.iter_mut().enumerate() {
        is_dag_started[i] = false;
        //Managing index of dag with param because Hash cannot be used for key of Hash.
        dag.add_param(NodeIndex::new(0), "dag_id", i as i32);
        let (required_core, execution_orders) =
            calculate_minimum_cores_and_execution_order(dag, scheduler);
        required_cores[i] = required_core as i32;
        execution_order[i] = execution_orders;
    }

    //Since the evaluation of the original paper is one hyper period.
    while current_time < hyper_period {
        for dag in &mut *dag_set {
            let offset = dag.get_head_offset();
            if current_time == offset {
                dag_queue.push(dag.clone());
            }
        }

        //1つ目 終了したジョブに対する操作
        for dag in &mut *dag_set {
            let dag_id = get_dag_id(dag);
            number_nodes_cores[dag_id] -= finished_dag_nodes[dag_id].len() as i32;

            for nodes in &finished_dag_nodes[dag_id] {
                if dag.get_suc_nodes(*nodes).is_none() {
                    number_dag_cores[dag_id] -= 1;
                }
            }

            finished_dag_nodes[dag_id].clear();
        }

        //2つ目
        while !dag_queue.is_empty() {
            //dag_queueの先頭のジョブを削除せずに取り出す
            let dag = dag_queue.get(0).unwrap();
            let dag_id = get_dag_id(dag);
            if required_cores[dag_id] > processor_cores - number_dag_cores.iter().sum::<i32>() {
                break;
            } else {
                dag_queue.remove(0);
                is_dag_started[dag_id] = true;
                number_dag_cores[dag_id] = required_cores[dag_id];
                number_nodes_cores[dag_id] = 0;
            }
        }

        //3つ目
        for dag in &mut *dag_set {
            let dag_id = get_dag_id(dag);
            if is_dag_started[dag_id] {
                //TODO: dagを事前計算フェーズの順番に並び替える
                //おそらく事前計算フェーズの結果をそのまま持ってくるだけ

                //事前計算フェーズの結果を配列で仮定
                while !execution_order[dag_id].is_empty() {
                    let dag_execution_order = &mut execution_order[dag_id];
                    let node = dag_execution_order.get(0).unwrap();
                    let pre_nodes = dag.get_pre_nodes(*node).unwrap_or_default();
                    if pre_nodes.len() as i32 == dag[*node].params["pre_done_count"]
                        && number_nodes_cores[dag_id] < number_dag_cores[dag_id]
                    {
                        number_nodes_cores[dag_id] += 1;
                        if let Some(core_index) = processor.get_idle_core_index() {
                            processor.allocate(core_index, &dag[*node]);
                            core_dag_id[core_index] = dag_id as i32;
                            dag_execution_order.remove(0);
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        //実行する
        let process_result = processor.process();
        current_time += 1;

        let finish_nodes: Vec<(usize, NodeIndex)> = process_result
            .iter()
            .enumerate()
            .filter_map(|(core_index, result)| {
                if let ProcessResult::Done(id) = result {
                    Some((core_index, *id))
                } else {
                    None
                }
            })
            .collect();

        for (core_index, node_index) in finish_nodes {
            let dag_id = core_dag_id[core_index] as usize;
            let dag = &mut dag_set[dag_id];
            finished_dag_nodes[dag_id].push(node_index);
            let suc_nodes = dag.get_suc_nodes(node_index).unwrap_or_default();
            for suc_node in suc_nodes {
                if let Some(value) = dag[suc_node].params.get_mut("pre_done_count") {
                    *value += 1;
                } else {
                    dag[suc_node].params.insert("pre_done_count".to_owned(), 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        dag.add_param(c0, "end_to_end_deadline", 100);
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

    #[test]
    fn test_calculate_minimum_cores_and_execution_order_normal() {
        let mut dag = create_sample_dag();
        let mut scheduler = FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let (minimum_cores, execution_order) =
            calculate_minimum_cores_and_execution_order(&mut dag, &mut scheduler);

        assert_eq!(minimum_cores, 1);
        assert_eq!(
            execution_order,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(4),
                NodeIndex::new(3),
                NodeIndex::new(1),
                NodeIndex::new(2)
            ]
        );
    }
}
