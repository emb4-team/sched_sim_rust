use lib::{
    graph_extension::{GraphExtension, NodeData},
    util::get_hyper_period,
};
use petgraph::Graph;
use std::collections::HashMap;

struct LabeledGraph<NodeData, i32> {
    label: usize,
    dag: Graph<NodeData, i32>,
}

#[allow(dead_code, clippy::ptr_arg)] // TODO: remove
pub fn dynfed(dag_set: &mut Vec<Graph<NodeData, i32>>) {
    let mut current_time = 0;
    let mut labeled_dag_set: Vec<LabeledGraph<NodeData, i32>> = Vec::new();
    let hyper_period = get_hyper_period(dag_set);
    let mut dag_queue: Vec<LabeledGraph<NodeData, i32>> = Vec::new();
    let number_sub_job_cores: HashMap<usize, i32> = HashMap::new();
    let number_dag_cores: HashMap<usize, i32> = HashMap::new();
    let mut is_dag_started: HashMap<usize, bool> = HashMap::new();

    for i in 0..dag_set.len() {
        is_dag_started.insert(i, false);
        let mut labeled_graph = LabeledGraph {
            label: i,
            dag: dag_set[i].clone(),
        };
        labeled_dag_set.push(labeled_graph);
    }

    while current_time < hyper_period {
        /*for labeled_dag in labeled_dag_set.iter() {
            if labeled_dag.dag.get_head_period() == Some(current_time) {
                dag_queue.push(*labeled_dag);
            }
        }*/
        //1つ目
        /*while !dag_queue.is_empty() {
            let labeled_dag = dag_queue.first().unwrap();
            let dag_id = labeled_dag_set
                .iter()
                .position(|target_labeled_dag| target_labeled_dag.label == labeled_dag.label);
            is_dag_started[&dag_id.unwrap()] = true;
        }*/
        //3つ目
        current_time += 1;
    }
}

#[cfg(test)]
mod tests {}
