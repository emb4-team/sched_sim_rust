use lib::dag_creator::{create_dag_set_from_dir, NodeData};
use lib::dag_handler::get_critical_paths;
use petgraph::graph::{Graph, NodeIndex};

fn cal_total_wect(dag: Graph<NodeData, f32>) -> f32 {
    let mut sum_wect = 0.0;
    for node in dag.node_indices() {
        sum_wect += dag[node].params["execution_time"];
    }
    sum_wect
}

fn get_critical_path_wect(critical_paths: Vec<Vec<NodeIndex>>, dag: Graph<NodeData, f32>) -> f32 {
    critical_paths[0]
        .iter()
        .map(|node| dag[*node].params["execution_time"])
        .sum()
}

fn is_high_utilization_task(sum_wect: f32, deadline: f32) -> bool {
    let utilization_rate = sum_wect / deadline;
    utilization_rate > 1.0
}

pub fn federated(folder_path: &str) {
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut deadlines: Vec<f32> = Vec::new();
    let mut sum_wects: Vec<f32> = Vec::new();
    let mut critical_path_wects: Vec<f32> = Vec::new();
    let mut utilization_rates: Vec<f32> = Vec::new();

    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let deadline = dag[last_node].params["end_to_end_deadline"];
        let sum_wect = cal_total_wect(dag.clone());
        let critical_paths = get_critical_paths(dag.clone());
        let critical_path_wect = get_critical_path_wect(critical_paths.clone(), dag.clone());

        if is_high_utilization_task(sum_wect, deadline) {
            println!("high utilization task: {:?}", dag);
        } else {
            println!("low utilization task: {:?}", dag);
        }

        deadlines.push(deadline);
        sum_wects.push(sum_wect);
        critical_path_wects.push(critical_path_wect);
        utilization_rates.push(cal_total_wect(dag.clone()) / deadline);
    }
    println!("deadlines: {:?}", deadlines);
    println!("sum_wects: {:?}", sum_wects);
    println!("critical_path_wects: {:?}", critical_path_wects);
    println!("utilization_rates: {:?}", utilization_rates);
}
