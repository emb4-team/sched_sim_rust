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

fn is_high_utilization_task(execution_time: f32, period: f32, deadline: f32) -> bool {
    // 関数の本体
    true
}

pub fn federated(folder_path: &str) {
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut deadlines: Vec<f32> = Vec::new();
    let mut sum_wects: Vec<f32> = Vec::new();
    let mut critical_path_wects: Vec<f32> = Vec::new();
    let mut utilization_rates: Vec<f32> = Vec::new();

    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let mut sum_wect = 0.0;
        let deadline = dag[last_node].params["end_to_end_deadline"];
        deadlines.push(deadline);

        sum_wects.push(cal_total_wect(dag.clone()));

        let mut critical_paths = get_critical_paths(dag.clone());
        critical_paths[0].pop();
        critical_paths[0].remove(0);

        critical_path_wects.push(get_critical_path_wect(critical_paths, dag.clone()));

        utilization_rates.push(cal_total_wect(dag.clone()) / deadline);
    }
    println!("deadlines: {:?}", deadlines);
    println!("sum_wects: {:?}", sum_wects);
    println!("critical_path_wects: {:?}", critical_path_wects);
    println!("utilization_rates: {:?}", utilization_rates);
}
