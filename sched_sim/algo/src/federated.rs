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

pub fn federated(folder_path: &str, core_num: usize) -> bool {
    let mut remaining_cores = core_num;
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut low_utilizations = 0.0;

    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let deadline = dag[last_node].params["end_to_end_deadline"];
        let sum_wect = cal_total_wect(dag.clone());
        let critical_paths = get_critical_paths(dag.clone());
        let critical_path_wect = get_critical_path_wect(critical_paths.clone(), dag.clone());

        if is_high_utilization_task(sum_wect, deadline) {
            let using_cores =
                ((sum_wect - critical_path_wect) / (deadline - critical_path_wect)).ceil();
            if using_cores as usize > remaining_cores {
                println!("Insufficient number of cores for the task set.");
                return false;
            } else {
                remaining_cores -= using_cores as usize;
            }
        } else {
            low_utilizations += sum_wect / deadline;
        }
    }

    if remaining_cores as f32 > 2.0 * low_utilizations {
        println!("can allocate task set to {} cores.", core_num);
        true
    } else {
        println!("cannot allocate task set to {} cores.", core_num);
        false
    }
}
