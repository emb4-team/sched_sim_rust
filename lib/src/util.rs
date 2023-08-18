use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
};
use chrono::{DateTime, Utc};
use log::{info, warn};
use num_integer::lcm;
use petgraph::graph::Graph;
use std::{
    fs::{self, OpenOptions},
    io::Write,
};
use yaml_rust::YamlLoader;

pub fn get_hyper_period(dag_set: &[Graph<NodeData, i32>]) -> i32 {
    let mut hyper_period = 1;
    for dag in dag_set {
        let dag_period = dag.get_head_period().unwrap();
        hyper_period = lcm(hyper_period, dag_period);
    }
    hyper_period
}

pub fn adjust_to_implicit_deadline(dag_set: &mut [Graph<NodeData, i32>]) {
    for dag in dag_set.iter_mut() {
        let period = dag.get_head_period();
        let end_to_end_deadline = dag.get_end_to_end_deadline();
        match (period, end_to_end_deadline) {
            (Some(period_value), Some(_)) => {
                if end_to_end_deadline != period {
                    warn!("In this algorithm, the period and the end-to-end deadline must be equal. Therefore, the end-to-end deadline is overridden by the period.");
                    dag.get_sink_nodes().iter().for_each(|&sink_i| {
                        if dag[sink_i].params.get("end_to_end_deadline").is_some() {
                            dag.update_param(sink_i, "end_to_end_deadline", period_value);
                        }
                    });
                }
            }
            (None, Some(deadline_value)) => {
                dag.add_param(dag.get_source_nodes()[0], "period", deadline_value);
            }
            (Some(period_value), None) => {
                dag.add_param(dag.get_sink_nodes()[0], "end_to_end_deadline", period_value);
            }
            (None, None) => {
                panic!("Either an period or end-to-end deadline is required for the schedule.");
            }
        }
    }
}

pub fn load_yaml(file_path: &str) -> Vec<yaml_rust::Yaml> {
    if !file_path.ends_with(".yaml") && !file_path.ends_with(".yml") {
        panic!("Invalid file type: {}", file_path);
    }
    let file_content = fs::read_to_string(file_path).unwrap();
    YamlLoader::load_from_str(&file_content).unwrap()
}

pub fn append_info_to_yaml(file_path: &str, info: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)
    {
        if let Err(err) = file.write_all(info.as_bytes()) {
            eprintln!("Failed to write to file: {}", err);
        }
    } else {
        eprintln!("Failed to open file: {}", file_path);
    }
}

pub fn create_yaml(folder_path: &str, file_name: &str) -> String {
    if fs::metadata(folder_path).is_err() {
        let _ = fs::create_dir_all(folder_path);
        info!("Created folder: {}", folder_path);
    }
    let file_path = format!("{}/{}.yaml", folder_path, file_name);
    if let Err(err) = fs::File::create(&file_path) {
        warn!("Failed to create file: {}", err);
    }
    file_path
}

pub fn create_scheduler_log_yaml(dir_path: &str, alg_name: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
    let file_name = format!("{}-{}-log", date, alg_name);
    create_yaml(dir_path, &file_name)
}

pub fn get_process_core_indices(process_result: &[ProcessResult]) -> Vec<usize> {
    process_result
        .iter()
        .enumerate()
        .filter_map(|(index, result)| {
            if matches!(result, ProcessResult::Continue) {
                Some(index)
            } else if let ProcessResult::Done(node_data) = result {
                // Do not include dummy source and sink nodes.
                if !node_data.params.contains_key("dummy") {
                    Some(index)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        dag.add_node(NodeData { id: 0, params });

        dag
    }

    fn create_dag_with_period(period: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        params.insert("period".to_owned(), period);
        let n0 = dag.add_node(NodeData { id: 0, params });

        params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        let n1 = dag.add_node(NodeData { id: 1, params });

        dag.add_edge(n0, n1, 0);

        dag
    }

    fn create_dag_with_deadline(deadline: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        let n0 = dag.add_node(NodeData { id: 0, params });

        params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        params.insert("end_to_end_deadline".to_owned(), deadline);
        let n1 = dag.add_node(NodeData { id: 1, params });

        dag.add_edge(n0, n1, 0);

        dag
    }

    fn create_dag_with_period_and_deadline(period: i32, deadline: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        params.insert("period".to_owned(), period);
        let n0 = dag.add_node(NodeData { id: 0, params });

        params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 4);
        params.insert("end_to_end_deadline".to_owned(), deadline);
        let n1 = dag.add_node(NodeData { id: 1, params });

        dag.add_edge(n0, n1, 0);

        dag
    }

    #[test]
    fn test_get_hyper_period_normal() {
        let dag_set = vec![
            create_dag_with_period(10),
            create_dag_with_period(20),
            create_dag_with_period(30),
            create_dag_with_period(40),
        ];
        assert_eq!(get_hyper_period(&dag_set), 120);
    }

    #[test]
    fn test_adjust_to_implicit_deadline_with_same_period_and_deadline() {
        let mut dag_set = vec![create_dag_with_period_and_deadline(10, 10)];
        adjust_to_implicit_deadline(&mut dag_set);
        assert_eq!(dag_set[0].get_head_period().unwrap(), 10);
        assert_eq!(dag_set[0].get_end_to_end_deadline().unwrap(), 10);
    }

    #[test]
    fn test_adjust_to_implicit_deadline_with_diff_period_and_deadline() {
        let mut dag_set = vec![create_dag_with_period_and_deadline(20, 10)];
        adjust_to_implicit_deadline(&mut dag_set);
        assert_eq!(dag_set[0].get_head_period().unwrap(), 20);
        assert_eq!(dag_set[0].get_end_to_end_deadline().unwrap(), 20);
    }

    #[test]
    fn test_adjust_to_implicit_deadline_with_period() {
        let mut dag_set = vec![create_dag_with_period(20)];
        adjust_to_implicit_deadline(&mut dag_set);
        assert_eq!(dag_set[0].get_head_period().unwrap(), 20);
        assert_eq!(dag_set[0].get_end_to_end_deadline().unwrap(), 20);
    }

    #[test]
    fn test_adjust_to_implicit_deadline_with_deadline() {
        let mut dag_set = vec![create_dag_with_deadline(20)];
        adjust_to_implicit_deadline(&mut dag_set);
        assert_eq!(dag_set[0].get_head_period().unwrap(), 20);
        assert_eq!(dag_set[0].get_end_to_end_deadline().unwrap(), 20);
    }

    #[test]
    #[should_panic]
    fn test_adjust_to_implicit_deadline_no_period_and_deadline() {
        let mut dag_set = vec![create_dag()];
        adjust_to_implicit_deadline(&mut dag_set);
    }
}
