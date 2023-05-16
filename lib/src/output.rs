use log::warn;
use petgraph::Graph;
use serde_derive::Serialize;
use serde_yaml;
use std::fs;
use std::io::Write;

use crate::graph_extension::{GraphExtension, NodeData};

#[derive(Serialize)]
struct DAGSet {
    total_utilization: f32,
    dag_set: Vec<DAG>,
}

#[derive(Serialize)]
#[allow(clippy::upper_case_acronyms)]
struct DAG {
    critical_path_length: f32,
    end_to_end_deadline: f32,
    volume: f32,
}

pub fn create_yaml_file(folder_path: &str, file_name: &str) -> String {
    let file_path = format!("{}/{}.yaml", folder_path, file_name);
    print!("Create file: {}", file_path);
    if let Err(err) = fs::File::create(&file_path) {
        warn!("Failed to create file: {}", err);
    }
    file_path
}

pub fn dag_set_info_to_yaml_file(mut dag_set: Vec<Graph<NodeData, f32>>, file_path: &str) {
    let mut total_utilization = 0.0;
    let mut dag_instances = Vec::new();

    for dag in dag_set.iter_mut() {
        let volume = dag.get_volume();
        let period = dag.get_head_period().unwrap();
        let critical_path = dag.get_critical_path();
        let critical_path_length = dag.get_total_wcet_from_nodes(&critical_path);
        total_utilization += volume / period;

        // Create DAG instance
        let dag_instance = DAG {
            critical_path_length,
            end_to_end_deadline: period,
            volume,
        };

        dag_instances.push(dag_instance);
    }

    let dag_set = DAGSet {
        total_utilization,
        dag_set: dag_instances,
    };

    // Serialize DAGSet to YAML
    let yaml = serde_yaml::to_string(&dag_set).expect("Failed to serialize DAGSet to YAML");

    // Write YAML to file
    std::fs::write(file_path, yaml).expect("Failed to write YAML file");
}

#[cfg(test)]
mod tests {
    use crate::dag_creator::create_dag_from_yaml;

    use super::*;
    use std::{collections::HashMap, fs::remove_file};

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 7.0));
        dag.add_edge(n0, n1, 1.0);
    }
}
