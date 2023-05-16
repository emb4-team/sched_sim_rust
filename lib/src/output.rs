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

pub fn graph_info_to_yaml_file(dag: &Graph<NodeData, f32>, file_path: &str) -> f32 {
    let volume = dag.get_volume();
    let period = dag.get_head_period().unwrap();
    //let critical_path_length = dag.get_critical_path_length();
    let utilization = 1;
    1.0
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

        let file_path = create_yaml_file("../outputs", "test");
        let _ = graph_to_yaml_file(&dag, &file_path);

        let dag = create_dag_from_yaml(file_path.as_str());
        assert_eq!(dag.node_count(), 2);
        assert_eq!(dag.edge_count(), 1);
        assert_eq!(dag[n0].id, 0);
        assert_eq!(dag[n1].id, 1);
        assert_eq!(dag[n0].params["execution_time"], 4.0);
        assert_eq!(dag[n1].params["execution_time"], 7.0);
        assert_eq!(dag[dag.edge_indices().next().unwrap()], 1.0);
        let _ = remove_file(file_path);
    }
}
