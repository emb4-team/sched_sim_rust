use log::{info, warn};
use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};
use serde_yaml;
use std::fs::{self, OpenOptions};
use std::io::Write;

use crate::graph_extension::{GraphExtension, NodeData};
use crate::processor::ProcessorBase;

#[derive(Serialize, Deserialize)]
struct DAGSetInfo {
    total_utilization: f32,
    each_dag_info: Vec<DAGInfo>,
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
struct DAGInfo {
    critical_path_length: i32,
    end_to_end_deadline: i32,
    volume: i32,
}

#[derive(Serialize, Deserialize)]
struct ProcessorInfo {
    number_of_cores: usize,
}

pub fn create_yaml_file(folder_path: &str, file_name: &str) -> String {
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

pub fn dump_processor_info_to_yaml(file_path: &str, processor: impl ProcessorBase) {
    let number_of_cores = processor.get_number_of_cores();
    let processor_info = ProcessorInfo { number_of_cores };
    let yaml =
        serde_yaml::to_string(&processor_info).expect("Failed to serialize ProcessorInfo to YAML");
    append_info_to_yaml(file_path, &yaml);
}

pub fn dump_dag_set_info_to_yaml(file_path: &str, mut dag_set: Vec<Graph<NodeData, i32>>) {
    let mut total_utilization = 0.0;
    let mut each_dag_info = Vec::new();

    for dag in dag_set.iter_mut() {
        let volume = dag.get_volume();
        let period = dag.get_head_period().unwrap();
        let critical_path = dag.get_critical_path();
        let critical_path_length = dag.get_total_wcet_from_nodes(&critical_path);
        total_utilization += volume as f32 / period as f32;

        let dag_info = DAGInfo {
            critical_path_length,
            end_to_end_deadline: period,
            volume,
        };

        each_dag_info.push(dag_info);
    }

    let dag_set_info = DAGSetInfo {
        total_utilization,
        each_dag_info,
    };

    let yaml =
        serde_yaml::to_string(&dag_set_info).expect("Failed to serialize DAGSetInfo to YAML");

    append_info_to_yaml(file_path, &yaml);
}

#[cfg(test)]
mod tests {
    use crate::homogeneous;

    use super::*;
    use std::{collections::HashMap, fs::remove_file, thread, time::Duration};

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 4);
            params.insert("period".to_owned(), 10);
            dag.add_node(NodeData { id: 3, params })
        };
        let n1 = dag.add_node(create_node(1, "execution_time", 4));
        let n2 = dag.add_node(create_node(2, "execution_time", 3));
        let n3 = dag.add_node(create_node(3, "execution_time", 3));
        dag.add_edge(n0, n1, 0);
        dag.add_edge(n0, n2, 0);
        dag.add_edge(n0, n3, 0);

        dag
    }

    #[test]
    fn test_dump_dag_set_info_to_yaml_file_normal() {
        let dag_set = vec![create_dag(), create_dag()];
        let file_path = create_yaml_file("../outputs", "tests");
        dump_dag_set_info_to_yaml(&file_path, dag_set);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let dag_set: DAGSetInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(dag_set.total_utilization, 2.8);
        assert_eq!(dag_set.each_dag_info.len(), 2);
        assert_eq!(dag_set.each_dag_info[1].critical_path_length, 8);
        assert_eq!(dag_set.each_dag_info[1].end_to_end_deadline, 10);
        assert_eq!(dag_set.each_dag_info[1].volume, 14);
        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_processor_info_to_yaml() {
        let file_path = create_yaml_file("../outputs", "tests");
        let homogeneous_processor = homogeneous::HomogeneousProcessor::new(4);
        dump_processor_info_to_yaml(&file_path, homogeneous_processor);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let number_of_cores: ProcessorInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(number_of_cores.number_of_cores, 4);
        remove_file(file_path).unwrap();
    }
}
