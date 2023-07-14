use lib::{
    graph_extension::NodeData,
    log::{DAGSetInfo, ProcessorInfo},
    processor::ProcessorBase,
    util::append_info_to_yaml,
};
use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};

use crate::federated::FederateResult;

#[derive(Serialize, Deserialize)]
struct ResultInfo<FederateResult> {
    result: FederateResult,
}

pub fn dump_processor_info_to_yaml(file_path: &str, processor: &impl ProcessorBase) {
    let number_of_cores = processor.get_number_of_cores();
    let processor_info = ProcessorInfo::new(number_of_cores);
    let yaml =
        serde_yaml::to_string(&processor_info).expect("Failed to serialize ProcessorInfo to YAML");
    append_info_to_yaml(file_path, &yaml);
}

pub fn dump_federated_result_to_file(file_path: &str, result: FederateResult) {
    let result_info = ResultInfo { result };
    let yaml =
        serde_yaml::to_string(&result_info).expect("Failed to serialize federated result to YAML");

    append_info_to_yaml(file_path, &yaml);
}

pub fn dump_dag_set_info_to_yaml(file_path: &str, dag_set: Vec<Graph<NodeData, i32>>) {
    let dag_set_info = DAGSetInfo::new(&dag_set);
    let yaml =
        serde_yaml::to_string(&dag_set_info).expect("Failed to serialize DAGSetInfo to YAML");

    append_info_to_yaml(file_path, &yaml);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use lib::{graph_extension::NodeData, homogeneous};
    use log::{info, warn};
    use petgraph::Graph;
    use std::{
        collections::HashMap,
        fs::{self, remove_file},
    };

    fn create_yaml_file_core(folder_path: &str, file_name: &str) -> String {
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

    fn create_scheduler_log_yaml_file(folder_path: &str, sched_name: &str) -> String {
        let now: DateTime<Utc> = Utc::now();
        let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
        let file_name = format!("{}-{}-log", date, sched_name);
        create_yaml_file_core(folder_path, &file_name)
    }

    #[derive(Deserialize)]
    pub struct TestDAGSetInfo {
        total_utilization: f32,
        each_dag_info: Vec<TestDAGInfo>,
    }

    #[derive(Deserialize)]
    struct TestDAGInfo {
        critical_path_length: i32,
        period: i32,
        end_to_end_deadline: i32,
        volume: i32,
        utilization: f32,
    }

    #[derive(Deserialize)]
    struct TestProcessorInfo {
        number_of_cores: usize,
    }

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_high_utilization_dag() -> Graph<NodeData, i32> {
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
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n0, n3, 1);

        dag
    }

    fn create_low_utilization_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 3);
            params.insert("period".to_owned(), 30);
            dag.add_node(NodeData { id: 2, params })
        };
        let n1 = dag.add_node(create_node(0, "execution_time", 3));
        let n2 = dag.add_node(create_node(1, "execution_time", 4));

        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag
    }

    fn create_period_exceeding_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 20);
        params.insert("period".to_owned(), 10);
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    #[test]
    fn test_dump_federated_result_to_file_normal() {
        let number_of_cores = 40;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path =
            create_scheduler_log_yaml_file("../lib/tests", "test_dump_federated_info_normal");
        dump_federated_result_to_file(&file_path, result);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo<FederateResult> = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(
            result_info.result,
            FederateResult::Schedulable {
                high_dedicated_cores: 6,
                low_dedicated_cores: 34,
            }
        );

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_federated_result_to_file_lack_cores_for_high_tasks() {
        let number_of_cores = 1;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_scheduler_log_yaml_file(
            "../lib/tests",
            "test_federated_lack_cores_for_high_tasks",
        );
        dump_federated_result_to_file(&file_path, result);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo<FederateResult> = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(
            result_info.result,
            FederateResult::Unschedulable {
                reason: (String::from("Insufficient number of cores for high-utilization tasks.")),
                insufficient_cores: 2
            }
        );

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_federated_result_to_file_lack_cores_for_low_tasks() {
        let number_of_cores = 3;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_low_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_scheduler_log_yaml_file(
            "../lib/tests",
            "test_federated_lack_cores_for_low_tasks",
        );
        dump_federated_result_to_file(&file_path, result);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo<FederateResult> = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(
            result_info.result,
            FederateResult::Unschedulable {
                reason: (String::from("Insufficient number of cores for low-utilization tasks.")),
                insufficient_cores: 2
            }
        );

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_federated_result_to_file_unsuited_tasks() {
        let number_of_cores = 1;
        let mut dag_set = vec![create_period_exceeding_dag()];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path =
            create_scheduler_log_yaml_file("../lib/tests", "test_federated_unsuited_tasks");
        dump_federated_result_to_file(&file_path, result);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo<FederateResult> = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(
            result_info.result,
            FederateResult::Unschedulable {
                reason: (String::from(
                    "The critical path length is greater than end_to_end_deadline."
                )),
                insufficient_cores: 0
            }
        );

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_dag_set_info_to_yaml_file_normal() {
        let dag_set = vec![create_high_utilization_dag(), create_high_utilization_dag()];
        let file_path = create_scheduler_log_yaml_file("../lib/tests", "dag_set_info");
        dump_dag_set_info_to_yaml(&file_path, dag_set);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let dag_set_info: TestDAGSetInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(dag_set_info.total_utilization, 1.4285715);
        assert_eq!(dag_set_info.each_dag_info.len(), 2);
        assert_eq!(dag_set_info.each_dag_info[1].critical_path_length, 8);
        assert_eq!(dag_set_info.each_dag_info[1].period, 10);
        assert_eq!(dag_set_info.each_dag_info[1].end_to_end_deadline, 0);
        assert_eq!(dag_set_info.each_dag_info[1].volume, 14);
        assert_eq!(dag_set_info.each_dag_info[1].utilization, 0.71428573);

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_processor_info_to_yaml() {
        let file_path = create_scheduler_log_yaml_file("../lib/tests", "processor_info");
        let homogeneous_processor = homogeneous::HomogeneousProcessor::new(4);
        dump_processor_info_to_yaml(&file_path, &homogeneous_processor);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let processor_info: TestProcessorInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(processor_info.number_of_cores, 4);

        remove_file(file_path).unwrap();
    }
}
