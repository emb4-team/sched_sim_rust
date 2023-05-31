use lib::output_log::append_info_to_yaml;
use serde_derive::{Deserialize, Serialize};

use crate::federated::FederateResult;

#[derive(Serialize, Deserialize)]
struct ResultInfo<FederateResult> {
    result: FederateResult,
}

pub fn dump_federated_result_to_file(file_path: &str, result: FederateResult) {
    let result_info = ResultInfo { result };
    let yaml =
        serde_yaml::to_string(&result_info).expect("Failed to serialize federated result to YAML");

    append_info_to_yaml(file_path, &yaml);
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::{graph_extension::NodeData, output_log::create_yaml_file};
    use petgraph::Graph;
    use std::{collections::HashMap, fs::remove_file};

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
        let dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(dag_set, number_of_cores);
        let file_path = create_yaml_file("../outputs", "test_dump_federated_info_normal");
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
        let dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(dag_set, number_of_cores);
        let file_path = create_yaml_file("../outputs", "test_federated_lack_cores_for_high_tasks");
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
        let dag_set = vec![
            create_high_utilization_dag(),
            create_low_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(dag_set, number_of_cores);
        let file_path = create_yaml_file("../outputs", "test_federated_lack_cores_for_low_tasks");
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
        let dag_set = vec![create_period_exceeding_dag()];
        let result = crate::federated::federated(dag_set, number_of_cores);
        let file_path = create_yaml_file("../outputs", "test_federated_unsuited_tasks");
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
}
