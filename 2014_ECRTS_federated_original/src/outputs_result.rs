use crate::federated::FederateResult;
use lib::{
    graph_extension::NodeData,
    log::{dump_struct, DAGSetInfo, ProcessorInfo},
    processor::ProcessorBase,
};
use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ResultInfo<FederateResult> {
    result: FederateResult,
}

pub(crate) fn dump_federated_result_to_yaml(file_path: &str, result: FederateResult) {
    let result_info = ResultInfo { result };
    dump_struct(file_path, &result_info);
}

pub(crate) fn dump_dag_set_info_to_yaml(file_path: &str, dag_set: Vec<Graph<NodeData, i32>>) {
    let dag_set_info = DAGSetInfo::new(&dag_set);
    dump_struct(file_path, &dag_set_info);
}

pub(crate) fn dump_processor_info_to_yaml(file_path: &str, processor: &impl ProcessorBase) {
    let processor_info = ProcessorInfo::new(processor.get_number_of_cores());
    dump_struct(file_path, &processor_info);
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::{
        graph_extension::NodeData,
        homogeneous,
        tests_helper::create_node,
        util::{create_yaml, load_yaml},
    };
    use petgraph::Graph;
    use std::{collections::BTreeMap, fs::remove_file};

    fn create_high_utilization_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = {
            let mut params = BTreeMap::new();
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
            let mut params = BTreeMap::new();
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
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), 20);
        params.insert("period".to_owned(), 10);
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    #[test]
    fn test_dump_federated_result_to_yaml_normal() {
        let number_of_cores = 40;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_yaml("../lib/tests", "test_dump_federated_info_normal");
        dump_federated_result_to_yaml(&file_path, result);

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
    fn test_dump_federated_result_to_yaml_lack_cores_for_high_tasks() {
        let number_of_cores = 1;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_yaml("../lib/tests", "test_federated_lack_cores_for_high_tasks");
        dump_federated_result_to_yaml(&file_path, result);

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
    fn test_dump_federated_result_to_yaml_lack_cores_for_low_tasks() {
        let number_of_cores = 3;
        let mut dag_set = vec![
            create_high_utilization_dag(),
            create_low_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_yaml("../lib/tests", "test_federated_lack_cores_for_low_tasks");
        dump_federated_result_to_yaml(&file_path, result);

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
    fn test_dump_federated_result_to_yaml_unsuited_tasks() {
        let number_of_cores = 1;
        let mut dag_set = vec![create_period_exceeding_dag()];
        let result = crate::federated::federated(&mut dag_set, number_of_cores);
        let file_path = create_yaml("../lib/tests", "test_federated_unsuited_tasks");
        dump_federated_result_to_yaml(&file_path, result);

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
    fn test_dump_dag_set_info_to_yaml_normal() {
        let dag_set = vec![create_high_utilization_dag(), create_high_utilization_dag()];
        let file_path = create_yaml("../lib/tests", "dag_set_info");
        dump_dag_set_info_to_yaml(&file_path, dag_set);

        let yaml_docs = load_yaml(&file_path);
        let yaml_doc = &yaml_docs[0];

        assert_eq!(yaml_doc["total_utilization"].as_f64().unwrap(), 1.4285715);
        assert_eq!(
            yaml_doc["each_dag_info"][1]["critical_path_length"]
                .as_i64()
                .unwrap(),
            8
        );
        assert_eq!(yaml_doc["each_dag_info"][1]["period"].as_i64().unwrap(), 10);
        assert_eq!(
            yaml_doc["each_dag_info"][1]["end_to_end_deadline"]
                .as_i64()
                .unwrap(),
            0
        );
        assert_eq!(yaml_doc["each_dag_info"][1]["volume"].as_i64().unwrap(), 14);
        assert_eq!(
            yaml_doc["each_dag_info"][1]["utilization"]
                .as_f64()
                .unwrap(),
            0.71428573
        );

        remove_file(file_path).unwrap();
    }

    #[test]
    fn test_dump_processor_info_to_yaml() {
        let file_path = create_yaml("../lib/tests", "processor_info");
        let homogeneous_processor = homogeneous::HomogeneousProcessor::new(4);
        dump_processor_info_to_yaml(&file_path, &homogeneous_processor);

        let yaml_docs = load_yaml(&file_path);
        let yaml_doc = &yaml_docs[0];

        assert_eq!(yaml_doc["number_of_cores"].as_i64().unwrap(), 4);

        remove_file(file_path).unwrap();
    }
}
