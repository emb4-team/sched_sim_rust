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
        assert_yaml_value, assert_yaml_values_for_prefix, homogeneous,
        tests_helper::{common_yaml_test, create_high_utilization_dag, create_low_utilization_dag},
    };

    #[test]
    fn test_dump_federated_result_to_yaml_normal() {
        common_yaml_test(
            "test_dump_federated_info_normal",
            |path| {
                let result = crate::federated::federated(
                    &mut [
                        create_high_utilization_dag(),
                        create_high_utilization_dag(),
                        create_low_utilization_dag(),
                    ],
                    40,
                );
                dump_federated_result_to_yaml(path.to_str().unwrap(), result);
            },
            |yaml_doc| {
                assert_yaml_value!(yaml_doc, "result.high_dedicated_cores", i64, 6);
                assert_yaml_value!(yaml_doc, "result.low_dedicated_cores", i64, 34);
            },
        );
    }

    #[test]
    fn test_dump_dag_set_info_to_yaml_normal() {
        common_yaml_test(
            "dag_set_info",
            |path| {
                let path_str = path.to_str().unwrap();
                dump_dag_set_info_to_yaml(
                    path_str,
                    vec![create_high_utilization_dag(), create_high_utilization_dag()],
                );
            },
            |yaml_doc| {
                assert_yaml_value!(yaml_doc, "total_utilization", f64, 1.4285715);

                assert_yaml_values_for_prefix!(
                    yaml_doc,
                    "each_dag_info[1]",
                    [
                        ("critical_path_length", i64, 8),
                        ("period", i64, 10),
                        ("end_to_end_deadline", i64, 0),
                        ("volume", i64, 14),
                        ("utilization", f64, 0.71428573)
                    ]
                );
            },
        );
    }

    #[test]
    fn test_dump_processor_info_to_yaml() {
        common_yaml_test(
            "processor_info",
            |path| {
                let homogeneous_processor = homogeneous::HomogeneousProcessor::new(4);
                dump_processor_info_to_yaml(path.to_str().unwrap(), &homogeneous_processor);
            },
            |yaml_doc| {
                assert_yaml_value!(yaml_doc, "number_of_cores", i64, 4);
            },
        );
    }
}
