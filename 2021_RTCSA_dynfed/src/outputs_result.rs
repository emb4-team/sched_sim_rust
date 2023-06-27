use lib::output_log::append_info_to_yaml;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ResultInfo {
    schedule_length: i32,
    hyper_period: i32,
    result: bool,
}

pub fn dump_dynfed_result_to_file(
    file_path: &str,
    schedule_length: i32,
    hyper_period: i32,
    result: bool,
) {
    let result_info = ResultInfo {
        schedule_length,
        hyper_period,
        result,
    };
    let yaml =
        serde_yaml::to_string(&result_info).expect("Failed to serialize federated result to YAML");

    append_info_to_yaml(file_path, &yaml);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adjust_to_implicit_deadline;
    use crate::dynfed::DynamicFederatedScheduler;
    use lib::{
        graph_extension::{GraphExtension, NodeData},
        homogeneous::HomogeneousProcessor,
        non_preemptive_scheduler::NonPreemptiveScheduler,
        output_log::create_scheduler_log_yaml_file,
        processor::ProcessorBase,
        scheduler::DAGSetSchedulerBase,
        util::get_hyper_period,
    };
    use petgraph::Graph;
    use std::collections::HashMap;
    use std::fs::remove_file;

    use super::dump_dynfed_result_to_file;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 50);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 10));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);
        dag.add_edge(n0_0, c2, 1);
        dag.add_edge(n1_0, c2, 1);

        dag
    }

    fn create_sample_dag2() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 11));
        let c1 = dag.add_node(create_node(1, "execution_time", 21));
        let c2 = dag.add_node(create_node(2, "execution_time", 21));
        dag.add_param(c0, "period", 53);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 11));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(n0_0, c2, 1);

        dag
    }

    #[test]
    fn test_dump_dynfed_result_to_file_normal() {
        let dag = create_sample_dag();
        let dag2 = create_sample_dag2();
        let mut dag_set = vec![dag, dag2];

        adjust_to_implicit_deadline(&mut dag_set);

        let mut dynfed_scheduler: DynamicFederatedScheduler<
            NonPreemptiveScheduler<HomogeneousProcessor>,
        > = DynamicFederatedScheduler::new(&dag_set, &HomogeneousProcessor::new(4));

        let schedule_length = dynfed_scheduler.schedule();

        let file_path = create_scheduler_log_yaml_file(
            "../lib/tests",
            "test_dump_dynfed_result_to_file_normal()",
        );

        dump_dynfed_result_to_file(
            &file_path,
            schedule_length,
            get_hyper_period(&dag_set),
            schedule_length < get_hyper_period(&dag_set),
        );

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(result_info.schedule_length, 103);
        assert_eq!(result_info.hyper_period, 2650);
        assert!(result_info.result);

        remove_file(file_path).unwrap();
    }
}
