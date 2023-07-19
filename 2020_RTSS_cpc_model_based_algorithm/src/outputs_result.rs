use lib::log::dump_struct;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ResultInfo {
    schedule_length: i32,
    period_factor: f32,
    result: bool,
}

pub fn dump_cpc_result_to_yaml(
    file_path: &str,
    schedule_length: i32,
    period_factor: f32,
    result: bool,
) {
    let result_info = ResultInfo {
        schedule_length,
        period_factor,
        result,
    };
    dump_struct(file_path, &result_info);
}

#[cfg(test)]
mod tests {
    use crate::prioritization_cpc_model;

    use super::*;
    use lib::{
        graph_extension::{GraphExtension, NodeData},
        homogeneous::HomogeneousProcessor,
        processor::ProcessorBase,
        scheduler::DAGSchedulerBase,
        scheduler_creator::{create_scheduler, SchedulerType},
        util::create_yaml,
    };
    use petgraph::Graph;
    use std::{collections::HashMap, fs::remove_file};

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_cpc_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 4);
            params.insert("period".to_owned(), 10);
            dag.add_node(NodeData { id: 0, params })
        };
        let c1 = dag.add_node(create_node(1, "execution_time", 4));
        let c2 = dag.add_node(create_node(2, "execution_time", 3));
        //nY_X is the Yth preceding node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 3));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 3));
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

    #[test]
    fn test_dump_cpc_result_to_yaml_normal() {
        let mut dag = create_cpc_dag();

        prioritization_cpc_model::assign_priority_to_cpc_model(&mut dag);

        let homogeneous_processor = HomogeneousProcessor::new(7);
        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &homogeneous_processor,
        );

        let (schedule_length, _) = fixed_priority_scheduler.schedule();

        let file_path = create_yaml("../lib/tests", "test_dump_federated_info_normal");
        let result = schedule_length < dag.get_head_period().unwrap();
        dump_cpc_result_to_yaml(&file_path, schedule_length, 10.0, result);

        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let result_info: ResultInfo = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(result_info.schedule_length, 11);
        assert!(!result_info.result);

        remove_file(file_path).unwrap();
    }
}
