use petgraph::graph::Graph;

use std::cmp::Ordering;

use crate::getset_dag_set_scheduler;
use crate::graph_extension::GraphExtension;
use crate::scheduler::NodeDataWrapper;
use crate::{
    graph_extension::NodeData, homogeneous::HomogeneousProcessor, log::DAGSetSchedulerLog,
    processor::ProcessorBase, scheduler::DAGSetSchedulerBase,
};

impl PartialOrd for NodeDataWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Define the keys to compare
        let key1 = "period";
        let key2 = "dag_id";
        match (self.0.params.get(key1), other.0.params.get(key1)) {
            (Some(self_val), Some(other_val)) => match self_val.cmp(other_val) {
                // If the keys are equal, compare by id
                Ordering::Equal => match self.0.id.partial_cmp(&other.0.id) {
                    // If the ids are also equal, compare by dag_id
                    Some(Ordering::Equal) => {
                        match (self.0.params.get(key2), other.0.params.get(key2)) {
                            (Some(self_dag), Some(other_dag)) => Some(self_dag.cmp(other_dag)),
                            (None, None) => Some(Ordering::Equal),
                            (Some(_), None) => Some(Ordering::Greater),
                            (None, Some(_)) => Some(Ordering::Less),
                        }
                    }
                    other => other,
                },
                other => Some(other),
            },
            // If neither of the keys exists, compare by id
            (None, None) => match self.0.id.partial_cmp(&other.0.id) {
                // If the ids are equal, compare by dag_id
                Some(Ordering::Equal) => {
                    match (self.0.params.get(key2), other.0.params.get(key2)) {
                        (Some(self_dag), Some(other_dag)) => Some(self_dag.cmp(other_dag)),
                        (None, None) => Some(Ordering::Equal),
                        (Some(_), None) => Some(Ordering::Greater),
                        (None, Some(_)) => Some(Ordering::Less),
                    }
                }
                other => other,
            },
            // If only one of the keys exists, the one with the key is greater
            (Some(_), None) => Some(Ordering::Greater),
            (None, Some(_)) => Some(Ordering::Less),
        }
    }
}

impl Ord for NodeDataWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub struct GlobalEDFScheduler {
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    log: DAGSetSchedulerLog,
}

impl DAGSetSchedulerBase<HomogeneousProcessor> for GlobalEDFScheduler {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        let mut dag_set = dag_set.to_vec();
        for (dag_id, dag) in dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
        }

        Self {
            dag_set: dag_set.clone(),
            processor: processor.clone(),
            log: DAGSetSchedulerLog::new(&dag_set, processor.get_number_of_cores()),
        }
    }

    getset_dag_set_scheduler!(HomogeneousProcessor);
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{graph_extension::GraphExtension, util::load_yaml};
    use std::{collections::BTreeMap, fs::remove_file};

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        // cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 150);
        dag.add_param(c2, "end_to_end_deadline", 50);
        // nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 10));

        // Create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        // Create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);
        dag.add_edge(n0_0, c2, 1);
        dag.add_edge(n1_0, c2, 1);

        dag
    }

    fn create_sample_dag2() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        // cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 100);
        dag.add_param(c2, "end_to_end_deadline", 60);
        // nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));

        // Create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        // Create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(n0_0, c2, 1);

        dag
    }

    #[test]
    fn test_global_edf_normal() {
        let dag = create_sample_dag();
        let dag2 = create_sample_dag2();
        let dag_set = vec![dag, dag2];

        let processor = HomogeneousProcessor::new(4);

        let mut global_edf_scheduler = GlobalEDFScheduler::new(&dag_set, &processor);
        let time = global_edf_scheduler.schedule();

        assert_eq!(time, 300);

        let file_path = global_edf_scheduler.dump_log("../lib/tests", "edf_test");
        let yaml_docs = load_yaml(&file_path);
        let yaml_doc = &yaml_docs[0];

        // Check the value of total_utilization
        assert_eq!(
            yaml_doc["dag_set_info"]["total_utilization"]
                .as_f64()
                .unwrap(),
            3.8095236
        );

        // Check the value of each_dag_info
        let each_dag_info = &yaml_doc["dag_set_info"]["each_dag_info"][0];
        assert_eq!(each_dag_info["critical_path_length"].as_i64().unwrap(), 50);
        assert_eq!(each_dag_info["period"].as_i64().unwrap(), 150);
        assert_eq!(each_dag_info["end_to_end_deadline"].as_i64().unwrap(), 50);
        assert_eq!(each_dag_info["volume"].as_i64().unwrap(), 70);
        assert_eq!(each_dag_info["utilization"].as_f64().unwrap(), 2.142857);

        // Check the value of processor_info
        assert_eq!(
            yaml_doc["processor_info"]["number_of_cores"]
                .as_i64()
                .unwrap(),
            4
        );

        // Check the value of dag_set_log
        let dag_set_log = &yaml_doc["dag_set_log"][0];
        assert_eq!(dag_set_log["dag_id"].as_i64().unwrap(), 0);
        let release_time = &dag_set_log["release_time"];
        assert_eq!(release_time[0].as_i64().unwrap(), 0);
        assert_eq!(release_time[1].as_i64().unwrap(), 150);
        let finish_time = &dag_set_log["finish_time"];
        assert_eq!(finish_time[0].as_i64().unwrap(), 50);
        assert_eq!(finish_time[1].as_i64().unwrap(), 200);

        // Check the value of node_set_logs
        let node_set_logs = &yaml_doc["node_set_logs"][0][0];
        let core_id = &node_set_logs["core_id"];
        assert_eq!(core_id[0].as_i64().unwrap(), 1);
        assert_eq!(core_id[1].as_i64().unwrap(), 0);
        assert_eq!(node_set_logs["dag_id"].as_i64().unwrap(), 0);
        assert_eq!(node_set_logs["node_id"].as_i64().unwrap(), 0);
        let start_time = &node_set_logs["start_time"];
        assert_eq!(start_time[0].as_i64().unwrap(), 0);
        assert_eq!(start_time[1].as_i64().unwrap(), 150);
        let finish_time = &node_set_logs["finish_time"];
        assert_eq!(finish_time[0].as_i64().unwrap(), 10);
        assert_eq!(finish_time[1].as_i64().unwrap(), 160);

        // Check the value of processor_log
        let processor_log = &yaml_doc["processor_log"];
        assert_eq!(
            processor_log["average_utilization"].as_f64().unwrap(),
            0.26666668
        );
        assert_eq!(
            processor_log["variance_utilization"].as_f64().unwrap(),
            0.06055556
        );

        // Check the value of core_logs
        let core_logs = &processor_log["core_logs"][0];
        assert_eq!(core_logs["core_id"].as_i64().unwrap(), 0);
        assert_eq!(core_logs["total_proc_time"].as_i64().unwrap(), 200);
        assert_eq!(core_logs["utilization"].as_f64().unwrap(), 0.6666667);

        remove_file(file_path).unwrap();
    }
}
