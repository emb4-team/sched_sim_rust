//! CPC model construction.
//! Paper Information
//! -----------------
//! Title: DAG Scheduling and Analysis on Multiprocessor Systems: Exploitation of Parallelism and Dependency
//! Authors: Shuai Zhao, Xiaotian Dai, Iain Bate, Alan Burns, Wanli Chang
//! Conference: RTSS 2020
//! -----------------
use std::collections::VecDeque;

use lib::graph_extension::*;
use petgraph::graph::{Graph, NodeIndex};

/// Algorithm 1: Step1 identifying capacity providers.
/// capacity provider is a sub paths of the critical path
/// See the second paragraph of IV. A. Concurrent provider and consumer model for a detailed explanation.
#[allow(dead_code)] // TODO: remove
pub fn get_providers(mut dag: Graph<NodeData, f32>) -> Vec<Vec<NodeIndex>> {
    let mut critical_path: VecDeque<NodeIndex> = dag.get_critical_path().into();
    let mut providers: Vec<Vec<NodeIndex>> = Vec::new();
    while !critical_path.is_empty() {
        let mut provider = vec![critical_path.pop_front().unwrap()];
        while !critical_path.is_empty() && dag.get_pre_nodes(critical_path[0]).unwrap().len() == 1 {
            provider.push(critical_path.pop_front().unwrap());
        }
        providers.push(provider);
    }
    providers
}

#[cfg(test)]

mod tests {
    use std::collections::HashMap;

    use super::*;

    fn create_sample_dag() -> Graph<NodeData, f32> {
        fn create_node(id: i32, key: &str, value: f32) -> NodeData {
            let mut params = HashMap::new();
            params.insert(key.to_string(), value);
            NodeData { id, params }
        }
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 1.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 1.0));
        let n1_0 = dag.add_node(create_node(2, "execution_time", 0.0));
        let n1_1 = dag.add_node(create_node(3, "execution_time", 0.0));

        let n2 = dag.add_node(create_node(4, "execution_time", 1.0));
        let n2_0 = dag.add_node(create_node(5, "execution_time", 0.0));
        let n2_1 = dag.add_node(create_node(6, "execution_time", 0.0));
        let n2_2 = dag.add_node(create_node(7, "execution_time", 0.0));

        let n3 = dag.add_node(create_node(8, "execution_time", 1.0));
        let n3_0 = dag.add_node(create_node(9, "execution_time", 0.0));
        let n3_1 = dag.add_node(create_node(10, "execution_time", 0.0));
        let n3_2 = dag.add_node(create_node(11, "execution_time", 0.0));
        let n4 = dag.add_node(create_node(12, "execution_time", 1.0));

        dag.add_edge(n0, n1, 0.0);
        dag.add_edge(n1, n2, 0.0);
        dag.add_edge(n2, n3, 0.0);
        dag.add_edge(n3, n4, 0.0);

        dag.add_edge(n0, n1_0, 0.0);
        dag.add_edge(n0, n1_1, 0.0);
        dag.add_edge(n0, n2_0, 0.0);

        dag.add_edge(n1_0, n2, 0.0);
        dag.add_edge(n1_1, n2, 0.0);

        dag.add_edge(n1, n2_1, 0.0);
        dag.add_edge(n1, n2_2, 0.0);

        dag.add_edge(n1_0, n2_1, 0.0);
        dag.add_edge(n1_1, n2_2, 0.0);

        dag.add_edge(n2_0, n3, 0.0);
        dag.add_edge(n2_1, n3, 0.0);
        dag.add_edge(n2_2, n3, 0.0);

        dag.add_edge(n2_0, n3_0, 0.0);
        dag.add_edge(n2_1, n3_1, 0.0);
        dag.add_edge(n2_2, n3_2, 0.0);

        dag.add_edge(n3_0, n4, 0.0);
        dag.add_edge(n3_1, n4, 0.0);
        dag.add_edge(n3_2, n4, 0.0);

        dag
    }

    #[test]
    fn get_providers_normal() {
        let dag = create_sample_dag();
        let providers = get_providers(dag);
        assert_eq!(providers.len(), 4);

        assert_eq!(providers[0][0].index(), 0);
        assert_eq!(providers[0][1].index(), 1);
        assert_eq!(providers[1][0].index(), 4);
        assert_eq!(providers[2][0].index(), 8);
        assert_eq!(providers[3][0].index(), 12);
    }
}
