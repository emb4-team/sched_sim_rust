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
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 1.0));
        let c1 = dag.add_node(create_node(1, "execution_time", 1.0));
        let c2 = dag.add_node(create_node(2, "execution_time", 1.0));
        let c3 = dag.add_node(create_node(3, "execution_time", 1.0));
        let c4 = dag.add_node(create_node(4, "execution_time", 1.0));
        let c5 = dag.add_node(create_node(5, "execution_time", 1.0));
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(6, "execution_time", 0.0));
        let n0_5 = dag.add_node(create_node(7, "execution_time", 0.0));

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);
        dag.add_edge(c1, c2, 1.0);
        dag.add_edge(c2, c3, 1.0);
        dag.add_edge(c3, c4, 1.0);
        dag.add_edge(c4, c5, 1.0);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1.0);
        dag.add_edge(n0_2, c2, 1.0);
        dag.add_edge(c3, n0_5, 1.0);
        dag.add_edge(n0_5, c5, 1.0);

        dag
    }

    #[test]
    fn get_providers_normal() {
        let dag = create_sample_dag();
        let providers = get_providers(dag);
        assert_eq!(providers.len(), 3);
        println!("{:?}", providers);

        assert_eq!(providers[0][0].index(), 0);
        assert_eq!(providers[0][1].index(), 1);
        assert_eq!(providers[1][0].index(), 2);
        assert_eq!(providers[1][1].index(), 3);
        assert_eq!(providers[1][2].index(), 4);
        assert_eq!(providers[2][0].index(), 5);
    }
}
