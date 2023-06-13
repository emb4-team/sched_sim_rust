use num_integer::lcm;
use petgraph::graph::Graph;

use crate::graph_extension::{GraphExtension, NodeData};

#[allow(dead_code)] //TODO: remove
pub fn get_hyper_period(dag_set: &Vec<Graph<NodeData, i32>>) -> i32 {
    let mut hyper_period = 1;
    for dag in dag_set {
        let dag_period = dag.get_head_period().unwrap();
        hyper_period = lcm(hyper_period, dag_period);
    }
    hyper_period
}

pub fn get_dag_id(node_data: &NodeData) -> usize {
    node_data.params["dag_id"] as usize
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_dag(period: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 4);
        params.insert("period".to_owned(), period);
        dag.add_node(NodeData { id: 0, params });

        dag
    }

    #[test]
    fn test_get_hyper_period_normal() {
        let dag_set = vec![
            create_dag(10),
            create_dag(20),
            create_dag(30),
            create_dag(40),
        ];
        assert_eq!(get_hyper_period(&dag_set), 120);
    }

    #[test]
    fn test_get_dag_id_normal() {
        let mut params = HashMap::new();
        params.insert("dag_id".to_owned(), 1);
        let node_data = NodeData { id: 0, params };
        assert_eq!(get_dag_id(&node_data), 1);
    }
}
