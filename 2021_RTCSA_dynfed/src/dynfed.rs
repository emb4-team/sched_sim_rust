use lib::graph_extension::{GraphExtension, NodeData};
use num_integer::lcm;
use petgraph::graph::Graph;

#[allow(dead_code)] //TODO: remove
pub fn get_hyper_period(dag_set: &Vec<Graph<NodeData, f32>>) -> i32 {
    let mut hyper_period = 1;
    for dag in dag_set {
        let dag_period = dag.get_head_period().unwrap() as i32;
        hyper_period = lcm(hyper_period, dag_period);
    }
    hyper_period
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_dag(period: f32) -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 4.0);
        params.insert("period".to_owned(), period);
        dag.add_node(NodeData { id: 0, params });

        dag
    }

    #[test]
    fn test_get_hyper_period() {
        let dag_set = vec![
            create_dag(10.0),
            create_dag(20.0),
            create_dag(30.0),
            create_dag(40.0),
        ];
        assert_eq!(get_hyper_period(&dag_set), 120);
    }
}
