use crate::graph_extension::{GraphExtension, NodeData};
use log::warn;
use num_integer::lcm;
use petgraph::graph::Graph;
use petgraph::graph::NodeIndex;

pub fn get_hyper_period(dag_set: &Vec<Graph<NodeData, i32>>) -> i32 {
    let mut hyper_period = 1;
    for dag in dag_set {
        let dag_period = dag.get_head_period().unwrap();
        hyper_period = lcm(hyper_period, dag_period);
    }
    hyper_period
}

pub fn adjust_to_implicit_deadline(dag_set: &mut [Graph<NodeData, i32>]) {
    for dag in dag_set.iter_mut() {
        if let (Some(period), Some(end_to_end_deadline)) =
            (dag.get_head_period(), dag.get_end_to_end_deadline())
        {
            enforce_equal_period_and_deadline(dag, period, end_to_end_deadline);
        } else if dag.get_head_period().is_none() {
            handle_missing_period(dag);
        } else if dag.get_end_to_end_deadline().is_none() {
            handle_missing_deadline(dag);
        }
    }
}

fn enforce_equal_period_and_deadline(
    dag: &mut Graph<NodeData, i32>,
    period: i32,
    end_to_end_deadline: i32,
) {
    if end_to_end_deadline != period {
        warn!("In this algorithm, the period and the end-to-end deadline must be equal. Therefore, the end-to-end deadline is overridden by the period.");
        let source_nodes = dag.get_source_nodes();
        let node_i = source_nodes
            .iter()
            .find(|&&node_i| dag[node_i].params.get("end_to_end_deadline").is_some())
            .unwrap();
        dag.update_param(*node_i, "end_to_end_deadline", period);
    }
}

fn handle_missing_period(dag: &mut Graph<NodeData, i32>) {
    let end_to_end_deadline = dag
        .get_end_to_end_deadline()
        .expect("Either an end-to-end deadline or period of time is required for the schedule.");
    dag.add_param(NodeIndex::new(0), "period", end_to_end_deadline);
}

fn handle_missing_deadline(dag: &mut Graph<NodeData, i32>) {
    let period = dag
        .get_head_period()
        .expect("Either an end-to-end deadline or period of time is required for the schedule.");
    dag.add_param(
        NodeIndex::new(dag.node_count() - 1),
        "end_to_end_deadline",
        period,
    );
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
}
