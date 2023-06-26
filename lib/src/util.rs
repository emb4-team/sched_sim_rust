use crate::graph_extension::{GraphExtension, NodeData};
use log::warn;
use num_integer::lcm;
use petgraph::graph::Graph;

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
        let period = dag.get_head_period();
        let end_to_end_deadline = dag.get_end_to_end_deadline();
        match (period, end_to_end_deadline) {
            (Some(period_value), Some(_)) => {
                if end_to_end_deadline != period {
                    warn!("In this algorithm, the period and the end-to-end deadline must be equal. Therefore, the end-to-end deadline is overridden by the period.");
                    dag.get_sink_nodes().iter().for_each(|&sink_i| {
                        if dag[sink_i].params.get("end_to_end_deadline").is_some() {
                            dag.update_param(sink_i, "end_to_end_deadline", period_value);
                        }
                    });
                }
            }
            (None, Some(deadline_value)) => {
                dag.add_param(dag.get_source_nodes()[0], "period", deadline_value);
            }
            (Some(period_value), None) => {
                dag.add_param(dag.get_sink_nodes()[0], "end_to_end_deadline", period_value);
            }
            (None, None) => {
                panic!("Either an period or end-to-end deadline is required for the schedule.");
            }
        }
    }
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
