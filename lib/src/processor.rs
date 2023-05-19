use crate::{core::*, graph_extension::NodeData};
use petgraph::Graph;

pub trait ProcessorBase {
    fn new(num_cores: usize) -> Self;
    fn set_time_unit(&mut self, time_unit: i32);
    fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool;
    fn process(&mut self) -> Vec<ProcessResult>;
    fn get_number_of_cores(&self) -> usize;
    fn get_idle_core_index(&mut self) -> Option<usize>;
}

pub fn get_minimum_time_unit_from_dag_set(dag_set: &Vec<Graph<NodeData, f32>>) -> i32 {
    fn round_fraction(num: f32, decimal_places: u32) -> f32 {
        let multiplier = 10_f32.powi(decimal_places as i32);
        (num * multiplier).round() / multiplier
    }
    let mut decimal_count = 1;
    // Initial value is set to 1.0 and returned as is if no decimal point is found.
    for dag in dag_set {
        for node_index in dag.node_indices() {
            let exec_time = &dag[node_index].params["execution_time"];
            // Check all nodes in dag_set because we need to use the same unit time in the processor
            if exec_time.fract() != 0.0 {
                // -2 to remove "0." from the string
                decimal_count = 10u32.pow(
                    (round_fraction(exec_time.fract(), 5)
                        .abs()
                        .to_string()
                        .chars()
                        .count()
                        - 2) as u32,
                );
                let new_time_unit = decimal_count;
                if new_time_unit > decimal_count {
                    decimal_count = new_time_unit;
                }
            }
        }
    }
    decimal_count as i32
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_get_minimum_time_unit_from_dag_set() {
        let mut dag0 = Graph::<NodeData, f32>::new();
        dag0.add_node(create_node(0, "execution_time", 30.0));

        let mut dag_set = vec![dag0];
        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 1);

        let mut dag1 = Graph::<NodeData, f32>::new();
        dag1.add_node(create_node(1, "execution_time", 3.0));
        dag_set.push(dag1);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 1);

        let mut dag2 = Graph::<NodeData, f32>::new();
        dag2.add_node(create_node(2, "execution_time", 50.2));
        dag_set.push(dag2);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 10);

        let mut dag3 = Graph::<NodeData, f32>::new();
        dag3.add_node(create_node(3, "execution_time", 0.03));
        dag_set.push(dag3);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 100);
    }
}
