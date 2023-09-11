use crate::parallel_provider_consumer::{get_f_consumers, get_providers};
use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

//Create a dag for f_consumer only
//NodeIndex is changed, but NodeData is retained.
//Example: id and params information
fn create_shrunk_dag(
    referenced_dag: &mut Graph<NodeData, i32>,
    retained_nodes: Vec<NodeIndex>,
) -> Graph<NodeData, i32> {
    let mut shrunk_dag = referenced_dag.clone();
    for node_i in shrunk_dag.node_indices().rev() {
        if !retained_nodes.contains(&node_i) && shrunk_dag.remove_node(node_i).is_none() {
            panic!("Node does not exist: {:?}", node_i);
        }
    }
    shrunk_dag
}

fn convert_shrunk_indices_to_original(
    shrunk_dag: &Graph<NodeData, i32>,
    shrunk_indices: &[NodeIndex],
) -> Vec<NodeIndex> {
    shrunk_indices
        .iter()
        .map(|&node_i| NodeIndex::new(shrunk_dag[node_i].id as usize))
        .collect()
}

fn prioritize_path_from_head_with_increment(
    original_dag: &mut Graph<NodeData, i32>,
    path: &[NodeIndex],
    base_priority: &mut i32,
) {
    for node_i in path {
        if !original_dag[*node_i].params.contains_key("priority") {
            original_dag.add_param(*node_i, "priority", *base_priority);
            *base_priority += 1;
        }
    }
}

#[allow(dead_code)] //TODO: remove
pub fn assign_priority_to_cpc_model(dag: &mut Graph<NodeData, i32>) {
    assign_priority_to_cpc_model_core(dag, &mut dag.clone(), &mut 0);
}

fn assign_priority_to_cpc_model_core(
    original_dag: &mut Graph<NodeData, i32>,
    shrunk_dag: &mut Graph<NodeData, i32>,
    current_priority: &mut i32,
) {
    let critical_path = shrunk_dag.get_critical_path();
    let providers = get_providers(shrunk_dag, &critical_path);
    let f_consumers = get_f_consumers(shrunk_dag, &critical_path);
    //Rule 1. Priority is given to critical nodes
    prioritize_path_from_head_with_increment(
        original_dag,
        &convert_shrunk_indices_to_original(shrunk_dag, &critical_path),
        current_priority,
    );
    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get(&provider) {
            let mut f_consumer_dag = create_shrunk_dag(shrunk_dag, f_consumer.to_vec());
            while f_consumer_dag.node_count() != 0 {
                let f_consumer_critical_path = f_consumer_dag.get_critical_path();
                //recursion if there are dependencies in the f-consumer.
                if f_consumer_critical_path.iter().any(|&node_i| {
                    f_consumer_dag
                        .get_pre_nodes(node_i)
                        .map_or(false, |pre_nodes| pre_nodes.len() > 1)
                }) {
                    assign_priority_to_cpc_model_core(
                        original_dag,
                        &mut f_consumer_dag,
                        current_priority,
                    );
                } else {
                    //Rule 3. give high priority to the nodes in the longest path
                    prioritize_path_from_head_with_increment(
                        original_dag,
                        &convert_shrunk_indices_to_original(
                            &f_consumer_dag,
                            &f_consumer_critical_path,
                        ),
                        current_priority,
                    );
                }
                f_consumer_dag.remove_nodes(&f_consumer_critical_path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::tests_helper::create_dag_for_cpc;

    #[test]
    fn test_assign_priority_cpc_model_normal() {
        let mut dag = create_dag_for_cpc();
        assign_priority_to_cpc_model(&mut dag);

        let expected_value = [0, 1, 2, 8, 6, 3, 7, 4, 5];
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["priority"],
                expected_value[dag[node_i].id as usize]
            );
        }
    }
}
