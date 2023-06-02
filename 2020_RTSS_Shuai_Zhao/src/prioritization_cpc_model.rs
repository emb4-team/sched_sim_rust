use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

use crate::parallel_provider_consumer::*;

//Create a dag for f_consumer only
//NodeIndex is changed, but NodeData is retained.
//Example: id and params information
fn create_shrunk_dag(
    dag: &mut Graph<NodeData, i32>,
    retained_nodes: Vec<NodeIndex>,
) -> Graph<NodeData, i32> {
    let mut shrunk_dag = dag.clone();
    for node_i in shrunk_dag.node_indices().rev() {
        if !retained_nodes.contains(&node_i) {
            if shrunk_dag.remove_node(node_i).is_some() {
                // node is removed
            } else {
                panic!("Node does not exist: {:?}", node_i);
            }
        }
    }
    shrunk_dag
}

//Transformations to access params in the original graph
fn convert_node_index_to_id(node_indices: &[NodeIndex], dag: &Graph<NodeData, i32>) -> Vec<usize> {
    node_indices
        .iter()
        .map(|node_i| dag[*node_i].id as usize)
        .collect::<Vec<_>>()
}

fn remove_ids_with_priority(dag: &mut Graph<NodeData, i32>, ids: &mut Vec<usize>) {
    for node_i in dag.node_indices() {
        if dag[node_i].params.contains_key("priority") {
            if let Some(position) = ids.iter().position(|x| NodeIndex::new(*x) == node_i) {
                ids.remove(position);
            }
        }
    }
}

fn assign_priority_to_id(dag: &mut Graph<NodeData, i32>, ids: &Vec<usize>, priority: &mut i32) {
    for id in ids {
        dag.add_param(NodeIndex::new(*id), "priority", *priority);
        *priority += 1;
    }
}

#[allow(dead_code)] //TODO: remove
pub fn assign_priority_cpc_model(
    original_dag: &mut Graph<NodeData, i32>,
    dag: &mut Graph<NodeData, i32>,
    priority: &mut i32,
) {
    let critical_path = dag.get_critical_path();
    let providers = get_providers(dag, critical_path.clone());
    let mut f_consumers = get_f_consumers(dag, critical_path.clone());

    //Rule 1. give high priority to critical paths
    let mut critical_path_ids = convert_node_index_to_id(&critical_path, dag);
    assign_priority_to_id(original_dag, &critical_path_ids, priority);

    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            let mut f_consumer_dag = create_shrunk_dag(&mut dag.clone(), f_consumer.to_vec());

            while f_consumer_dag.node_count() != 0 {
                let f_consumer_critical_path = f_consumer_dag.get_critical_path();

                critical_path_ids =
                    convert_node_index_to_id(&f_consumer_critical_path, &f_consumer_dag);

                //:Recursion if there are dependencies in the f-consumer.
                for node_i in f_consumer_critical_path.clone() {
                    if let Some(pre_nodes) = f_consumer_dag.get_pre_nodes(node_i) {
                        if pre_nodes.len() > 1 {
                            assign_priority_cpc_model(original_dag, &mut f_consumer_dag, priority);
                            break;
                        }
                    }
                }

                //remove the nodes in the f_consumer_dag from the longest path
                remove_ids_with_priority(original_dag, &mut critical_path_ids);

                //Rule 3. give high priority to the nodes in the longest path
                assign_priority_to_id(original_dag, &critical_path_ids, priority);

                for node_i in f_consumer_critical_path {
                    f_consumer_dag.remove_node(node_i);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag_not_consolidated() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 10));
        let c2 = dag.add_node(create_node(2, "execution_time", 10));

        //create non-critical node.
        //No distinction is made because of the complexity.
        let n3 = dag.add_node(create_node(3, "execution_time", 3));
        let n4 = dag.add_node(create_node(4, "execution_time", 2));
        let n5 = dag.add_node(create_node(5, "execution_time", 3));
        let n6 = dag.add_node(create_node(6, "execution_time", 1));
        let n7 = dag.add_node(create_node(7, "execution_time", 1));
        let n8 = dag.add_node(create_node(8, "execution_time", 3));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n3, 1);
        dag.add_edge(n3, c2, 1);
        dag.add_edge(c0, n4, 1);
        dag.add_edge(n4, n6, 1);
        dag.add_edge(n5, n6, 1);
        dag.add_edge(c0, n5, 1);
        dag.add_edge(n5, n7, 1);
        dag.add_edge(n6, n8, 1);
        dag.add_edge(n7, n8, 1);
        dag.add_edge(n8, c2, 1);

        dag
    }

    fn create_sample_dag_complex() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 10));
        let c2 = dag.add_node(create_node(2, "execution_time", 10));

        //create non-critical node.
        //No distinction is made because of the complexity.
        let n3 = dag.add_node(create_node(3, "execution_time", 2));
        let n4 = dag.add_node(create_node(4, "execution_time", 2));
        let n5 = dag.add_node(create_node(5, "execution_time", 3));
        let n6 = dag.add_node(create_node(6, "execution_time", 1));
        let n7 = dag.add_node(create_node(7, "execution_time", 1));
        let n8 = dag.add_node(create_node(8, "execution_time", 3));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n3, 1);
        dag.add_edge(n3, c2, 1);
        dag.add_edge(c0, n4, 1);
        dag.add_edge(n4, n6, 1);
        dag.add_edge(c0, n5, 1);
        dag.add_edge(n5, n6, 1);
        dag.add_edge(n5, n7, 1);
        dag.add_edge(n6, n8, 1);
        dag.add_edge(n7, n8, 1);
        dag.add_edge(n8, c2, 1);

        dag
    }

    ///DAG in Figure 2 (b) of the paper
    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 4));
        let c1 = dag.add_node(create_node(1, "execution_time", 4));
        let c2 = dag.add_node(create_node(2, "execution_time", 4));
        let c3 = dag.add_node(create_node(3, "execution_time", 4));
        let c4 = dag.add_node(create_node(4, "execution_time", 4));

        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(5, "execution_time", 2));
        let n1_2 = dag.add_node(create_node(6, "execution_time", 1));
        let n0_3 = dag.add_node(create_node(7, "execution_time", 3));
        let n1_3 = dag.add_node(create_node(8, "execution_time", 2));
        let n2_3 = dag.add_node(create_node(9, "execution_time", 1));
        let n0_4 = dag.add_node(create_node(10, "execution_time", 3));
        let n1_4 = dag.add_node(create_node(11, "execution_time", 2));
        let n2_4 = dag.add_node(create_node(12, "execution_time", 2));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);
        dag.add_edge(c2, c3, 1);
        dag.add_edge(c3, c4, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1);
        dag.add_edge(n0_2, c2, 1);
        dag.add_edge(c0, n1_2, 1);
        dag.add_edge(n1_2, c2, 1);
        dag.add_edge(c0, n0_3, 1);
        dag.add_edge(n0_3, c3, 1);
        dag.add_edge(c1, n1_3, 1);
        dag.add_edge(n1_3, c3, 1);
        dag.add_edge(c1, n2_3, 1);
        dag.add_edge(n2_3, c3, 1);
        dag.add_edge(n0_3, n0_4, 1);
        dag.add_edge(n0_4, c4, 1);
        dag.add_edge(n1_3, n1_4, 1);
        dag.add_edge(n1_4, c4, 1);
        dag.add_edge(n2_3, n2_4, 1);
        dag.add_edge(n2_4, c4, 1);

        dag
    }

    #[test]
    fn test_assign_priority_cpc_model_normal() {
        let mut dag = create_sample_dag();
        let mut clone_dag = dag.clone();
        let expected_value = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

        assign_priority_cpc_model(&mut dag, &mut clone_dag, &mut 0);
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["priority"],
                expected_value[dag[node_i].id as usize]
            );
        }
    }

    #[test]
    fn test_assign_priority_cpc_model_normal_dag_not_consolidated() {
        let mut dag = create_sample_dag_not_consolidated();
        let mut clone_dag = dag.clone();
        let expected_value = vec![0, 1, 2, 8, 6, 3, 7, 4, 5];

        assign_priority_cpc_model(&mut dag, &mut clone_dag, &mut 0);
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["priority"],
                expected_value[dag[node_i].id as usize]
            );
        }
    }

    #[test]
    fn test_assign_priority_cpc_model_normal_recursion() {
        let mut dag = create_sample_dag_complex();
        let mut clone_dag = dag.clone();
        let expected_value = vec![0, 1, 2, 8, 6, 3, 7, 4, 5];

        assign_priority_cpc_model(&mut dag, &mut clone_dag, &mut 0);
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["priority"],
                expected_value[dag[node_i].id as usize]
            );
        }
    }
}
