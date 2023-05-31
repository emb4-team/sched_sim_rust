use std::collections::HashSet;

use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

use crate::parallel_provider_consumer::*;

fn remove_nodes(dag: &mut Graph<NodeData, i32>, nodes: Vec<NodeIndex>) {
    let mut nodes_to_remove = Vec::new();
    for node in dag.node_indices() {
        if !nodes.contains(&node) {
            nodes_to_remove.push(node);
        }
    }
    for node in nodes_to_remove {
        dag.remove_node(node);
    }
}

pub fn prioritization_cpc_model_loop(
    dag: &mut Graph<NodeData, i32>,
    clone_dag: &mut Graph<NodeData, i32>,
    priority: &mut i32,
    critical_path: Vec<NodeIndex>,
) {
    // Clone and original have misaligned NodeIndexes.
    // Therefore, the critical path is aligned with the clone.
    let mut critical_path_nodes = Vec::new();
    for critical_node in critical_path {
        for node in clone_dag.node_indices() {
            if clone_dag[node].id == dag[critical_node].id {
                critical_path_nodes.push(node);
            }
        }
    }
    //Critical paths are in reverse order to match original order
    let mut origin_critical_path_nodes = Vec::new();
    for node in dag.node_indices() {
        for critical_node in &critical_path_nodes {
            if dag[node].id == clone_dag[*critical_node].id {
                origin_critical_path_nodes.push(*critical_node);
            }
        }
    }

    let providers = get_providers(clone_dag, origin_critical_path_nodes.clone());
    let mut f_consumers = get_f_consumers(clone_dag, origin_critical_path_nodes.clone());

    //Rule 1. give high priority to critical paths
    for critical_node in origin_critical_path_nodes {
        clone_dag.add_param(critical_node, "priority", *priority);
    }

    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                *priority += 1;
                //Acquisition of the node with the longest earliest execution time in f_consumer
                let mut longest_node = f_consumer
                    .iter()
                    .max_by_key(|f_consumer_node| {
                        clone_dag[**f_consumer_node].params["current_length"]
                    })
                    .cloned()
                    .unwrap();

                let mut longest_path = vec![longest_node];

                //HACK: Acquisition of the longest path
                while let Some(pre_nodes) = clone_dag.get_pre_nodes(longest_node) {
                    //Facilitates exploration
                    let f_consumer_set: HashSet<_> = f_consumer.iter().collect();
                    //To find the longest path in the current f-consumer, terminate if all predecessor nodes are different
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer_set.contains(pre_node))
                    {
                        break;
                    }

                    //Find the reference node in the current f-consumer
                    //Search in reverse order, with Index based on the fastest one.
                    for pre_node in pre_nodes.iter().rev() {
                        if f_consumer_set.contains(pre_node) {
                            longest_node = *pre_node;
                            break;
                        }
                    }
                    let mut longest_current_length =
                        clone_dag[longest_node].params["current_length"];
                    for pre_node in pre_nodes.iter() {
                        if f_consumer.contains(pre_node) {
                            let current_length = clone_dag[*pre_node].params["current_length"];
                            if current_length > longest_current_length {
                                longest_node = *pre_node;
                                longest_current_length = current_length;
                            }
                        }
                    }
                    longest_path.push(longest_node);
                }

                //HACK:Recursion if there are dependencies in the f-consumer.
                for node in longest_path.clone() {
                    if let Some(pre_nodes) = clone_dag.get_pre_nodes(node) {
                        if pre_nodes.len() > 1 {
                            let mut clone_clone_dag = clone_dag.clone();
                            remove_nodes(&mut clone_clone_dag, f_consumer.clone());
                            prioritization_cpc_model_loop(
                                &mut clone_clone_dag,
                                clone_dag,
                                priority,
                                longest_path.clone(),
                            );
                            break;
                        }
                    }
                }

                //Rule 3. give high priority to the nodes in the longest path
                for node in longest_path {
                    clone_dag.add_param(node, "priority", *priority);
                }

                //remove the nodes in the longest path from the f-consumer
                for node in clone_dag.node_indices() {
                    if clone_dag[node].params.contains_key("priority") {
                        if let Some(position) = f_consumer.iter().position(|x| *x == node) {
                            f_consumer.remove(position);
                        }
                    }
                }
            }
        }
    }

    // Granting priority to the clone is also applied to the original
    for node in dag.node_indices() {
        let id = dag[node].id;
        let maybe_clone_node = clone_dag.node_indices().find(|n| clone_dag[*n].id == id);

        if let Some(clone_node) = maybe_clone_node {
            if !clone_dag[clone_node].params.contains_key("priority") {
                continue;
            }
            dag.add_param(node, "priority", clone_dag[clone_node].params["priority"]);
        }
    }
}

#[allow(dead_code)] //TODO: remove
pub fn prioritization_cpc_model(dag: &mut Graph<NodeData, i32>) {
    let mut priority = 0;
    let critical_path = dag.get_critical_path();
    let providers = get_providers(dag, critical_path.clone());
    let mut f_consumers = get_f_consumers(dag, critical_path.clone());

    //Rule 1. give high priority to critical paths
    for critical_node in critical_path {
        dag.add_param(critical_node, "priority", priority);
    }

    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                priority += 1;
                //Acquisition of the node with the longest earliest execution time in f_consumer
                let mut longest_node = f_consumer
                    .iter()
                    .max_by_key(|f_consumer_node| dag[**f_consumer_node].params["current_length"])
                    .cloned()
                    .unwrap();

                let mut longest_path = vec![longest_node];

                //HACK: Acquisition of the longest path
                while let Some(pre_nodes) = dag.get_pre_nodes(longest_node) {
                    //Facilitates exploration
                    let f_consumer_set: HashSet<_> = f_consumer.iter().collect();
                    //To find the longest path in the current f-consumer, terminate if all predecessor nodes are different
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer_set.contains(pre_node))
                    {
                        break;
                    }

                    //Find the reference node in the current f-consumer
                    //Search in reverse order, with Index based on the fastest one.
                    for pre_node in pre_nodes.iter().rev() {
                        if f_consumer_set.contains(pre_node) {
                            longest_node = *pre_node;
                            break;
                        }
                    }
                    let mut longest_current_length = dag[longest_node].params["current_length"];
                    for pre_node in pre_nodes.iter() {
                        if f_consumer.contains(pre_node) {
                            let current_length = dag[*pre_node].params["current_length"];
                            if current_length > longest_current_length {
                                longest_node = *pre_node;
                                longest_current_length = current_length;
                            }
                        }
                    }
                    longest_path.push(longest_node);
                }

                //HACK:Recursion if there are dependencies in the f-consumer.
                for node in longest_path.clone() {
                    if let Some(pre_nodes) = dag.get_pre_nodes(node) {
                        if pre_nodes.len() > 1 {
                            let mut clone_dag = dag.clone();
                            remove_nodes(&mut clone_dag, f_consumer.clone());
                            prioritization_cpc_model_loop(
                                dag,
                                &mut clone_dag,
                                &mut priority,
                                longest_path.clone(),
                            );
                            break;
                        }
                    }
                }

                //Rule 3. give high priority to the nodes in the longest path
                for node in longest_path {
                    dag.add_param(node, "priority", priority);
                }

                //remove the nodes in the longest path from the f-consumer
                for node in dag.node_indices() {
                    if dag[node].params.contains_key("priority") {
                        if let Some(position) = f_consumer.iter().position(|x| *x == node) {
                            f_consumer.remove(position);
                        }
                    }
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

        let n3 = dag.add_node(create_node(3, "execution_time", 3));
        let n4 = dag.add_node(create_node(4, "execution_time", 2));
        let n5 = dag.add_node(create_node(5, "execution_time", 3));
        let n6 = dag.add_node(create_node(6, "execution_time", 1));
        let n7 = dag.add_node(create_node(7, "execution_time", 1));
        let n8 = dag.add_node(create_node(8, "execution_time", 3));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);
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

        let n3 = dag.add_node(create_node(3, "execution_time", 2));
        let n4 = dag.add_node(create_node(4, "execution_time", 2));
        let n5 = dag.add_node(create_node(5, "execution_time", 3));
        let n6 = dag.add_node(create_node(6, "execution_time", 1));
        let n7 = dag.add_node(create_node(7, "execution_time", 1));
        let n8 = dag.add_node(create_node(8, "execution_time", 3));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);
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
    fn test() {
        let mut dag = create_sample_dag();
        let expected_value = vec![0, 0, 0, 0, 0, 1, 2, 5, 3, 4, 8, 6, 7];

        prioritization_cpc_model(&mut dag);

        for node in dag.node_indices() {
            assert_eq!(
                dag[node].params["priority"],
                expected_value[dag[node].id as usize]
            );
        }
    }

    #[test]
    fn test2() {
        let mut dag = create_sample_dag_not_consolidated();
        let expected_value = vec![0, 0, 0, 4, 2, 1, 1, 3, 1];

        prioritization_cpc_model(&mut dag);
        for node in dag.node_indices() {
            assert_eq!(
                dag[node].params["priority"],
                expected_value[dag[node].id as usize]
            );
        }
    }

    #[test]
    fn test3() {
        let mut dag = create_sample_dag_complex();
        let expected_value = vec![0, 0, 0, 4, 2, 1, 1, 3, 1];

        prioritization_cpc_model(&mut dag);
        for node in dag.node_indices() {
            assert_eq!(
                dag[node].params["priority"],
                expected_value[dag[node].id as usize]
            );
        }
    }
}
