use std::collections::HashSet;

use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

use crate::parallel_provider_consumer::*;

fn get_longest_node(dag: &Graph<NodeData, i32>, f_consumer: &[NodeIndex]) -> NodeIndex {
    let longest_node = f_consumer
        .iter()
        .max_by_key(|&f_consumer_node| dag[*f_consumer_node].params["current_length"])
        .cloned()
        .unwrap();
    longest_node
}

fn find_pre_longest_node(
    dag: &Graph<NodeData, i32>,
    f_consumer: &[NodeIndex],
    pre_nodes: Vec<NodeIndex>,
) -> NodeIndex {
    let mut longest_node = None;
    let mut longest_current_length = 0;
    for pre_node in pre_nodes.iter().rev() {
        if f_consumer.contains(pre_node) {
            let current_length = dag[*pre_node].params["current_length"];
            if current_length > longest_current_length {
                longest_node = Some(*pre_node);
                longest_current_length = current_length;
            }
        }
    }
    longest_node.unwrap()
}

fn remove_nodes_with_priority(dag: &mut Graph<NodeData, i32>, f_consumer: &mut Vec<NodeIndex>) {
    for node in dag.node_indices() {
        if dag[node].params.contains_key("priority") {
            if let Some(position) = f_consumer.iter().position(|x| *x == node) {
                f_consumer.remove(position);
            }
        }
    }
}

fn assign_priority_to_path(dag: &mut Graph<NodeData, i32>, path: &Vec<NodeIndex>, priority: i32) {
    for node in path {
        dag.add_param(*node, "priority", priority);
    }
}

pub fn prioritization_cpc_model_loop(
    dag: &mut Graph<NodeData, i32>,
    reduction_dag: &mut Graph<NodeData, i32>,
    priority: &mut i32,
    longest_path: Vec<NodeIndex>,
) {
    //Match the indexes of the reduced dag and the original dag because they are different & align their order.
    let reduction_critical_path: Vec<NodeIndex> = longest_path
        .iter()
        .flat_map(|longest_node| {
            reduction_dag
                .node_indices()
                .filter(|node| reduction_dag[*node].id == dag[*longest_node].id)
                .collect::<Vec<_>>()
        })
        .collect();
    let critical_path: Vec<NodeIndex> = dag
        .node_indices()
        .flat_map(|node| {
            reduction_critical_path
                .iter()
                .filter(|reduction_critical_node| {
                    dag[node].id == reduction_dag[**reduction_critical_node].id
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect();
    //Match the indexes of the reduced dag and the original dag because they are different & align their order.

    let providers = get_providers(reduction_dag, critical_path.clone());
    let mut f_consumers = get_f_consumers(reduction_dag, critical_path.clone());

    //Rule 1. give high priority to critical paths
    assign_priority_to_path(reduction_dag, &critical_path, *priority);

    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                *priority += 1;
                //Acquisition of the node with the longest earliest execution time in f_consumer
                let mut longest_node = get_longest_node(reduction_dag, f_consumer);

                let mut longest_path = vec![longest_node];
                //Facilitates exploration
                let f_consumer_set: HashSet<_> = f_consumer.iter().collect();

                while let Some(pre_nodes) = reduction_dag.get_pre_nodes(longest_node) {
                    //To find the longest path in the current f-consumer, terminate if all predecessor nodes are different
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer_set.contains(pre_node))
                    {
                        break;
                    }

                    //Search in reverse order, with Index based on the fastest one.
                    longest_node = find_pre_longest_node(reduction_dag, f_consumer, pre_nodes);
                    longest_path.push(longest_node);
                }

                //HACK:Recursion if there are dependencies in the f-consumer.
                for node in longest_path.clone() {
                    if let Some(mut pre_nodes) = reduction_dag.get_pre_nodes(node) {
                        pre_nodes.retain(|pre_node| !critical_path.contains(pre_node));
                        if pre_nodes.len() > 1 {
                            let mut clone_clone_dag = reduction_dag.clone();
                            clone_clone_dag.reduction_dag(f_consumer.clone());
                            prioritization_cpc_model_loop(
                                dag,
                                reduction_dag,
                                priority,
                                longest_path.clone(),
                            );
                            break;
                        }
                    }
                }

                //Rule 3. give high priority to the nodes in the longest path
                assign_priority_to_path(reduction_dag, &longest_path, *priority);

                //remove the nodes in the longest path from the f-consumer
                remove_nodes_with_priority(reduction_dag, f_consumer);
            }
        }
    }

    // Granting priority to the clone is also applied to the original
    for node in dag.node_indices() {
        let id = dag[node].id;
        let maybe_clone_node = reduction_dag
            .node_indices()
            .find(|n| reduction_dag[*n].id == id);

        if let Some(clone_node) = maybe_clone_node {
            if !reduction_dag[clone_node].params.contains_key("priority") {
                continue;
            }
            dag.add_param(
                node,
                "priority",
                reduction_dag[clone_node].params["priority"],
            );
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
    assign_priority_to_path(dag, &critical_path, priority);

    //Rule 2. Priority is given to consumers for providers located before
    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                priority += 1;
                //--Can be replaced by reduction_dag&get_critical_path--
                //Acquisition of the node with the longest earliest execution time in f_consumer
                //longest_path is the critical path in the current f-consumer
                let mut longest_node = get_longest_node(dag, f_consumer);
                let mut longest_path = vec![longest_node];

                //Facilitates exploration
                let f_consumer_set: HashSet<_> = f_consumer.iter().collect();
                while let Some(pre_nodes) = dag.get_pre_nodes(longest_node) {
                    //Nodes other than f-consumer are not targeted
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer_set.contains(pre_node))
                    {
                        break;
                    }

                    //Search in reverse order, with Index based on the fastest one.
                    longest_node = find_pre_longest_node(dag, f_consumer, pre_nodes);
                    longest_path.push(longest_node);
                }
                //--Can be replaced by reduction_dag&get_critical_path--

                //HACK:Recursion if there are dependencies in the f-consumer.
                for node in longest_path.clone() {
                    if let Some(mut pre_nodes) = dag.get_pre_nodes(node) {
                        pre_nodes.retain(|pre_node| !critical_path.contains(pre_node));

                        if pre_nodes.len() > 1 {
                            let mut f_consumer_dag = dag.clone();
                            f_consumer_dag.reduction_dag(f_consumer.clone());
                            prioritization_cpc_model_loop(
                                dag,
                                &mut f_consumer_dag,
                                &mut priority,
                                longest_path.clone(),
                            );
                            break;
                        }
                    }
                }

                //Rule 3. give high priority to the nodes in the longest path
                assign_priority_to_path(dag, &longest_path, priority);

                //remove the nodes in the longest path from the f-consumer
                remove_nodes_with_priority(dag, f_consumer);
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
    fn test_prioritization_cpc_model_normal() {
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
    fn test_prioritization_cpc_model_normal_dag_not_consolidated() {
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
    fn test_prioritization_cpc_model_normal_recursion() {
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
