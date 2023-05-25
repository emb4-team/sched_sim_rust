use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

use crate::parallel_provider_consumer::*;

#[allow(dead_code)]
//dagから指定したノードの配列に含まれるノード以外を削除する
fn remove_nodes(dag: &mut Graph<NodeData, f32>, nodes: Vec<NodeIndex>) {
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

#[allow(dead_code)]
pub fn p_loop(
    origin_dag: &mut Graph<NodeData, f32>,
    dag: &mut Graph<NodeData, f32>,
    priority: &mut f32,
) {
    let critical_path = dag.get_critical_path();
    let providers = get_providers(dag, critical_path.clone());
    let mut f_consumers = get_f_consumers(dag, critical_path.clone());

    for critical_node in critical_path {
        dag.add_params(critical_node, "priority", *priority);
    }

    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                *priority += 1.0;
                let mut longest_path = Vec::new();
                let mut longest_current_length = 0.0;
                let mut longest_node_index = 0;
                for (index, f_consumer_node) in f_consumer.iter().enumerate() {
                    let current_length = dag[*f_consumer_node].params["current_length"];
                    if current_length > longest_current_length {
                        longest_current_length = current_length;
                        longest_node_index = index;
                    }
                }
                let mut longest_node = f_consumer[longest_node_index];
                longest_path.push(longest_node);

                while let Some(pre_nodes) = dag.get_pre_nodes(longest_node) {
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer.contains(pre_node))
                    {
                        break;
                    }
                    longest_node = pre_nodes[0];
                    let mut longest_current_length = dag[longest_node].params["current_length"];
                    for pre_node in pre_nodes {
                        if f_consumer.contains(&pre_node) {
                            let current_length = dag[pre_node].params["current_length"];
                            if current_length > longest_current_length {
                                longest_node = pre_node;
                                longest_current_length = current_length;
                            }
                        }
                    }
                    longest_path.push(longest_node);
                }
                for node in longest_path.clone() {
                    if let Some(pre_nodes) = dag.get_pre_nodes(node) {
                        if pre_nodes.len() > 1 {
                            println!("node: {:?}", node);
                            let mut clone_dag = dag.clone();
                            remove_nodes(&mut clone_dag, f_consumer.clone());

                            p(&mut clone_dag);
                        }
                    }
                }

                for node in longest_path {
                    if !dag[node].params.contains_key("priority") {
                        dag.add_params(node, "priority", *priority);
                    }
                }

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

    for node in origin_dag.node_indices() {
        let id = origin_dag[node].id;
        let maybe_clone_node = dag.node_indices().find(|n| dag[*n].id == id);

        if let Some(clone_node) = maybe_clone_node {
            if dag[clone_node].params.contains_key("priority") {
                origin_dag.add_params(node, "priority", dag[clone_node].params["priority"]);
            }
        }
    }
}

#[allow(dead_code)]
pub fn p(dag: &mut Graph<NodeData, f32>) {
    let mut priority = 0.0;
    let critical_path = dag.get_critical_path();
    let providers = get_providers(dag, critical_path.clone());
    let mut f_consumers = get_f_consumers(dag, critical_path.clone());

    for critical_node in critical_path {
        dag.add_params(critical_node, "priority", priority);
    }

    for provider in providers {
        if let Some(f_consumer) = f_consumers.get_mut(&provider) {
            while !f_consumer.is_empty() {
                priority += 1.0;
                let mut longest_path = Vec::new();
                let mut longest_current_length = 0.0;
                let mut longest_node_index = 0;
                for (index, f_consumer_node) in f_consumer.iter().enumerate() {
                    let current_length = dag[*f_consumer_node].params["current_length"];
                    if current_length > longest_current_length {
                        longest_current_length = current_length;
                        longest_node_index = index;
                    }
                }
                let mut longest_node = f_consumer[longest_node_index];
                longest_path.push(longest_node);

                println!("longest_node: {:?}", longest_node);
                println!("priority: {:?}", priority);

                while let Some(pre_nodes) = dag.get_pre_nodes(longest_node) {
                    if pre_nodes
                        .iter()
                        .all(|pre_node| !f_consumer.contains(pre_node))
                    {
                        break;
                    }
                    longest_node = pre_nodes[0];
                    let mut longest_current_length = dag[longest_node].params["current_length"];
                    for pre_node in pre_nodes {
                        if f_consumer.contains(&pre_node) {
                            let current_length = dag[pre_node].params["current_length"];
                            if current_length > longest_current_length {
                                longest_node = pre_node;
                                longest_current_length = current_length;
                            }
                        }
                    }
                    longest_path.push(longest_node);
                }
                for node in longest_path.clone() {
                    if let Some(pre_nodes) = dag.get_pre_nodes(node) {
                        if pre_nodes.len() > 1 {
                            let mut clone_dag = dag.clone();
                            remove_nodes(&mut clone_dag, f_consumer.clone());

                            p_loop(dag, &mut clone_dag, &mut priority);
                            break;
                        }
                    }
                }

                //dagのnodeがpriorityを持っていない場合，priorityを追加する．

                for node in longest_path {
                    if !dag[node].params.contains_key("priority") {
                        dag.add_params(node, "priority", priority);
                    }
                }

                for node in dag.node_indices() {
                    if dag[node].params.contains_key("priority") {
                        if let Some(position) = f_consumer.iter().position(|x| *x == node) {
                            f_consumer.remove(position);
                        }
                    }
                }
                println!("priority: {:?}", priority);
            }
        }
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag_not_consolidated() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10.0));
        let c1 = dag.add_node(create_node(1, "execution_time", 10.0));
        let c2 = dag.add_node(create_node(2, "execution_time", 10.0));

        let n3 = dag.add_node(create_node(3, "execution_time", 2.0));
        let n4 = dag.add_node(create_node(4, "execution_time", 2.0));
        let n5 = dag.add_node(create_node(5, "execution_time", 3.0));
        let n6 = dag.add_node(create_node(6, "execution_time", 1.0));
        let n7 = dag.add_node(create_node(7, "execution_time", 1.0));
        let n8 = dag.add_node(create_node(8, "execution_time", 3.0));

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);
        dag.add_edge(c1, c2, 1.0);
        dag.add_edge(c0, n3, 1.0);
        dag.add_edge(n3, c2, 1.0);
        dag.add_edge(c0, n4, 1.0);
        dag.add_edge(n4, n6, 1.0);
        dag.add_edge(n4, n7, 1.0);
        dag.add_edge(c0, n5, 1.0);
        dag.add_edge(n5, n7, 1.0);
        dag.add_edge(n6, n8, 1.0);
        dag.add_edge(n7, n8, 1.0);
        dag.add_edge(n8, c2, 1.0);

        dag
    }

    fn create_sample_dag_complex() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10.0));
        let c1 = dag.add_node(create_node(1, "execution_time", 10.0));
        let c2 = dag.add_node(create_node(2, "execution_time", 10.0));

        let n3 = dag.add_node(create_node(3, "execution_time", 2.0));
        let n4 = dag.add_node(create_node(4, "execution_time", 2.0));
        let n5 = dag.add_node(create_node(5, "execution_time", 3.0));
        let n6 = dag.add_node(create_node(6, "execution_time", 1.0));
        let n7 = dag.add_node(create_node(7, "execution_time", 1.0));
        let n8 = dag.add_node(create_node(8, "execution_time", 3.0));

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);
        dag.add_edge(c1, c2, 1.0);
        dag.add_edge(c0, n3, 1.0);
        dag.add_edge(n3, n6, 1.0);
        dag.add_edge(c0, n4, 1.0);
        dag.add_edge(n4, n6, 1.0);
        dag.add_edge(c0, n5, 1.0);
        dag.add_edge(n5, n7, 1.0);
        dag.add_edge(n6, n8, 1.0);
        dag.add_edge(n7, n8, 1.0);
        dag.add_edge(n8, c2, 1.0);

        dag
    }

    ///DAG in Figure 2 (b) of the paper
    fn create_sample_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let c1 = dag.add_node(create_node(1, "execution_time", 4.0));
        let c2 = dag.add_node(create_node(2, "execution_time", 4.0));
        let c3 = dag.add_node(create_node(3, "execution_time", 4.0));
        let c4 = dag.add_node(create_node(4, "execution_time", 4.0));
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(5, "execution_time", 2.0));
        let n1_2 = dag.add_node(create_node(6, "execution_time", 1.0));
        let n0_3 = dag.add_node(create_node(7, "execution_time", 3.0));
        let n1_3 = dag.add_node(create_node(8, "execution_time", 2.0));
        let n2_3 = dag.add_node(create_node(9, "execution_time", 1.0));
        let n0_4 = dag.add_node(create_node(10, "execution_time", 3.0));
        let n1_4 = dag.add_node(create_node(11, "execution_time", 2.0));
        let n2_4 = dag.add_node(create_node(12, "execution_time", 1.0));

        //create critical path edges
        dag.add_edge(c0, c1, 1.0);
        dag.add_edge(c1, c2, 1.0);
        dag.add_edge(c2, c3, 1.0);
        dag.add_edge(c3, c4, 1.0);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1.0);
        dag.add_edge(n0_2, c2, 1.0);
        dag.add_edge(c0, n1_2, 1.0);
        dag.add_edge(n1_2, c2, 1.0);
        dag.add_edge(c0, n0_3, 1.0);
        dag.add_edge(n0_3, c3, 1.0);
        dag.add_edge(c1, n1_3, 1.0);
        dag.add_edge(n1_3, c3, 1.0);
        dag.add_edge(c1, n2_3, 1.0);
        dag.add_edge(n2_3, c3, 1.0);
        dag.add_edge(n0_3, n0_4, 1.0);
        dag.add_edge(n0_4, c4, 1.0);
        dag.add_edge(n1_3, n1_4, 1.0);
        dag.add_edge(n1_4, c4, 1.0);
        dag.add_edge(n2_3, n2_4, 1.0);
        dag.add_edge(n2_4, c4, 1.0);

        dag
    }

    #[test]
    fn test2() {
        let mut dag = create_sample_dag_not_consolidated();
        p(&mut dag);
        println!();
        for node in dag.node_indices() {
            println!(
                "node: id {}  priority {:?}",
                dag[node].id, dag[node].params["priority"]
            );
        }
    }

    #[test]
    fn test3() {
        let mut dag = create_sample_dag_complex();
        p(&mut dag);
        println!();
        for node in dag.node_indices() {
            println!(
                "node: id {}  priority {:?}",
                dag[node].id, dag[node].params["priority"]
            );
        }
    }

    #[test]
    fn test1() {
        let mut dag = create_sample_dag_not_consolidated();
        let critical_path = dag.get_critical_path();
        println!("critical path: {:?}", critical_path);
        let providers = get_providers(&dag, critical_path.clone());
        println!("providers: {:?}", providers);
        let f_consumers = get_f_consumers(&mut dag, critical_path.clone());
        println!("f_consumers: {:?}", f_consumers);
        for node in critical_path {
            dag.add_params(node, "priority", 0.0);
        }
        for node in dag.node_indices() {
            println!(
                "node: id {}  params {:?} index {}",
                dag[node].id,
                dag[node].params,
                node.index()
            );
        }
        let mut clone_dag = dag.clone();
        remove_nodes(&mut clone_dag, f_consumers[&providers[0]].clone());
        for node in clone_dag.node_indices() {
            clone_dag.add_params(node, "priority", 1.0);
        }
        //clone_dagがpriorityを持つようになったので、dagに反映させる
        //ただし，NodeIndexが変わっているので，idでマッピングする
        for node in dag.node_indices() {
            let id = dag[node].id;
            let maybe_clone_node = clone_dag.node_indices().find(|n| clone_dag[*n].id == id);

            if let Some(clone_node) = maybe_clone_node {
                dag.add_params(node, "priority", clone_dag[clone_node].params["priority"]);
            }
        }
        println!();
        for node in dag.node_indices() {
            println!(
                "node: id {}  params {:?} index {}",
                dag[node].id,
                dag[node].params,
                node.index()
            );
        }
    }

    #[test]
    fn test() {
        let mut dag = create_sample_dag();
        p(&mut dag);

        for node in dag.node_indices() {
            println!(
                "node: {}, priority: {}",
                dag[node].id, dag[node].params["priority"]
            );
            println!(
                "node_current_length: {}",
                dag[node].params["current_length"]
            );
        }
    }
}
