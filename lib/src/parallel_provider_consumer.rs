use crate::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

pub fn identifying_capacity_providers(mut dag: Graph<NodeData, f32>) -> Vec<Vec<NodeIndex>> {
    let critical_path = dag.get_critical_path();
    let mut providers = Vec::new();
    let mut i = 0;
    while i < critical_path.len() {
        let mut provider = Vec::new();
        provider.push(critical_path[i]);
        if i < critical_path.len() - 1 {
            let mut pre_nodes = dag.get_pre_nodes(critical_path[i + 1]).unwrap();
            while pre_nodes.len() == 1 && pre_nodes[0] == critical_path[i] {
                provider.push(critical_path[i + 1]);
                i += 1;
                pre_nodes = dag.get_pre_nodes(critical_path[i + 1]).unwrap();
            }
        }

        providers.push(provider);
        i += 1;
    }

    providers
}

#[cfg(test)]

mod tests {
    use std::collections::HashMap;

    use super::*;

    fn create_sample_dag() -> Graph<NodeData, f32> {
        fn create_node(id: i32, key: &str, value: f32) -> NodeData {
            let mut params = HashMap::new();
            params.insert(key.to_string(), value);
            NodeData { id, params }
        }
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 1.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 1.0));
        let n1_0 = dag.add_node(create_node(2, "execution_time", 0.0));
        let n1_1 = dag.add_node(create_node(3, "execution_time", 0.0));

        let n2 = dag.add_node(create_node(4, "execution_time", 1.0));
        let n2_0 = dag.add_node(create_node(5, "execution_time", 0.0));
        let n2_1 = dag.add_node(create_node(6, "execution_time", 0.0));
        let n2_2 = dag.add_node(create_node(7, "execution_time", 0.0));

        let n3 = dag.add_node(create_node(8, "execution_time", 1.0));
        let n3_0 = dag.add_node(create_node(9, "execution_time", 0.0));
        let n3_1 = dag.add_node(create_node(10, "execution_time", 0.0));
        let n3_2 = dag.add_node(create_node(11, "execution_time", 0.0));
        let n4 = dag.add_node(create_node(12, "execution_time", 1.0));

        dag.add_edge(n0, n1, 0.0);
        dag.add_edge(n1, n2, 0.0);
        dag.add_edge(n2, n3, 0.0);
        dag.add_edge(n3, n4, 0.0);

        dag.add_edge(n0, n1_0, 0.0);
        dag.add_edge(n0, n1_1, 0.0);
        dag.add_edge(n0, n2_0, 0.0);

        dag.add_edge(n1_0, n2, 0.0);
        dag.add_edge(n1_1, n2, 0.0);

        dag.add_edge(n1, n2_1, 0.0);
        dag.add_edge(n1, n2_2, 0.0);

        dag.add_edge(n1_0, n2_1, 0.0);
        dag.add_edge(n1_1, n2_2, 0.0);

        dag.add_edge(n2_0, n3, 0.0);
        dag.add_edge(n2_1, n3, 0.0);
        dag.add_edge(n2_2, n3, 0.0);

        dag.add_edge(n2_0, n3_0, 0.0);
        dag.add_edge(n2_1, n3_1, 0.0);
        dag.add_edge(n2_2, n3_2, 0.0);

        dag.add_edge(n3_0, n4, 0.0);
        dag.add_edge(n3_1, n4, 0.0);
        dag.add_edge(n3_2, n4, 0.0);

        dag
    }

    #[test]
    fn identifying_capacity_providers_normal() {
        let dag = create_sample_dag();
        let providers = identifying_capacity_providers(dag);
        println!("{:?}", providers);
        assert_eq!(providers.len(), 4);
    }
}
