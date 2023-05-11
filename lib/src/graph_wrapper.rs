use crate::graph_extension::NodeData;
use petgraph::graph::{Graph, NodeIndex};

pub trait GraphWrapper {
    fn add_node_check_duplicated_id(&mut self, node: NodeData) -> NodeIndex;
}

impl GraphWrapper for Graph<NodeData, f32> {
    fn add_node_check_duplicated_id(&mut self, node: NodeData) -> NodeIndex {
        let node_index = self.add_node(node);
        let node_id = self[node_index].id;

        for existing_node_index in self.node_indices() {
            if existing_node_index != node_index {
                let existing_node_id = self[existing_node_index].id;
                assert_ne!(
                    existing_node_id, node_id,
                    "Node id is duplicated. id: {}",
                    node_id
                );
            }
        }

        node_index
    }
}

mod tests {
    use super::*;
    use std::collections::HashMap;

    #[allow(dead_code)]
    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_add_node_normal() {
        let mut dag = Graph::<NodeData, f32>::new();

        let n0 = dag.add_node_check_duplicated_id(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node_check_duplicated_id(create_node(1, "execution_time", 3.0));

        assert_eq!(dag[n0].id, 0);
        assert_eq!(dag[n1].id, 1);
    }

    #[test]
    #[should_panic]
    fn test_add_node_duplicated_id() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node_check_duplicated_id(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node_check_duplicated_id(create_node(0, "execution_time", 3.0));

        assert_eq!(dag[n0].id, 1);
        assert_eq!(dag[n1].id, 1);
    }
}
