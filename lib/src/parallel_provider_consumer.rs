use crate::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

pub fn identifying_capacity_providers(mut dag: Graph<NodeData, f32>) -> Vec<Vec<NodeIndex>> {
    let critical_path = dag.get_critical_path();
    let mut providers = Vec::new();
    for critical_node in critical_path {
        let mut provider = Vec::new();
        provider.push(critical_node);
        if let Some(suc_nodes) = dag.get_suc_nodes(critical_node) {
            for suc_node in suc_nodes {
                if let Some(pre_nodes) = dag.get_pre_nodes(suc_node) {
                    if pre_nodes.len() == 1 && pre_nodes[0] == critical_node {
                        provider.push(suc_node);
                    }
                }
            }
        }
        providers.push(provider);
    }
    providers
}
