use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};

use crate::parallel_provider_consumer::*;

fn p(dag: &mut Graph<NodeData, f32>) {
    let critical_path = dag.get_critical_path();
    let providers = get_providers(dag, critical_path.clone());
    let f_consumers = get_f_consumers(dag, critical_path.clone());

    for critical_node in critical_path {
        dag.add_params(critical_node, "priority", 0.0);
    }
}
