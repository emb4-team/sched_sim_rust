use crate::dag_creator::{self, *};
//use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
//use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};
use std::collections::HashMap;

fn _create_dummy_node(id: i32) -> NodeData {
    let mut params = HashMap::new();
    params.insert(String::from("execution_time"), 0.0);

    NodeData { id, params }
}

fn _connect_nodes(
    dag: &Graph<NodeData, f32>,
    new_dag: &mut Graph<NodeData, f32>,
    dummy_node_index: NodeIndex,
    direction: petgraph::Direction,
) {
    let nodes = dag.externals(direction).collect::<Vec<_>>();

    for node_index in nodes {
        match direction {
            Incoming => new_dag.add_edge(dummy_node_index, node_index, 0.0),
            Outgoing => new_dag.add_edge(node_index, dummy_node_index, 0.0),
        };
    }
}

fn _add_dummy_nodes(dag: &Graph<NodeData, f32>) -> Graph<NodeData, f32> {
    let mut new_dag: Graph<dag_creator::NodeData, f32> = (*dag).clone();

    let start_node_index = new_dag.add_node(_create_dummy_node(-1));
    let end_node_index = new_dag.add_node(_create_dummy_node(-2));

    _connect_nodes(
        dag,
        &mut new_dag,
        start_node_index,
        petgraph::Direction::Incoming,
    );
    _connect_nodes(
        dag,
        &mut new_dag,
        end_node_index,
        petgraph::Direction::Outgoing,
    );
    new_dag
}
