use crate::handle_segment::{calculate_segments_deadline, create_segments};
use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::{graph::Graph, visit::Topo};
use std::vec;

#[allow(dead_code)]
pub fn decompose(dag: &mut Graph<NodeData, i32>) {
    let mut segments = create_segments(dag);
    calculate_segments_deadline(dag, &mut segments);

    // `deadline_factor` is used to scale the deadline of a node to an integer type.
    // The fifth decimal place is truncated.
    let deadline_factor = 100000.0;
    let mut int_scaled_deadline = vec![0; dag.node_count()];
    for segment in segments.iter() {
        segment.nodes.iter().for_each(|node| {
            int_scaled_deadline[node.id as usize] += (segment.deadline * deadline_factor) as i32;
        });
    }
    let int_scaled_offset = calc_int_scaled_offsets(dag, &int_scaled_deadline);

    // Set integer scaled node relative deadline.
    for node_i in dag.node_indices() {
        dag.add_param(
            node_i,
            "int_scaled_node_relative_deadline",
            int_scaled_deadline[node_i.index()] + int_scaled_offset[node_i.index()],
        );
    }
}

fn calc_int_scaled_offsets(dag: &Graph<NodeData, i32>, deadlines: &[i32]) -> Vec<i32> {
    let mut int_scaled_offsets = vec![0; dag.node_count()];

    // Sort because offsets need to be calculated in the order of execution.
    let mut topo_order = Topo::new(dag);
    while let Some(node_i) = topo_order.next(dag) {
        if let Some(pre_nodes) = dag.get_pre_nodes(node_i) {
            // offset = maximum of offset + deadline of predecessor nodes.
            let max_offset = pre_nodes
                .iter()
                .map(|pre_node_i| {
                    let pre_idx = pre_node_i.index();
                    int_scaled_offsets[pre_idx] + deadlines[pre_idx]
                })
                .max()
                .unwrap_or(0);
            int_scaled_offsets[node_i.index()] = max_offset;
        }
    }

    int_scaled_offsets
}

#[cfg(test)]
mod tests {
    use lib::graph_extension::GraphExtension;

    use super::*;
    use std::collections::BTreeMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }
    fn create_sample_dag(period: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_param(n0, "period", period);
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        dag
    }

    #[test]
    fn test_decompose_normal_float() {
        let mut dag = create_sample_dag(120);
        decompose(&mut dag);

        let expect_relative_deadline = [322857, 1356578, 7641428, 6672857, 11999999];
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["int_scaled_node_relative_deadline"],
                expect_relative_deadline[node_i.index()]
            );
        }
    }
}
