use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::Topo;
use std::vec;

use crate::handle_segment::*;

#[allow(dead_code)]
pub fn decompose(dag: &mut Graph<NodeData, i32>) {
    let mut segments = create_segments(dag);
    calculate_segments_deadline(dag, &mut segments);

    // Add deadlines to nodes.
    let mut nodes_deadline = vec![0.0; dag.node_count()];
    for segment in segments.iter() {
        segment.nodes.iter().for_each(|node| {
            nodes_deadline[node.id as usize] += segment.deadline;
        });
    }
    // Convert to rational numbers to handle floating point numbers.
    for (i, node_deadline) in nodes_deadline.iter().enumerate() {
        let rounded_node_deadline: f32 = format!("{:.5}", node_deadline).parse().unwrap();
        let integer_part_str = rounded_node_deadline.trunc().abs().to_string();
        let fractional_part_str = rounded_node_deadline
            .abs()
            .to_string()
            .chars()
            .skip(integer_part_str.len() + 1)
            .collect::<String>();
        let deadline_factor = 10u64.pow(fractional_part_str.len().try_into().unwrap()) as i32;
        let node_i = NodeIndex::new(i);
        dag.add_param(node_i, "deadline_factor", deadline_factor);
        dag.add_param(
            node_i,
            "integer_scaled_deadline",
            (rounded_node_deadline * deadline_factor as f32) as i32,
        );
    }

    // Sort because offsets need to be calculated in the order of execution.
    let mut topo = Topo::new(&*dag);
    let mut sorted_nodes: Vec<_> = Vec::new();
    while let Some(node_i) = topo.next(&*dag) {
        sorted_nodes.push(node_i);
    }

    // Add offset to nodes.
    for node_i in sorted_nodes {
        let pre_nodes = dag.get_pre_nodes(node_i);
        match pre_nodes {
            None => dag.add_param(node_i, "integer_scaled_offset", 0),
            // offset = maximum of offset + deadline of predecessor node
            Some(nodes) => {
                let mut max_offset = 0;
                for pre_node_i in nodes {
                    let pre_node = dag.node_weight(pre_node_i).unwrap();
                    let pre_node_offset = pre_node.params.get("integer_scaled_offset").unwrap();
                    let pre_node_deadline = pre_node.params.get("integer_scaled_deadline").unwrap();
                    let offset = pre_node_offset + pre_node_deadline;

                    if max_offset < offset {
                        max_offset = offset;
                    }
                }
                dag.add_param(node_i, "integer_scaled_offset", max_offset);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use lib::graph_extension::GraphExtension;

    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
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

        let expect_deadline = [322857, 1033721, 7318570, 5316279, 4358571];
        let expect_offset = [0, 322857, 322857, 1356578, 7641427];
        for node_i in dag.node_indices() {
            assert_eq!(
                dag[node_i].params["integer_scaled_deadline"],
                expect_deadline[node_i.index()]
            );
            assert_eq!(dag[node_i].params["deadline_factor"], 100000);
            assert_eq!(
                dag[node_i].params["integer_scaled_offset"],
                expect_offset[node_i.index()]
            );
        }
    }
}
