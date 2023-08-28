use crate::handle_segment::*;
use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::{graph::Graph, visit::Topo};
use std::vec;

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

    // Set deadline factor.
    let deadline_factor = 10u64.pow(5) as i32;
    dag.set_dag_param("deadline_factor", deadline_factor);

    // Set integer absolute deadline.
    let integer_scaled_deadline = nodes_deadline
        .iter()
        .map(|&node_deadline| (node_deadline * deadline_factor as f32) as i32)
        .collect::<Vec<_>>();
    let integer_scaled_offset = compute_offsets(dag, &integer_scaled_deadline);
    for (node_i, (&deadline, &offset)) in dag
        .node_indices()
        .zip(integer_scaled_deadline.iter().zip(&integer_scaled_offset))
    {
        dag.add_param(node_i, "node_integer_absolute_deadline", deadline + offset);
    }
}

fn compute_offsets(dag: &Graph<NodeData, i32>, deadlines: &[i32]) -> Vec<i32> {
    let mut offsets = vec![0; deadlines.len()];

    // Sort because offsets need to be calculated in the order of execution.
    let mut topo_order = Topo::new(dag);
    while let Some(node_i) = topo_order.next(dag) {
        let idx = node_i.index();
        if let Some(pre_nodes) = dag.get_pre_nodes(node_i) {
            // offset = maximum of offset + deadline of predecessor nodes.
            let max_offset = pre_nodes
                .iter()
                .map(|pre_node_i| {
                    let pre_idx = pre_node_i.index();
                    offsets[pre_idx] + deadlines[pre_idx]
                })
                .max()
                .unwrap_or(0);
            offsets[idx] = max_offset;
        }
    }

    offsets
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

        let expect_absolute_deadline = [322857, 1356578, 7641428, 6672857, 11999999];
        for node_i in dag.node_indices() {
            assert_eq!(dag[node_i].params["deadline_factor"], 100000);
            assert_eq!(
                dag[node_i].params["node_integer_absolute_deadline"],
                expect_absolute_deadline[node_i.index()]
            );
        }
    }
}
