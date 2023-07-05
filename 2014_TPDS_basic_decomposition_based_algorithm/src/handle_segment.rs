use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::Graph;

#[allow(dead_code)] //TODO: remove
pub enum SegmentClassification {
    Heavy,
    Light,
}

pub struct Segment {
    pub nodes: Vec<NodeData>,
    pub begin_range: i32,
    pub end_range: i32,
    pub deadline: f32,                         //TODO: use
    pub classification: SegmentClassification, //TODO: use
}

#[allow(dead_code)] //TODO: remove
pub fn create_segments(dag: &mut Graph<NodeData, i32>) -> Vec<Segment> {
    dag.calculate_earliest_finish_times();

    let mut node_indices: Vec<_> = dag.node_indices().collect();
    node_indices
        .sort_by_key(|&node_i| dag.node_weight(node_i).unwrap().params["earliest_finish_time"]);

    let mut segments: Vec<Segment> = Vec::with_capacity(node_indices.len());

    for i in 0..node_indices.len() {
        let begin_range = if i == 0 {
            0
        } else {
            dag.node_weight(node_indices[i - 1]).unwrap().params["earliest_finish_time"]
        };

        let segment = Segment {
            nodes: Vec::new(),
            begin_range,
            end_range: dag.node_weight(node_indices[i]).unwrap().params["earliest_finish_time"],
            deadline: 0.0,
            classification: SegmentClassification::Heavy,
        };

        segments.push(segment);
    }

    for &node_i in &node_indices {
        let node = dag.node_weight(node_i).unwrap();

        for segment in &mut segments {
            if node.params["earliest_start_time"] <= segment.begin_range
                && segment.end_range <= node.params["earliest_finish_time"]
            {
                segment.nodes.push(node.clone());
            }
        }
    }

    segments
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

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
    fn test_create_segment_normal() {
        let mut dag = create_sample_dag(120);
        let segments = create_segments(&mut dag);

        assert_eq!(segments.len(), 5);

        assert_eq!(segments[0].nodes.len(), 1);
        assert_eq!(segments[1].nodes.len(), 2);
        assert_eq!(segments[2].nodes.len(), 2);
        assert_eq!(segments[3].nodes.len(), 1);
        assert_eq!(segments[4].nodes.len(), 1);

        assert_eq!(segments[0].begin_range, 0);
        assert_eq!(segments[0].end_range, 4);
        assert_eq!(segments[1].begin_range, 4);
        assert_eq!(segments[1].end_range, 11);
        assert_eq!(segments[2].begin_range, 11);
        assert_eq!(segments[2].end_range, 47);
        assert_eq!(segments[3].begin_range, 47);
        assert_eq!(segments[3].end_range, 59);
        assert_eq!(segments[4].begin_range, 59);
        assert_eq!(segments[4].end_range, 113);
    }
}
