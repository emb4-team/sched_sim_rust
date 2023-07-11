use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::Graph;

pub enum SegmentClassification {
    Heavy,
    Light,
}

#[derive(PartialEq, Debug)]
pub enum SegmentsClassification {
    Heavy,
    Light,
    Mixture,
}

pub struct Segment {
    pub nodes: Vec<NodeData>,
    pub begin_range: i32,
    pub end_range: i32,
    pub deadline: f32, //TODO: use
    pub classification: Option<SegmentClassification>,
    pub parallel_degree: i32,
}

#[allow(dead_code)] //TODO: remove
pub fn create_segments(dag: &mut Graph<NodeData, i32>) -> Vec<Segment> {
    dag.calculate_earliest_finish_times();

    let mut earliest_finish_times = Vec::new();
    for node in dag.node_weights_mut() {
        earliest_finish_times.push(node.params["earliest_finish_time"]);
    }

    earliest_finish_times.dedup();
    earliest_finish_times.sort();

    let mut segments: Vec<Segment> = Vec::with_capacity(earliest_finish_times.len());
    for i in 0..earliest_finish_times.len() {
        let begin_range = if i == 0 {
            0
        } else {
            earliest_finish_times[i - 1]
        };
        let segment = Segment {
            nodes: Vec::new(),
            begin_range,
            end_range: earliest_finish_times[i],
            deadline: 0.0,
            classification: None,
            parallel_degree: 0,
        };
        segments.push(segment);
    }

    for node in dag.node_weights() {
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

#[allow(dead_code)] //TODO: remove
fn classify_segment(volume: f32, period: f32, crit_path_len: f32, segment: &mut Segment) {
    if segment.nodes.is_empty() {
        unreachable!("Segment is empty")
    }

    segment.classification =
        if segment.parallel_degree as f32 > volume / ((2.0 * period) - crit_path_len) {
            Some(SegmentClassification::Heavy)
        } else {
            Some(SegmentClassification::Light)
        };
}

#[allow(dead_code)] //TODO: remove
fn classify_segments(
    volume: f32,
    period: f32,
    crit_path_len: f32,
    segments: &mut [Segment],
) -> SegmentsClassification {
    for segment in segments.iter_mut() {
        classify_segment(volume, period, crit_path_len, segment);
    }

    let (mut heavy_count, mut light_count) = (0, 0);

    for segment in segments {
        match segment.classification {
            Some(SegmentClassification::Heavy) => heavy_count += 1,
            Some(SegmentClassification::Light) => light_count += 1,
            None => unreachable!("Segment classification error"),
        }
    }

    match (heavy_count > 0, light_count > 0) {
        (true, true) => SegmentsClassification::Mixture,
        (true, false) => SegmentsClassification::Heavy,
        (false, true) => SegmentsClassification::Light,
        _ => unreachable!("Segments classification error"),
    }
}

#[cfg(test)]
mod tests {
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

    fn create_duplicates_dag(period: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 7));
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

    #[test]
    fn test_create_segment_duplicates() {
        let mut dag = create_duplicates_dag(120);
        let segments = create_segments(&mut dag);

        assert_eq!(segments.len(), 4);

        assert_eq!(segments[0].nodes.len(), 1);
        assert_eq!(segments[1].nodes.len(), 2);
        assert_eq!(segments[2].nodes.len(), 2);
        assert_eq!(segments[3].nodes.len(), 1);

        assert_eq!(segments[0].begin_range, 0);
        assert_eq!(segments[0].end_range, 4);
        assert_eq!(segments[1].begin_range, 4);
        assert_eq!(segments[1].end_range, 11);
        assert_eq!(segments[2].begin_range, 11);
        assert_eq!(segments[2].end_range, 47);
        assert_eq!(segments[3].begin_range, 47);
        assert_eq!(segments[3].end_range, 65);
    }
}
