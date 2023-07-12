use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::Graph;

pub enum SegmentClassification {
    Heavy,
    Light,
}

pub enum DAGClassification {
    Heavy,
    Light,
    Mixture,
}

pub struct Segment {
    pub nodes: Vec<NodeData>,
    pub begin_range: i32,
    pub end_range: i32,
    pub deadline: f32,
    pub classification: Option<SegmentClassification>,
    pub exe_req: i32, // execution requirement
    pub parallel_degree: i32,
    pub volume: i32,
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
        let end_range = earliest_finish_times[i];

        let segment = Segment {
            nodes: Vec::new(),
            begin_range,
            end_range,
            deadline: 0.0,
            classification: None,
            exe_req: end_range - begin_range,
            parallel_degree: 0,
            volume: 0,
        };

        segments.push(segment);
    }

    for node in dag.node_weights() {
        for segment in &mut segments {
            if node.params["earliest_start_time"] <= segment.begin_range
                && segment.end_range <= node.params["earliest_finish_time"]
            {
                segment.nodes.push(node.clone());
                segment.parallel_degree += 1;
                segment.volume += segment.exe_req;
            }
        }
    }

    segments
}

fn classify_segment(volume: f32, period: f32, crit_path_len: f32, segment: &mut Segment) {
    assert!(!segment.nodes.is_empty());

    segment.classification =
        if segment.parallel_degree as f32 > volume / ((2.0 * period) - crit_path_len) {
            Some(SegmentClassification::Heavy)
        } else {
            Some(SegmentClassification::Light)
        };
}

fn classify_dag(
    volume: f32,
    period: f32,
    crit_path_len: f32,
    segments: &mut [Segment],
) -> DAGClassification {
    for segment in segments.iter_mut() {
        classify_segment(volume, period, crit_path_len, segment);
    }

    let (mut heavy_count, mut light_count) = (0, 0);
    for segment in segments {
        match segment.classification {
            Some(SegmentClassification::Heavy) => heavy_count += 1,
            Some(SegmentClassification::Light) => light_count += 1,
            _ => unreachable!("Segment classification error"),
        }
    }

    match (heavy_count > 0, light_count > 0) {
        (true, true) => DAGClassification::Mixture,
        (true, false) => DAGClassification::Heavy,
        (false, true) => DAGClassification::Light,
        _ => unreachable!("Segments classification error"),
    }
}

#[allow(dead_code)] //TODO: remove
pub fn calculate_segments_deadline(dag: &mut Graph<NodeData, i32>, segments: &mut [Segment]) {
    let volume = dag.get_volume() as f32;
    let period = dag.get_head_period().unwrap() as f32;
    let crit_path = dag.get_critical_path();
    let crit_path_len = dag.get_total_wcet_from_nodes(&crit_path) as f32;

    let classification = classify_dag(volume, period, crit_path_len, segments);
    match classification {
        DAGClassification::Heavy => {
            for segment in segments {
                segment.deadline = (period / volume) * segment.volume as f32;
            }
        }
        DAGClassification::Light => {
            for segment in segments {
                segment.deadline = (period / crit_path_len) * segment.exe_req as f32;
            }
        }
        DAGClassification::Mixture => {
            let (mut heavy_segment_volume, mut light_segment_length) = (0, 0);
            for segment in segments.iter_mut() {
                match segment.classification {
                    Some(SegmentClassification::Heavy) => {
                        heavy_segment_volume += segment.volume;
                    }
                    Some(SegmentClassification::Light) => {
                        light_segment_length += segment.exe_req;
                    }
                    _ => unreachable!("Segment classification error"),
                }
            }

            let light_segment_period = crit_path_len / 2.0;
            for segment in segments {
                match segment.classification {
                    Some(SegmentClassification::Heavy) => {
                        segment.deadline = (period - light_segment_period)
                            / heavy_segment_volume as f32
                            * segment.volume as f32;
                    }
                    Some(SegmentClassification::Light) => {
                        segment.deadline = light_segment_period / light_segment_length as f32
                            * segment.exe_req as f32;
                    }
                    _ => unreachable!("Segment classification error"),
                }
            }
        }
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

    #[test]
    fn test_calculate_segments_deadline_normal_heavy() {
        let mut dag = create_sample_dag(150);
        let mut segments = create_segments(&mut dag);
        calculate_segments_deadline(&mut dag, &mut segments);

        assert_eq!(segments[0].deadline, 3.8461537);
        assert_eq!(segments[1].deadline, 13.461538);
        assert_eq!(segments[2].deadline, 69.23077);
        assert_eq!(segments[3].deadline, 11.538462);
        assert_eq!(segments[4].deadline, 51.923077);
    }

    #[test]
    fn test_calculate_segments_deadline_normal_light() {
        let mut dag = create_sample_dag(65);
        let mut segments = create_segments(&mut dag);
        calculate_segments_deadline(&mut dag, &mut segments);

        assert_eq!(segments[0].deadline, 2.300885);
        assert_eq!(segments[1].deadline, 4.026549);
        assert_eq!(segments[2].deadline, 20.707964);
        assert_eq!(segments[3].deadline, 6.9026546);
        assert_eq!(segments[4].deadline, 31.061947);
    }

    #[test]
    fn test_calculate_segments_deadline_normal_mixture() {
        let mut dag = create_sample_dag(120);
        let mut segments = create_segments(&mut dag);
        calculate_segments_deadline(&mut dag, &mut segments);

        assert_eq!(segments[0].deadline, 3.2285714);
        assert_eq!(segments[1].deadline, 10.33721);
        assert_eq!(segments[2].deadline, 53.16279);
        assert_eq!(segments[3].deadline, 9.685715);
        assert_eq!(segments[4].deadline, 43.585712);
    }
}
