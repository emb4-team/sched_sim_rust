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
    pub execution_requirement: i32, // end_range - begin_range
    pub parallel_degree: i32,       // number of nodes in the segment
    pub volume: i32,                // execution_requirement * nodes.len()
}

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
            execution_requirement: end_range - begin_range,
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
                segment.volume += segment.execution_requirement;
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
                segment.deadline = (period / crit_path_len) * segment.execution_requirement as f32;
            }
        }
        DAGClassification::Mixture => {
            let (mut heavy_segment_volume, mut light_segment_length) = (0, 0);
            for segment in segments.iter() {
                match segment.classification {
                    Some(SegmentClassification::Heavy) => {
                        heavy_segment_volume += segment.volume;
                    }
                    Some(SegmentClassification::Light) => {
                        light_segment_length += segment.execution_requirement;
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
                            * segment.execution_requirement as f32;
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
    use lib::tests_helper::create_dag_for_segment;

    fn test_create_segment_helper(
        is_duplicate: bool,
        expected_segments_len: usize,
        expected_node_counts: &[usize],
        expected_begin_ranges: &[i32],
        expected_end_ranges: &[i32],
    ) {
        let mut dag = create_dag_for_segment(120, is_duplicate);
        let segments = create_segments(&mut dag);

        assert_eq!(segments.len(), expected_segments_len);

        for i in 0..segments.len() {
            assert_eq!(
                segments[i].nodes.len(),
                expected_node_counts[i],
                "Failed at segment {}",
                i
            );
            assert_eq!(
                segments[i].begin_range, expected_begin_ranges[i],
                "Failed at segment {}",
                i
            );
            assert_eq!(
                segments[i].end_range, expected_end_ranges[i],
                "Failed at segment {}",
                i
            );
        }
    }

    fn test_calculate_segments_deadline_helper(period: i32, expected_deadlines: &[f32]) {
        let mut dag = create_dag_for_segment(period, false);
        let mut segments = create_segments(&mut dag);
        calculate_segments_deadline(&mut dag, &mut segments);

        for (i, &deadline) in expected_deadlines.iter().enumerate() {
            assert!(
                (segments[i].deadline - deadline).abs() < f32::EPSILON,
                "Failed at segment {}. Expected: {}, but got: {}",
                i,
                deadline,
                segments[i].deadline
            );
        }
    }

    #[test]
    fn test_create_segment_normal() {
        let expected_node_counts = [1, 2, 2, 1, 1];
        let expected_begin_ranges = [0, 4, 11, 47, 59];
        let expected_end_ranges = [4, 11, 47, 59, 113];

        test_create_segment_helper(
            false,
            5,
            &expected_node_counts,
            &expected_begin_ranges,
            &expected_end_ranges,
        );
    }

    #[test]
    fn test_create_segment_duplicates() {
        let expected_node_counts = [1, 2, 2, 1];
        let expected_begin_ranges = [0, 4, 11, 47];
        let expected_end_ranges = [4, 11, 47, 65];

        test_create_segment_helper(
            true,
            4,
            &expected_node_counts,
            &expected_begin_ranges,
            &expected_end_ranges,
        );
    }

    #[test]
    fn test_calculate_segments_deadline_normal_heavy() {
        let expected_deadlines = [3.8461537, 13.461538, 69.23077, 11.538462, 51.923077];
        test_calculate_segments_deadline_helper(150, &expected_deadlines);
    }

    #[test]
    fn test_calculate_segments_deadline_normal_light() {
        let expected_deadlines = [2.300885, 4.026549, 20.707964, 6.9026546, 31.061947];
        test_calculate_segments_deadline_helper(65, &expected_deadlines);
    }

    #[test]
    fn test_calculate_segments_deadline_normal_mixture() {
        let expected_deadlines = [3.2285714, 10.33721, 53.16279, 9.685715, 43.585712];
        test_calculate_segments_deadline_helper(120, &expected_deadlines);
    }
}
