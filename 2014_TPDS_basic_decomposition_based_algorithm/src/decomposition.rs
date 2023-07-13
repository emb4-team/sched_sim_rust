use std::vec;

use lib::graph_extension::NodeData;
use petgraph::graph::Graph;

use crate::handle_segment::*;

#[allow(dead_code)]
pub fn decompose(dag: &mut Graph<NodeData, i32>) {
    let mut segments = create_segments(dag);
    calculate_segments_deadline(dag, &mut segments);

    let mut nodes_deadline = vec![0.0; dag.node_count()];
    for segment in segments.iter() {
        segment.nodes.iter().for_each(|node| {
            nodes_deadline[node.id as usize] += segment.deadline;
        });
    }

    for node_deadline in nodes_deadline {
        if node_deadline.fract() == 0.0 {
            println!("{} 1 {}", node_deadline, node_deadline as i32);
            continue;
        }
        let integer_part_str = node_deadline.trunc().abs().to_string();
        let decimal_part_str = node_deadline
            .abs()
            .to_string()
            .chars()
            .skip(integer_part_str.len() + 1)
            .collect::<String>();

        let mut decimal_part = decimal_part_str.parse::<f32>().unwrap();
        decimal_part /= 10u64.pow(decimal_part_str.len().try_into().unwrap()) as f32;

        let rounded_decimal_part: f32 = format!("{:.5}", decimal_part).parse().unwrap();
        let rounded_deadline = integer_part_str.parse::<f32>().unwrap() + rounded_decimal_part;

        let decimal_part_str = rounded_deadline
            .abs()
            .to_string()
            .chars()
            .skip(integer_part_str.len() + 1)
            .collect::<String>();

        println!(
            "{} {} {}",
            rounded_deadline,
            decimal_part_str.len(),
            rounded_deadline * 10u64.pow(decimal_part_str.len().try_into().unwrap()) as f32
        );
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
    fn test_decompose_normal() {
        let mut dag = create_sample_dag(120);
        decompose(&mut dag);
    }
}
