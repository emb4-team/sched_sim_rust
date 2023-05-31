use lib::graph_extension::{GraphExtension, NodeData};
use lib::homogeneous::HomogeneousProcessor;
use lib::processor::ProcessorBase;
use petgraph::graph::{Graph, NodeIndex};
use std::collections::HashMap;

#[allow(dead_code)] // TODO: remove
pub fn pre_computation(
    dag: &mut Graph<NodeData, f32>,
) -> HashMap<Graph<NodeData, i32>, Vec<NodeIndex>> {
    let vol = dag.get_volume() as i32;
    let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap() as i32;
    let m_min = (vol as f32 / end_to_end_deadline as f32).ceil() as usize;
    let execution_order: HashMap<Graph<NodeData, i32>, Vec<NodeIndex>> = HashMap::new();

    #[allow(unused_variables, unused_mut)] // TODO: remove
    let mut processor = HomogeneousProcessor::new(m_min);

    /*while 固定優先度スケジューラ (processor, dag) > end_to_end_deadline {
        m_min += 1;
        processor = HomogeneousProcessor::new(m_min);
      }
    固定優先度スケジューラ (processor, dag)から実行順序を取得*/
    execution_order
}

#[cfg(test)]

mod tests {}
