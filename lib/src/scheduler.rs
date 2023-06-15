use std::collections::VecDeque;

use crate::{graph_extension::NodeData, processor::ProcessorBase};
use petgraph::graph::{Graph, NodeIndex};
pub trait SchedulerBase<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self;
    fn set_dag(&mut self, dag: &Graph<NodeData, i32>);
    fn set_processor(&mut self, processor: &T);
    fn schedule(&mut self) -> (i32, VecDeque<NodeIndex>);
}

#[derive(Clone, Default)]
pub struct ScheduledNodeData {
    pub core_id: usize,
    pub node_id: i32,
    pub start_time: i32,
    pub end_time: i32,
}

#[derive(Clone, Default)]
pub struct ScheduledProcessorData {
    pub scheduled_core_data: Vec<ScheduledCoreData>,
    pub average_utilization_rate: f32,
    pub variance_utilization_rate: f32,
}

impl ScheduledProcessorData {
    pub fn set_average_utilization_rate(&mut self) {
        self.average_utilization_rate = self
            .scheduled_core_data
            .iter()
            .map(|core_data| core_data.utilization_rate)
            .sum::<f32>()
            / self.scheduled_core_data.len() as f32;
    }

    pub fn set_variance_utilization_rate(&mut self) {
        self.variance_utilization_rate = self
            .scheduled_core_data
            .iter()
            .map(|core_data| (core_data.utilization_rate - self.average_utilization_rate).powi(2))
            .sum::<f32>()
            / self.scheduled_core_data.len() as f32;
    }
}

#[derive(Clone, Default)]
pub struct ScheduledCoreData {
    pub core_id: usize,
    pub total_proc_time: i32,
    pub utilization_rate: f32,
}

impl ScheduledCoreData {
    pub fn set_utilization_rate(&mut self, schedule_length: i32) {
        self.utilization_rate = self.total_proc_time as f32 / schedule_length as f32;
    }
}
