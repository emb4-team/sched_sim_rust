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
pub struct NodeLog {
    pub core_id: usize,
    pub node_id: i32,
    pub start_time: i32,
    pub finish_time: i32,
}

#[derive(Clone, Default)]
pub struct ProcessorLog {
    pub core_logs: Vec<CoreLog>,
    pub average_utilization_rate: f32,
    pub variance_utilization_rate: f32,
}

impl ProcessorLog {
    pub fn calculate_average_utilization(&mut self) {
        self.average_utilization_rate = self
            .core_logs
            .iter()
            .map(|core_log| core_log.utilization_rate)
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }

    pub fn calculate_variance_utilization(&mut self) {
        self.variance_utilization_rate = self
            .core_logs
            .iter()
            .map(|core_log| (core_log.utilization_rate - self.average_utilization_rate).powi(2))
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }
}

#[derive(Clone, Default)]
pub struct CoreLog {
    pub core_id: usize,
    pub total_proc_time: i32,
    pub utilization_rate: f32,
}

impl CoreLog {
    pub fn calculate_utilization(&mut self, schedule_length: i32) {
        self.utilization_rate = self.total_proc_time as f32 / schedule_length as f32;
    }
}
