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
    pub node_id: usize,
    pub start_time: i32,
    pub finish_time: i32,
}

impl NodeLog {
    pub fn new(node_id: usize) -> Self {
        Self {
            core_id: Default::default(),
            node_id,
            start_time: Default::default(),
            finish_time: Default::default(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ProcessorLog {
    pub core_logs: Vec<CoreLog>,
    pub average_utilization: f32,
    pub variance_utilization: f32,
}

impl ProcessorLog {
    pub fn new(num_cores: usize) -> Self {
        Self {
            core_logs: (0..num_cores).map(CoreLog::new).collect(),
            average_utilization: Default::default(),
            variance_utilization: Default::default(),
        }
    }
    pub fn calculate_average_utilization(&mut self) {
        self.average_utilization = self
            .core_logs
            .iter()
            .map(|core_log| core_log.utilization)
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }

    pub fn calculate_variance_utilization(&mut self) {
        self.variance_utilization = self
            .core_logs
            .iter()
            .map(|core_log| (core_log.utilization - self.average_utilization).powi(2))
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }
}

#[derive(Clone, Default)]
pub struct CoreLog {
    pub core_id: usize,
    pub total_proc_time: i32,
    pub utilization: f32,
}

impl CoreLog {
    pub fn new(core_id: usize) -> Self {
        Self {
            core_id,
            total_proc_time: Default::default(),
            utilization: Default::default(),
        }
    }
    pub fn calculate_utilization(&mut self, schedule_length: i32) {
        self.utilization = self.total_proc_time as f32 / schedule_length as f32;
    }
}
