use std::collections::VecDeque;

use crate::{graph_extension::NodeData, processor::ProcessorBase};
use petgraph::graph::{Graph, NodeIndex};

use serde_derive::{Deserialize, Serialize};

pub trait DAGSchedulerBase<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self
    where
        Self: Sized;
    fn set_dag(&mut self, dag: &Graph<NodeData, i32>);
    fn set_processor(&mut self, processor: &T);
    fn get_node_logs(&self) -> &Vec<NodeLog>;
    fn get_processor_log(&self) -> &ProcessorLog;
    fn schedule(&mut self) -> (i32, VecDeque<NodeIndex>);
    fn sort_ready_queue(&mut self);
}

pub trait DAGSetSchedulerBase<T: ProcessorBase + Clone> {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &T) -> Self;
    fn schedule(&mut self) -> i32;
}

#[derive(Clone, Default)]
pub struct DAGSchedulerContext<T: ProcessorBase + Clone> {
    pub dag: Graph<NodeData, i32>,
    pub processor: T,
    pub ready_queue: VecDeque<NodeIndex>,
}

impl<T: ProcessorBase + Clone> DAGSchedulerContext<T> {
    pub fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self {
        Self {
            dag: dag.clone(),
            processor: processor.clone(),
            ready_queue: VecDeque::new(),
        }
    }

    pub fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.dag = dag.clone();
    }

    pub fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
    }
}

#[derive(Clone, Default)]
pub struct DAGSchedulerLog {
    pub node_logs: Vec<NodeLog>,
    pub processor_log: ProcessorLog,
}

impl DAGSchedulerLog {
    pub fn new(dag: &Graph<NodeData, i32>, num_cores: usize) -> Self {
        Self {
            node_logs: dag
                .node_indices()
                .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
                .collect(),
            processor_log: ProcessorLog::new(num_cores),
        }
    }

    pub fn init_node_logs(&mut self, dag: &Graph<NodeData, i32>) {
        self.node_logs = dag
            .node_indices()
            .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
            .collect()
    }

    pub fn init_processor_log(&mut self, num_cores: usize) {
        self.processor_log = ProcessorLog::new(num_cores);
    }

    pub fn get_node_logs(&self) -> &Vec<NodeLog> {
        &self.node_logs
    }

    pub fn get_processor_log(&self) -> &ProcessorLog {
        &self.processor_log
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGLog {
    pub dag_id: usize,
    pub release_time: i32,
    pub start_time: i32,
    pub finish_time: i32,
    pub minimum_cores: i32,
}

impl DAGLog {
    pub fn new(dag_id: usize) -> Self {
        Self {
            dag_id,
            release_time: Default::default(),
            start_time: Default::default(),
            finish_time: Default::default(),
            minimum_cores: Default::default(),
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct NodeLog {
    pub core_id: usize,
    pub dag_id: usize, // Used to distinguish DAGs when the scheduler input is DAGSet
    pub node_id: usize,
    pub start_time: i32,
    pub finish_time: i32,
}

impl NodeLog {
    pub fn new(dag_id: usize, node_id: usize) -> Self {
        Self {
            core_id: Default::default(),
            dag_id,
            node_id,
            start_time: Default::default(),
            finish_time: Default::default(),
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ProcessorLog {
    pub average_utilization: f32,
    pub variance_utilization: f32,
    pub core_logs: Vec<CoreLog>,
}

impl ProcessorLog {
    pub fn new(num_cores: usize) -> Self {
        Self {
            average_utilization: Default::default(),
            variance_utilization: Default::default(),
            core_logs: (0..num_cores).map(CoreLog::new).collect(),
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

    pub fn calculate_cores_utilization(&mut self, schedule_length: i32) {
        for core_log in self.core_logs.iter_mut() {
            core_log.calculate_utilization(schedule_length);
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
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
