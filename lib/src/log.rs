use log::warn;
use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};

use crate::{
    graph_extension::{GraphExtension, NodeData},
    output_log::append_info_to_yaml,
};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSetInfo {
    total_utilization: f32,
    each_dag_info: Vec<DAGInfo>,
}

impl DAGSetInfo {
    pub fn new(dag_set: &[Graph<NodeData, i32>]) -> Self {
        let mut total_utilization = 0.0;
        let mut each_dag_info = Vec::new();

        for dag in dag_set.iter() {
            let dag_info = DAGInfo::new(dag);
            total_utilization += dag_info.volume as f32 / dag_info.period as f32;
            each_dag_info.push(dag_info);
        }

        Self {
            total_utilization,
            each_dag_info,
        }
    }

    pub fn get_total_utilization(&self) -> f32 {
        self.total_utilization
    }

    pub fn get_each_dag_info(&self) -> Vec<DAGInfo> {
        self.each_dag_info.clone()
    }

    pub fn dump_dag_set_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self).expect("Failed to serialize DAGSetInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGInfo {
    critical_path_length: i32,
    period: i32,
    end_to_end_deadline: i32,
    volume: i32,
    utilization: f32,
}

impl DAGInfo {
    pub fn new(dag: &Graph<NodeData, i32>) -> Self {
        let period = dag.get_head_period().unwrap_or(0);
        let end_to_end_deadline = dag.get_end_to_end_deadline().unwrap_or(0);
        let volume = dag.get_volume();
        let utilization = match (end_to_end_deadline, period) {
            (0, 0) => {
                warn!("Both period and end_to_end_deadline are not set.");
                0.0
            }
            (_, 0) => {
                warn!("Period is not set.");
                0.0
            }
            (0, _) => period as f32 / volume as f32,
            (_, _) => period as f32 / volume as f32,
        };

        let critical_path = dag.clone().get_critical_path();
        Self {
            critical_path_length: dag.get_total_wcet_from_nodes(&critical_path),
            period,
            end_to_end_deadline,
            volume,
            utilization,
        }
    }

    pub fn get_critical_path_length(&self) -> i32 {
        self.critical_path_length
    }

    pub fn get_period(&self) -> i32 {
        self.period
    }

    pub fn get_volume(&self) -> i32 {
        self.volume
    }

    pub fn dump_dag_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self).expect("Failed to serialize DAGInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }

    pub fn get_utilization(&self) -> f32 {
        self.utilization
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ProcessorInfo {
    pub number_of_cores: usize,
}

impl ProcessorInfo {
    pub fn new(number_of_cores: usize) -> Self {
        Self { number_of_cores }
    }

    pub fn dump_processor_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self).expect("Failed to serialize ProcessorInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSetLog {
    pub dag_set_log: Vec<DAGLog>,
}

impl DAGSetLog {
    pub fn new(dag_set: &[Graph<NodeData, i32>]) -> Self {
        let mut dag_set_log = Vec::with_capacity(dag_set.len());
        for i in 0..dag_set.len() {
            dag_set_log.push(DAGLog::new(i));
        }
        Self { dag_set_log }
    }

    pub fn dump_dag_set_log_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.dag_set_log)
            .expect("Failed to serialize ProcessorInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGLog {
    pub dag_id: usize,
    pub release_time: i32,
    pub start_time: i32,
    pub finish_time: i32,
}

impl DAGLog {
    pub fn new(dag_id: usize) -> Self {
        Self {
            dag_id,
            release_time: Default::default(),
            start_time: Default::default(),
            finish_time: Default::default(),
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
pub struct NodeLogs {
    pub node_logs: Vec<NodeLog>,
}

impl NodeLogs {
    pub fn new(dag: &Graph<NodeData, i32>) -> Self {
        let mut node_logs = Vec::with_capacity(dag.node_count());
        for node in dag.node_indices() {
            node_logs.push(NodeLog::new(0, dag[node].id as usize));
        }
        Self { node_logs }
    }

    pub fn dump_node_logs_to_yaml(&self, file_path: &str) {
        let node_logs = NodeLogs {
            node_logs: self.node_logs.to_vec(),
        };

        let yaml = serde_yaml::to_string(&node_logs).expect("Failed to serialize NodeLogs to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct NodeSetLogs {
    pub node_set_logs: Vec<Vec<NodeLog>>,
}

impl NodeSetLogs {
    pub fn new(dag_set: &[Graph<NodeData, i32>]) -> Self {
        let mut node_set_logs = Vec::with_capacity(dag_set.len());
        for (i, dag) in dag_set.iter().enumerate() {
            let mut node_logs = Vec::with_capacity(dag.node_count());
            for node in dag.node_indices() {
                node_logs.push(NodeLog::new(i, dag[node].id as usize));
            }
            node_set_logs.push(node_logs);
        }

        Self { node_set_logs }
    }

    pub fn dump_node_set_logs_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.node_set_logs)
            .expect("Failed to serialize NodeSetLogs to YAML");
        append_info_to_yaml(file_path, &yaml);
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

    fn calculate_average_utilization(&mut self) {
        self.average_utilization = self
            .core_logs
            .iter()
            .map(|core_log| core_log.utilization)
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }

    fn calculate_variance_utilization(&mut self) {
        self.variance_utilization = self
            .core_logs
            .iter()
            .map(|core_log| (core_log.utilization - self.average_utilization).powi(2))
            .sum::<f32>()
            / self.core_logs.len() as f32;
    }

    fn calculate_cores_utilization(&mut self, schedule_length: i32) {
        for core_log in self.core_logs.iter_mut() {
            core_log.calculate_utilization(schedule_length);
        }
    }

    fn dump_processor_log_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self).expect("Failed to serialize ProcessorLog to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct CoreLog {
    pub core_id: usize,
    pub total_proc_time: i32,
    pub utilization: f32,
}

impl CoreLog {
    fn new(core_id: usize) -> Self {
        Self {
            core_id,
            total_proc_time: Default::default(),
            utilization: Default::default(),
        }
    }

    fn calculate_utilization(&mut self, schedule_length: i32) {
        self.utilization = self.total_proc_time as f32 / schedule_length as f32;
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSchedulerLog {
    pub dag_info: DAGInfo,
    pub processor_info: ProcessorInfo,
    pub node_logs: NodeLogs,
    pub processor_log: ProcessorLog,
}

impl DAGSchedulerLog {
    pub fn new(dag: &mut Graph<NodeData, i32>, num_cores: usize) -> Self {
        Self {
            dag_info: DAGInfo::new(dag),
            processor_info: ProcessorInfo::new(num_cores),
            node_logs: NodeLogs::new(dag),
            processor_log: ProcessorLog::new(num_cores),
        }
    }

    pub fn write_allocating_log(
        &mut self,
        node_data: &NodeData,
        core_id: usize,
        current_time: i32,
    ) {
        let node_id = node_data.id as usize;
        self.node_logs.node_logs[node_id].core_id = core_id;
        self.node_logs.node_logs[node_id].start_time = current_time;
        self.processor_log.core_logs[core_id].total_proc_time +=
            node_data.params.get("execution_time").unwrap_or(&0);
    }

    pub fn write_finishing_node_log(&mut self, node_data: &NodeData, current_time: i32) {
        self.node_logs.node_logs[node_data.id as usize].finish_time = current_time;
    }

    pub fn write_scheduling_log(&mut self, schedule_length: i32) {
        self.processor_log
            .calculate_cores_utilization(schedule_length);
        self.processor_log.calculate_average_utilization();
        self.processor_log.calculate_variance_utilization();
    }

    pub fn dump_log_to_yaml(&self, file_path: &str) {
        self.dag_info.dump_dag_info_to_yaml(file_path);
        self.processor_info.dump_processor_info_to_yaml(file_path);
        self.node_logs.dump_node_logs_to_yaml(file_path);
        self.processor_log.dump_processor_log_to_yaml(file_path);
    }
}

#[allow(dead_code)] //TODO remove
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSetSchedulerLog {
    pub dag_set_info: DAGSetInfo,
    pub processor_info: ProcessorInfo,
    pub dag_set_log: DAGSetLog,
    pub node_set_logs: NodeSetLogs,
    pub processor_log: ProcessorLog,
}

impl DAGSetSchedulerLog {
    pub fn new(dag_set: &mut [Graph<NodeData, i32>], num_cores: usize) -> Self {
        Self {
            dag_set_info: DAGSetInfo::new(dag_set),
            processor_info: ProcessorInfo::new(num_cores),
            dag_set_log: DAGSetLog::new(dag_set),
            node_set_logs: NodeSetLogs::new(dag_set),
            processor_log: ProcessorLog::new(num_cores),
        }
    }

    pub fn write_dag_release_time_log(&mut self, dag_id: usize, release_time: i32) {
        self.dag_set_log.dag_set_log[dag_id].release_time = release_time;
    }

    pub fn write_dag_start_time_log(&mut self, dag_id: usize, start_time: i32) {
        self.dag_set_log.dag_set_log[dag_id].start_time = start_time;
    }

    pub fn write_dag_finish_time_log(&mut self, dag_id: usize, finish_time: i32) {
        self.dag_set_log.dag_set_log[dag_id].finish_time = finish_time;
    }

    pub fn write_allocating_log(
        &mut self,
        dag_id: usize,
        node_id: usize,
        core_id: usize,
        current_time: i32,
        proc_time: i32,
    ) {
        self.node_set_logs.node_set_logs[dag_id][node_id].core_id = core_id;
        self.node_set_logs.node_set_logs[dag_id][node_id].start_time = current_time;
        self.processor_log.core_logs[core_id].total_proc_time += proc_time;
    }

    pub fn write_finishing_node_log(&mut self, node_data: &NodeData, finish_time: i32) {
        self.node_set_logs.node_set_logs[node_data.params["dag_id"] as usize]
            [node_data.id as usize]
            .finish_time = finish_time;
    }

    pub fn write_scheduling_log(&mut self, schedule_length: i32) {
        self.processor_log
            .calculate_cores_utilization(schedule_length);
        self.processor_log.calculate_average_utilization();
        self.processor_log.calculate_variance_utilization();
    }

    pub fn dump_log_to_yaml(&self, file_path: &str) {
        self.dag_set_info.dump_dag_set_info_to_yaml(file_path);
        self.processor_info.dump_processor_info_to_yaml(file_path);
        self.dag_set_log.dump_dag_set_log_to_yaml(file_path);
        self.node_set_logs.dump_node_set_logs_to_yaml(file_path);
        self.processor_log.dump_processor_log_to_yaml(file_path);
    }
}
