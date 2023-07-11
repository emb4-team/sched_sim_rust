use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};

use crate::{
    graph_extension::{GraphExtension, NodeData},
    output_log::append_info_to_yaml,
};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSetInfo {
    pub total_utilization: f32,
    pub each_dag_info: Vec<DAGInfo>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGInfo {
    pub critical_path_length: i32,
    pub end_to_end_deadline: i32,
    pub volume: i32,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ProcessorInfo {
    pub number_of_cores: usize,
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

#[derive(Serialize, Deserialize)]
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

    pub fn dump_processor_log_to_yaml(&self, file_path: &str) {
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

#[allow(dead_code)] //TODO remove
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGSchedulerLog {
    pub dag_info: DAGInfo,
    pub processor_info: ProcessorInfo,
    pub node_logs: NodeLogs,
    pub processor_log: ProcessorLog,
}

impl DAGSchedulerLog {
    pub fn new(dag: &mut Graph<NodeData, i32>, num_cores: usize) -> Self {
        let volume = dag.get_volume();
        let period = dag.get_head_period().unwrap_or(0);
        let critical_path = dag.get_critical_path();
        let critical_path_length = dag.get_total_wcet_from_nodes(&critical_path);

        let dag_info = DAGInfo {
            critical_path_length,
            end_to_end_deadline: period,
            volume,
        };

        let processor_info = ProcessorInfo {
            number_of_cores: num_cores,
        };

        Self {
            dag_info,
            processor_info,
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
        self.dump_dag_info_to_yaml(file_path);
        self.dump_processor_info_to_yaml(file_path);
        self.node_logs.dump_node_logs_to_yaml(file_path);
        self.processor_log.dump_processor_log_to_yaml(file_path);
    }

    pub fn dump_dag_info_to_yaml(&self, file_path: &str) {
        let yaml =
            serde_yaml::to_string(&self.dag_info).expect("Failed to serialize DAGInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }

    pub fn dump_processor_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.processor_info)
            .expect("Failed to serialize ProcessorInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}

pub struct DAGSetSchedulerLog {
    pub dag_set_info: DAGSetInfo,
    pub processor_info: ProcessorInfo,
    pub dag_set_log: DAGSetLog,
    pub node_set_logs: NodeSetLogs,
    pub processor_log: ProcessorLog,
}

impl DAGSetSchedulerLog {
    pub fn new(dag_set: &mut [Graph<NodeData, i32>], num_cores: usize) -> Self {
        let mut total_utilization = 0.0;
        let mut dag_infos = Vec::new();
        let mut node_logs = Vec::new();

        for dag in dag_set.iter_mut() {
            let volume = dag.get_volume();
            let period = dag.get_head_period().unwrap();
            let critical_path = dag.get_critical_path();
            let critical_path_length = dag.get_total_wcet_from_nodes(&critical_path);
            total_utilization += volume as f32 / period as f32;

            let dag_info = DAGInfo {
                critical_path_length,
                end_to_end_deadline: period,
                volume,
            };

            dag_infos.push(dag_info);
            node_logs.push(NodeLogs::new(dag));
        }

        let dag_set_info = DAGSetInfo {
            total_utilization,
            each_dag_info: dag_infos,
        };

        let processor_info = ProcessorInfo {
            number_of_cores: num_cores,
        };

        Self {
            dag_set_info,
            processor_info,
            dag_set_log: DAGSetLog::new(dag_set),
            node_set_logs: NodeSetLogs::new(dag_set),
            processor_log: ProcessorLog::new(num_cores),
        }
    }

    pub fn dump_log_to_yaml(&self, file_path: &str) {
        self.dump_dag_set_info_to_yaml(file_path);
        self.dump_processor_info_to_yaml(file_path);
        self.dag_set_log.dump_dag_set_log_to_yaml(file_path);
        self.node_set_logs.dump_node_set_logs_to_yaml(file_path);
        self.processor_log.dump_processor_log_to_yaml(file_path);
    }

    pub fn dump_dag_set_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.dag_set_info)
            .expect("Failed to serialize DAGSetInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }

    pub fn dump_processor_info_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.processor_info)
            .expect("Failed to serialize ProcessorInfo to YAML");
        append_info_to_yaml(file_path, &yaml);
    }
}
