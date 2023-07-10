use petgraph::Graph;
use serde_derive::{Deserialize, Serialize};

use crate::{graph_extension::NodeData, output_log::append_info_to_yaml};

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

#[derive(Serialize, Deserialize)]
pub struct DAGSetLog {
    pub dag_set_log: Vec<DAGLog>,
}

#[allow(dead_code)] //TODO remove
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct DAGschedulerLog {
    pub node_logs: Vec<NodeLog>,
    pub processor_log: ProcessorLog,
}

impl DAGschedulerLog {
    pub fn new(dag: &Graph<NodeData, i32>, num_cores: usize) -> Self {
        Self {
            node_logs: dag
                .node_indices()
                .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
                .collect(),
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
        self.node_logs[node_id].core_id = core_id;
        self.node_logs[node_id].start_time = current_time;
        self.processor_log.core_logs[core_id].total_proc_time +=
            node_data.params.get("execution_time").unwrap_or(&0);
    }

    pub fn write_finishing_node_log(&mut self, node_data: &NodeData, current_time: i32) {
        self.node_logs[node_data.id as usize].finish_time = current_time;
    }

    pub fn write_scheduling_log(&mut self, schedule_length: i32) {
        self.processor_log
            .calculate_cores_utilization(schedule_length);
        self.processor_log.calculate_average_utilization();
        self.processor_log.calculate_variance_utilization();
    }

    pub fn dump_log_to_yaml(&self, file_path: &str) {
        self.dump_node_logs_to_yaml(file_path);
        self.dump_processor_log_to_yaml(file_path);
    }

    pub fn dump_node_logs_to_yaml(&self, file_path: &str) {
        let node_logs = NodeLogs {
            node_logs: self.node_logs.to_vec(),
        };
        let yaml = serde_yaml::to_string(&node_logs).expect("Failed to serialize NodeLogs to YAML");
        append_info_to_yaml(file_path, &yaml);
    }

    pub fn dump_processor_log_to_yaml(&self, file_path: &str) {
        let yaml = serde_yaml::to_string(&self.processor_log)
            .expect("Failed to serialize ProcessorLog to YAML");
        append_info_to_yaml(file_path, &yaml);
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

#[derive(Serialize, Deserialize)]
pub struct NodeLogs {
    pub node_logs: Vec<NodeLog>,
}

#[derive(Serialize, Deserialize)]
pub struct NodeSetLogs {
    pub node_set_logs: Vec<Vec<NodeLog>>,
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
