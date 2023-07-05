//! # Scheduler
use std::collections::VecDeque;

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    processor::ProcessorBase,
};
use petgraph::graph::{Graph, NodeIndex};

use serde_derive::{Deserialize, Serialize};

const DUMMY_EXECUTION_TIME: i32 = 1;

pub trait DAGSchedulerBase<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self
    where
        Self: Sized;
    fn set_dag(&mut self, dag: &Graph<NodeData, i32>);
    fn set_processor(&mut self, processor: &T);
    fn set_ready_queue(&mut self, ready_queue: VecDeque<NodeIndex>);
    fn get_dag(&mut self) -> Graph<NodeData, i32>;
    fn get_processor(&mut self) -> T;
    fn get_ready_queue(&mut self) -> VecDeque<NodeIndex>;
    fn set_node_logs(&mut self, node_logs: Vec<NodeLog>);
    fn set_processor_log(&mut self, processor_log: ProcessorLog);
    fn get_node_logs(&mut self) -> Vec<NodeLog>;
    fn get_processor_log(&mut self) -> ProcessorLog;
    fn schedule(&mut self) -> (i32, VecDeque<NodeIndex>) {
        {
            let mut dag = self.get_dag(); //To avoid adding pre_node_count to the original DAG
            let mut processor = self.get_processor();
            let mut ready_queue = self.get_ready_queue();
            let mut node_logs = self.get_node_logs();
            let mut processor_log = self.get_processor_log();
            let mut execution_order = VecDeque::new();

            let source_node_i = dag.add_dummy_source_node();

            dag[source_node_i]
                .params
                .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);
            let sink_node_i = dag.add_dummy_sink_node();
            dag[sink_node_i]
                .params
                .insert("execution_time".to_string(), DUMMY_EXECUTION_TIME);

            ready_queue.push_back(source_node_i);

            let mut current_time = 0;
            loop {
                self.sort_ready_queue(&mut ready_queue);

                //Assign the highest priority task first to the first idle core found.
                while let Some(core_index) = processor.get_idle_core_index() {
                    if let Some(node_i) = ready_queue.pop_front() {
                        processor.allocate_specific_core(core_index, &dag[node_i]);

                        if node_i != source_node_i && node_i != sink_node_i {
                            let task_id = dag[node_i].id as usize;
                            node_logs[task_id].core_id = core_index;
                            node_logs[task_id].start_time = current_time - DUMMY_EXECUTION_TIME;
                            processor_log.core_logs[core_index].total_proc_time +=
                                dag[node_i].params.get("execution_time").unwrap_or(&0);
                        }
                        execution_order.push_back(node_i);
                    } else {
                        break;
                    }
                }

                //Move one unit time so that the core state of the previous loop does not remain.
                let mut process_result = processor.process();
                current_time += 1;

                //Process until there is a task finished.
                while !process_result
                    .iter()
                    .any(|result| matches!(result, ProcessResult::Done(_)))
                {
                    process_result = processor.process();
                    current_time += 1;
                }

                let finish_nodes: Vec<NodeIndex> = process_result
                    .iter()
                    .filter_map(|result| {
                        if let ProcessResult::Done(node_data) = result {
                            let node_id = node_data.id as usize;
                            let node_i = NodeIndex::new(node_id);
                            if node_i != source_node_i && node_i != sink_node_i {
                                node_logs[node_id].finish_time =
                                    current_time - DUMMY_EXECUTION_TIME;
                            }
                            Some(node_i)
                        } else {
                            None
                        }
                    })
                    .collect();

                if finish_nodes.len() == 1 && dag.get_suc_nodes(finish_nodes[0]).is_none() {
                    break; // The scheduling has finished because the dummy sink node has completed.
                }

                //Executable if all predecessor nodes are done
                for finish_node in finish_nodes {
                    let suc_nodes = dag.get_suc_nodes(finish_node).unwrap_or_default();
                    for suc_node in suc_nodes {
                        dag.increment_pre_done_count(suc_node);
                        if dag.is_node_ready(suc_node) {
                            ready_queue.push_back(suc_node);
                        }
                    }
                }
            }

            //remove dummy nodes
            dag.remove_dummy_sink_node();
            dag.remove_dummy_source_node();

            //Remove the dummy node from the execution order.
            execution_order.pop_back();
            execution_order.pop_front();

            let schedule_length = current_time - DUMMY_EXECUTION_TIME * 2;
            processor_log.calculate_cores_utilization(schedule_length);

            processor_log.calculate_average_utilization();

            processor_log.calculate_variance_utilization();

            self.set_node_logs(node_logs);
            self.set_processor_log(processor_log);

            //Return the normalized total time taken to finish all tasks.
            (schedule_length, execution_order)
        }
    }
    fn sort_ready_queue(&mut self, ready_queue: &mut VecDeque<NodeIndex>);
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
