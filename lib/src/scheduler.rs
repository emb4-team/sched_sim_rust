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
    fn schedule(&mut self) -> (i32, VecDeque<NodeIndex>);
    fn sort_ready_queue(&mut self, ready_queue: &mut VecDeque<NodeIndex>);
}

/// This function implements a fixed priority scheduling algorithm on a DAG (Directed Acyclic Graph).
///
/// # Arguments
///
/// * `processor` - An object that implements `ProcessorBase` trait, representing a CPU or a collection of CPU cores.
/// * `dag` - A mutable reference to a Graph object, representing the dependencies among tasks.
///
/// # Returns
///
/// * A floating point number representing the normalized total time taken to finish all tasks.
/// * A vector of NodeIndex, representing the order of tasks finished.
///
/// # Description
///
/// The function `fixed_priority_scheduler` is responsible for task scheduling based on a Directed Acyclic Graph (DAG).
/// Specifically, it schedules tasks, associated with priority, on a processor, and continues to do so until all tasks have been executed.
/// The tasks are scheduled in order of their priority, from highest to lowest (smaller values have higher priority).
///
/// Initially, a processor and a DAG are passed to this function.
/// Dummy source and sink nodes are added to the DAG. These nodes symbolize the start and end points of the tasks, respectively.
/// In the main loop of the function, the following operations are carried out:
///
/// 1. Nodes representing tasks that are ready to be scheduled are sorted by their priority.
/// 2. If there is an idle core available, the task with the highest priority is allocated to it.
/// 3. The processor processes for a single unit of time. This is repeated until all tasks are completed.
/// 4. Once all tasks are completed, dummy source and sink nodes are removed from the DAG.
///
/// The function returns the total time taken to complete all tasks (excluding the execution time of the dummy tasks) and the order in which the tasks were executed.
///
/// # Example
///
/// Refer to the examples in the tests code.
///
pub fn schedule<T>(scheduler: &mut impl DAGSchedulerBase<T>) -> (i32, VecDeque<NodeIndex>)
where
    T: ProcessorBase + Clone,
{
    let mut dag = scheduler.get_dag(); //To avoid adding pre_node_count to the original DAG
    let mut processor = scheduler.get_processor();
    let mut ready_queue = scheduler.get_ready_queue();
    let mut node_logs = scheduler.get_node_logs();
    let mut processor_log = scheduler.get_processor_log();
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
        scheduler.sort_ready_queue(&mut ready_queue);

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
                        node_logs[node_id].finish_time = current_time - DUMMY_EXECUTION_TIME;
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

    scheduler.set_node_logs(node_logs);
    scheduler.set_processor_log(processor_log);

    //Return the normalized total time taken to finish all tasks.
    (schedule_length, execution_order)
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

    pub fn set_ready_queue(&mut self, ready_queue: &VecDeque<NodeIndex>) {
        self.ready_queue = ready_queue.clone();
    }

    pub fn get_dag(&mut self) -> Graph<NodeData, i32> {
        self.dag.clone()
    }

    pub fn get_processor(&mut self) -> T {
        self.processor.clone()
    }

    pub fn get_ready_queue(&mut self) -> VecDeque<NodeIndex> {
        self.ready_queue.clone()
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

    pub fn get_node_logs(&mut self) -> Vec<NodeLog> {
        self.node_logs.clone()
    }

    pub fn get_processor_log(&mut self) -> ProcessorLog {
        self.processor_log.clone()
    }

    pub fn set_node_logs(&mut self, node_logs: Vec<NodeLog>) {
        self.node_logs = node_logs;
    }

    pub fn set_processor_log(&mut self, processor_log: ProcessorLog) {
        self.processor_log = processor_log;
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
