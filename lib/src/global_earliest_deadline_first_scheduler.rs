use std::collections::VecDeque;

use crate::{graph_extension::NodeData, log::*, processor::ProcessorBase, scheduler::*};

use petgraph::Graph;

#[derive(Clone, Default)]
pub struct GlobalEarliestDeadlineFirstScheduler<T>
where
    T: ProcessorBase + Clone,
{
    dag: Graph<NodeData, i32>,
    processor: T,
    log: DAGSchedulerLog,
}

impl<T> DAGSchedulerBase<T> for GlobalEarliestDeadlineFirstScheduler<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self {
        Self {
            dag: dag.clone(),
            processor: processor.clone(),
            log: DAGSchedulerLog::new(dag, processor.get_number_of_cores()),
        }
    }

    fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.dag = dag.clone();
        self.log.update_dag(dag);
    }

    fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
        self.log
            .update_processor(ProcessorLog::new(processor.get_number_of_cores()));
    }

    fn set_log(&mut self, log: DAGSchedulerLog) {
        self.log = log;
    }

    fn get_dag(&self) -> Graph<NodeData, i32> {
        self.dag.clone()
    }

    fn get_processor(&self) -> T {
        self.processor.clone()
    }

    fn get_log(&self) -> DAGSchedulerLog {
        self.log.clone()
    }

    fn sort_ready_queue(ready_queue: &mut VecDeque<NodeData>) {
        if ready_queue
            .front()
            .unwrap()
            .params
            .contains_key("deadline_factor")
        {
            ready_queue.make_contiguous().sort_by_key(|node| {
                *node
                    .params
                    .get("earliest_start_times")
                    .expect("please use calculate_earliest_start_times() before scheduling")
                    + *node.params.get("integer_scaled_deadline").unwrap()
                        / *node.params.get("deadline_factor").unwrap()
            });
        } else {
            ready_queue.make_contiguous().sort_by_key(|node| {
                *node
                    .params
                    .get("latest_finish_time")
                    .expect("please use calculate_latest_finish_times() before scheduling")
            });
        }
    }
}
