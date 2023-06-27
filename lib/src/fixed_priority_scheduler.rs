use std::collections::VecDeque;

use crate::{
    core::ProcessResult,
    graph_extension::{GraphExtension, NodeData},
    non_preemptive_scheduler::NonPreemptiveScheduler,
    processor::ProcessorBase,
    scheduler::DAGSchedulerBase,
};

use petgraph::{graph::NodeIndex, Graph};

const DUMMY_EXECUTION_TIME: i32 = 1;

#[derive(Clone, Default)]
pub struct FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    pub non_preemptive_scheduler: NonPreemptiveScheduler<T>,
}

impl<T> DAGSchedulerBase<T> for FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &Graph<NodeData, i32>, processor: &T) -> Self {
        self::FixedPriorityScheduler {
            non_preemptive_scheduler: NonPreemptiveScheduler::new(dag, processor),
        }
    }

    fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.non_preemptive_scheduler.set_dag(dag);
    }

    fn set_processor(&mut self, processor: &T) {
        self.non_preemptive_scheduler.set_processor(processor);
    }

    fn schedule<F>(&mut self, func: F) -> (i32, VecDeque<NodeIndex>)
    where
        F: Fn(&Graph<NodeData, i32>, &mut VecDeque<NodeIndex>),
    {
        self.non_preemptive_scheduler.schedule(func)
    }
}

fn sort_by_priority(dag: &Graph<NodeData, i32>, ready_queue: &mut VecDeque<NodeIndex>) {
    ready_queue.make_contiguous().sort_by_key(|&node| {
        dag[node].params.get("priority").unwrap_or_else(|| {
            eprintln!(
                "Warning: 'priority' parameter not found for node {:?}",
                node
            );
            &999 // Because sorting cannot be done well without a priority
        })
    });
}
