use petgraph::Graph;

use crate::{
    fixed_priority_scheduler::FixedPriorityScheduler, graph_extension::NodeData,
    processor::ProcessorBase, scheduler::DAGSchedulerBase,
};

pub enum SchedulerType {
    FixedPriorityScheduler,
}

pub fn create_scheduler<T>(
    scheduler_type: SchedulerType,
    dag: &Graph<NodeData, i32>,
    processor: &T,
) -> Box<impl DAGSchedulerBase<T> + 'static>
where
    T: ProcessorBase + Clone + 'static,
{
    match scheduler_type {
        SchedulerType::FixedPriorityScheduler => {
            Box::new(FixedPriorityScheduler::new(dag, processor))
        }
    }
}
