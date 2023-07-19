/*use petgraph::Graph;

use crate::{
    fixed_priority_scheduler::FixedPriorityScheduler, graph_extension::NodeData,
    processor::ProcessorBase, scheduler::DAGSchedulerBase,
};

pub enum SchedulerType {
    FixedPriorityScheduler,
}

pub fn create_scheduler<T>(
    scheduler_type: SchedulerType,
    dag: &mut Graph<NodeData, i32>,
    processor: &T,
) -> Box<impl DAGSchedulerBase<T>>
where
    T: ProcessorBase + Clone,
{
    match scheduler_type {
        SchedulerType::FixedPriorityScheduler => {
            Box::new(FixedPriorityScheduler::new(dag, processor))
        }
    }
}
*/
