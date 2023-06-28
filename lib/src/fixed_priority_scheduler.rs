use std::collections::VecDeque;

use crate::{
    graph_extension::NodeData, non_preemptive_scheduler::NonPreemptiveScheduler,
    processor::ProcessorBase, scheduler::DAGSchedulerBase,
};

use petgraph::{graph::NodeIndex, Graph};

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

    fn schedule<F>(&mut self, _: F) -> (i32, VecDeque<NodeIndex>)
    where
        F: Fn(&Graph<NodeData, i32>, &mut VecDeque<NodeIndex>),
    {
        let func = |dag: &Graph<NodeData, i32>, ready_queue: &mut VecDeque<NodeIndex>| {
            sort_by_priority(dag, ready_queue)
        };
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::homogeneous::HomogeneousProcessor;
    use crate::processor::ProcessorBase;

    fn dummy_sort(_: &Graph<NodeData, i32>, _: &mut VecDeque<NodeIndex>) {}

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn add_params(dag: &mut Graph<NodeData, i32>, node: NodeIndex, key: &str, value: i32) {
        let node_added = dag.node_weight_mut(node).unwrap();
        node_added.params.insert(key.to_string(), value);
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        add_params(&mut dag, c0, "priority", 0);
        add_params(&mut dag, c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 12));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        add_params(&mut dag, n0_0, "priority", 2);
        add_params(&mut dag, n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(2));
        let result = fixed_priority_scheduler.schedule(dummy_sort);

        assert_eq!(result.0, 92);

        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_concurrent_task() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        add_params(&mut dag, c0, "priority", 0);
        add_params(&mut dag, c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        add_params(&mut dag, n0_0, "priority", 2);
        add_params(&mut dag, n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(3));
        let result = fixed_priority_scheduler.schedule(dummy_sort);

        assert_eq!(result.0, 92);
        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(2)
            ]
        );
    }

    #[test]
    fn test_fixed_priority_scheduler_schedule_used_twice_for_same_dag() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        dag.add_node(create_node(0, "execution_time", 1));

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule(dummy_sort);
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(1));
        let result = fixed_priority_scheduler.schedule(dummy_sort);
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);
    }

    #[test]
    fn test_fixed_priority_scheduler_log_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        add_params(&mut dag, c0, "priority", 0);
        add_params(&mut dag, c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 12));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        add_params(&mut dag, n0_0, "priority", 2);
        add_params(&mut dag, n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        let mut fixed_priority_scheduler =
            FixedPriorityScheduler::new(&dag, &HomogeneousProcessor::new(2));
        fixed_priority_scheduler.schedule(dummy_sort);

        assert_eq!(
            fixed_priority_scheduler
                .non_preemptive_scheduler
                .processor_log
                .average_utilization,
            0.61956525
        );

        assert_eq!(
            fixed_priority_scheduler
                .non_preemptive_scheduler
                .processor_log
                .variance_utilization,
            0.14473063
        );

        assert_eq!(
            fixed_priority_scheduler
                .non_preemptive_scheduler
                .processor_log
                .core_logs[0]
                .core_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler
                .non_preemptive_scheduler
                .processor_log
                .core_logs[0]
                .total_proc_time,
            92
        );
        assert_eq!(
            fixed_priority_scheduler
                .non_preemptive_scheduler
                .processor_log
                .core_logs[0]
                .utilization,
            1.0
        );

        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[0].core_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[0].dag_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[0].node_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[0].start_time,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[0].finish_time,
            52
        );

        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[1].core_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[1].dag_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[1].node_id,
            1
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[1].start_time,
            52
        );
        assert_eq!(
            fixed_priority_scheduler.non_preemptive_scheduler.node_logs[1].finish_time,
            92
        );
    }
}
