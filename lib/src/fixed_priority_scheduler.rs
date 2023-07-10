use std::collections::VecDeque;

use crate::{graph_extension::NodeData, log::*, processor::ProcessorBase, scheduler::*};

use petgraph::Graph;

#[derive(Clone, Default)]
pub struct FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    pub dag: Graph<NodeData, i32>,
    pub processor: T,
    pub log: DAGschedulerLog,
}

impl<T> DAGSchedulerBase<T> for FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    fn new(dag: &mut Graph<NodeData, i32>, processor: &T) -> Self {
        Self {
            dag: dag.clone(),
            processor: processor.clone(),
            log: DAGschedulerLog::new(dag, processor.get_number_of_cores()),
        }
    }

    fn get_name(&self) -> String {
        "FixedPriorityScheduler".to_string()
    }

    fn set_dag(&mut self, dag: &Graph<NodeData, i32>) {
        self.dag = dag.clone();
        self.log.node_logs = dag
            .node_indices()
            .map(|node_index| NodeLog::new(0, dag[node_index].id as usize))
            .collect()
    }

    fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
        self.log.processor_log = ProcessorLog::new(processor.get_number_of_cores());
    }

    fn get_dag(&mut self) -> Graph<NodeData, i32> {
        self.dag.clone()
    }

    fn get_processor(&mut self) -> T {
        self.processor.clone()
    }

    fn get_log(&mut self) -> &mut DAGschedulerLog {
        &mut self.log
    }

    fn sort_ready_queue(ready_queue: &mut VecDeque<NodeData>) {
        ready_queue.make_contiguous().sort_by_key(|node| {
            *node.params.get("priority").unwrap_or_else(|| {
                eprintln!(
                    "Warning: 'priority' parameter not found for node {:?}",
                    node
                );
                &999 // Because sorting cannot be done well without a priority
            })
        });
    }

    fn dump_characteristic_log(&mut self, _file_path: &str) {}
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::homogeneous::HomogeneousProcessor;
    use crate::processor::ProcessorBase;
    use crate::scheduler_creator::{create_scheduler, SchedulerType};
    use petgraph::graph::{Graph, NodeIndex};

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
        add_params(&mut dag, c0, "period", 100);
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

        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &HomogeneousProcessor::new(2),
        );
        let result = fixed_priority_scheduler.schedule();

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
        add_params(&mut dag, c0, "period", 100);
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

        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &HomogeneousProcessor::new(3),
        );
        let result = fixed_priority_scheduler.schedule();

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
        let c0 = dag.add_node(create_node(0, "execution_time", 1));
        add_params(&mut dag, c0, "period", 100);
        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &HomogeneousProcessor::new(1),
        );
        let result = fixed_priority_scheduler.schedule();
        assert_eq!(result.0, 1);
        assert_eq!(result.1, vec![NodeIndex::new(0)]);

        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &HomogeneousProcessor::new(1),
        );
        let result = fixed_priority_scheduler.schedule();
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
        add_params(&mut dag, c0, "period", 100);
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

        let mut fixed_priority_scheduler = create_scheduler(
            SchedulerType::FixedPriorityScheduler,
            &mut dag,
            &HomogeneousProcessor::new(2),
        );
        fixed_priority_scheduler.schedule();

        assert_eq!(
            fixed_priority_scheduler
                .get_log()
                .processor_log
                .average_utilization,
            0.61956525
        );

        assert_eq!(
            fixed_priority_scheduler
                .get_log()
                .processor_log
                .variance_utilization,
            0.14473063
        );

        assert_eq!(
            fixed_priority_scheduler.get_log().processor_log.core_logs[0].core_id,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.get_log().processor_log.core_logs[0].total_proc_time,
            92
        );
        assert_eq!(
            fixed_priority_scheduler.get_log().processor_log.core_logs[0].utilization,
            1.0
        );

        assert_eq!(fixed_priority_scheduler.get_log().node_logs[0].core_id, 0);
        assert_eq!(fixed_priority_scheduler.get_log().node_logs[0].dag_id, 0);
        assert_eq!(fixed_priority_scheduler.get_log().node_logs[0].node_id, 0);
        assert_eq!(
            fixed_priority_scheduler.get_log().node_logs[0].start_time,
            0
        );
        assert_eq!(
            fixed_priority_scheduler.get_log().node_logs[0].finish_time,
            52
        );

        assert_eq!(fixed_priority_scheduler.get_log().node_logs[1].core_id, 0);
        assert_eq!(fixed_priority_scheduler.get_log().node_logs[1].dag_id, 0);
        assert_eq!(fixed_priority_scheduler.get_log().node_logs[1].node_id, 1);
        assert_eq!(
            fixed_priority_scheduler.get_log().node_logs[1].start_time,
            52
        );
        assert_eq!(
            fixed_priority_scheduler.get_log().node_logs[1].finish_time,
            92
        );
    }
}
