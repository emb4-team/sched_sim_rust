use std::collections::VecDeque;

use crate::{graph_extension::NodeData, log::*, processor::ProcessorBase, scheduler::*};

use petgraph::Graph;

#[derive(Clone, Default)]
pub struct FixedPriorityScheduler<T>
where
    T: ProcessorBase + Clone,
{
    dag: Graph<NodeData, i32>,
    processor: T,
    log: DAGSchedulerLog,
}

impl<T> DAGSchedulerBase<T> for FixedPriorityScheduler<T>
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
        self.log.set_node_logs(NodeLogs::new(dag));
    }

    fn set_processor(&mut self, processor: &T) {
        self.processor = processor.clone();
        self.log
            .set_processor_log(ProcessorLog::new(processor.get_number_of_cores()));
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::remove_file;

    use super::*;
    use crate::homogeneous::HomogeneousProcessor;
    use crate::processor::ProcessorBase;
    use crate::scheduler_creator::{create_scheduler, SchedulerType};
    use petgraph::graph::{Graph, NodeIndex};
    use serde_derive::Deserialize;

    #[derive(Deserialize)]
    struct TestDAGSchedulerLog {
        dag_info: TestDAGInfo,
        processor_info: TestProcessorInfo,
        node_logs: TestNodeLogs,
        processor_log: TestProcessorLog,
    }

    #[derive(Deserialize)]
    struct TestDAGInfo {
        critical_path_length: i32,
        period: i32,
        end_to_end_deadline: i32,
        volume: i32,
        utilization: f32,
    }

    #[derive(Deserialize)]
    struct TestProcessorInfo {
        number_of_cores: usize,
    }

    #[derive(Deserialize)]
    struct TestNodeLogs {
        node_logs: Vec<TestNodeLog>,
    }

    #[derive(Deserialize)]
    struct TestNodeLog {
        core_id: usize,
        dag_id: usize, // Used to distinguish DAGs when the scheduler input is DAGSet
        node_id: usize,
        start_time: i32,
        finish_time: i32,
    }

    #[derive(Deserialize)]
    struct TestProcessorLog {
        average_utilization: f32,
        variance_utilization: f32,
        core_logs: Vec<TestCoreLog>,
    }

    #[derive(Deserialize)]
    struct TestCoreLog {
        core_id: usize,
        total_proc_time: i32,
        utilization: f32,
    }

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

        let file_path = fixed_priority_scheduler.dump_log("tests", "test");
        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let log: TestDAGSchedulerLog = serde_yaml::from_str(&file_contents).unwrap();

        assert_eq!(log.dag_info.critical_path_length, 92);
        assert_eq!(log.dag_info.period, 100);
        assert_eq!(log.dag_info.end_to_end_deadline, 0);
        assert_eq!(log.dag_info.volume, 114);
        assert_eq!(log.dag_info.utilization, 0.877193);

        assert_eq!(log.processor_info.number_of_cores, 2);

        assert_eq!(log.processor_log.average_utilization, 0.61956525);
        assert_eq!(log.processor_log.variance_utilization, 0.14473063);
        assert_eq!(log.processor_log.core_logs[0].core_id, 0);
        assert_eq!(log.processor_log.core_logs[0].total_proc_time, 92);
        assert_eq!(log.processor_log.core_logs[0].utilization, 1.0);

        assert_eq!(log.node_logs.node_logs[0].dag_id, 0);
        assert_eq!(log.node_logs.node_logs[0].node_id, 0);
        assert_eq!(log.node_logs.node_logs[0].core_id, 0);
        assert_eq!(log.node_logs.node_logs[0].start_time, 0);
        assert_eq!(log.node_logs.node_logs[0].finish_time, 52);

        remove_file(file_path).unwrap();
    }
}
