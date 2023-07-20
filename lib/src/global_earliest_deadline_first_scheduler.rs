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
        if !ready_queue.is_empty()
            && ready_queue
                .front()
                .unwrap()
                .params
                .contains_key("deadline_factor")
        {
            ready_queue.make_contiguous().sort_by_key(|node| {
                *node.params.get("earliest_start_time").unwrap()
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    //use std::fs::remove_file;

    use super::*;
    use crate::graph_extension::GraphExtension;
    use crate::homogeneous::HomogeneousProcessor;
    use crate::processor::ProcessorBase;
    //use crate::util::load_yaml;
    use petgraph::graph::{Graph, NodeIndex};

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag(period: i32) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_param(n0, "period", period);
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        dag
    }

    #[test]
    fn test_global_earliest_deadline_first_scheduler_schedule_normal() {
        let mut dag = create_sample_dag(120);

        dag.add_dummy_source_node();
        dag.add_dummy_sink_node();
        dag.calculate_latest_finish_times();
        dag.remove_dummy_source_node();
        dag.remove_dummy_sink_node();
        let mut global_earliest_deadline_first_scheduler =
            GlobalEarliestDeadlineFirstScheduler::new(&dag, &HomogeneousProcessor::new(2));
        let result = global_earliest_deadline_first_scheduler.schedule();

        assert_eq!(result.0, 113);

        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(2),
                NodeIndex::new(1),
                NodeIndex::new(3),
                NodeIndex::new(4)
            ]
        );
    }
}
