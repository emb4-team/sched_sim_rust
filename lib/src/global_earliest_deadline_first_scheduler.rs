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

//test
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

    #[test]
    fn test_global_earliest_deadline_first_scheduler_schedule_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 52));
        let c1 = dag.add_node(create_node(1, "execution_time", 40));
        dag.add_param(c0, "priority", 0);
        dag.add_param(c0, "period", 100);
        dag.add_param(c1, "priority", 0);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(2, "execution_time", 12));
        let n1_0 = dag.add_node(create_node(3, "execution_time", 10));
        dag.add_param(n0_0, "priority", 2);
        dag.add_param(n1_0, "priority", 1);

        //create critical path edges
        dag.add_edge(c0, c1, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);

        dag.calculate_latest_finish_times();
        let mut global_earliest_deadline_first_scheduler =
            GlobalEarliestDeadlineFirstScheduler::new(&dag, &HomogeneousProcessor::new(2));
        let result = global_earliest_deadline_first_scheduler.schedule();

        assert_eq!(result.0, 102);

        assert_eq!(
            result.1,
            vec![
                NodeIndex::new(0),
                NodeIndex::new(3),
                NodeIndex::new(2),
                NodeIndex::new(1)
            ]
        );
    }
}
