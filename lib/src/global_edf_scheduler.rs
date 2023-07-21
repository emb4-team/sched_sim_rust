use petgraph::Graph;

use crate::{
    graph_extension::NodeData, homogeneous::HomogeneousProcessor, log::DAGSetSchedulerLog,
    processor::ProcessorBase, scheduler::DAGSetSchedulerBase,
};

pub struct GlobalEDFScheduler {
    dag_set: Vec<Graph<NodeData, i32>>,
    processor: HomogeneousProcessor,
    log: DAGSetSchedulerLog,
}

impl DAGSetSchedulerBase<HomogeneousProcessor> for GlobalEDFScheduler {
    fn new(dag_set: &[Graph<NodeData, i32>], processor: &HomogeneousProcessor) -> Self {
        Self {
            dag_set: dag_set.to_vec(),
            processor: processor.clone(),
            log: DAGSetSchedulerLog::new(dag_set, processor.get_number_of_cores()),
        }
    }

    fn schedule(&mut self) -> i32 {
        todo!()
    }

    fn get_log(&self) -> DAGSetSchedulerLog {
        self.log.clone()
    }

    fn set_log(&mut self, log: DAGSetSchedulerLog) {
        self.log = log;
    }
}
