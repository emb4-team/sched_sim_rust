use petgraph::Graph;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    graph_extension::{GraphExtension, NodeData},
    homogeneous::HomogeneousProcessor,
    log::DAGSetSchedulerLog,
    processor::ProcessorBase,
    scheduler::DAGSetSchedulerBase,
    util::get_hyper_period,
};

// Define a new wrapper type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDataWrapper(NodeData);

impl NodeDataWrapper {
    fn new(id: i32, params: BTreeMap<String, i32>) -> Self {
        Self(NodeData::new(id, params))
    }

    fn get_node_data(&self) -> NodeData {
        self.0.clone()
    }
}

impl PartialOrd for NodeDataWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Define the key to compare
        let key = "period";
        match (self.0.params.get(key), other.0.params.get(key)) {
            (Some(self_val), Some(other_val)) => match self_val.cmp(other_val) {
                // If the keys are equal, compare by id
                Ordering::Equal => self.0.id.partial_cmp(&other.0.id),
                other => Some(other),
            },
            // If neither of the keys exists, compare by id
            (None, None) => self.0.id.partial_cmp(&other.0.id),
            // If only one of the keys exists, the one with the key is greater
            (Some(_), None) => Some(Ordering::Greater),
            (None, Some(_)) => Some(Ordering::Less),
        }
    }
}

impl Ord for NodeDataWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[derive(Clone, Debug)]
struct DAGStateManager {
    is_released: bool,
    is_started: bool,
    release_count: i32,
}

impl DAGStateManager {
    fn new() -> Self {
        Self {
            is_released: false,
            is_started: false,
            release_count: 0,
        }
    }

    fn release(&mut self) {
        self.is_released = true;
    }

    fn get_is_released(&self) -> bool {
        self.is_released
    }

    fn start(&mut self) {
        self.is_started = true;
    }

    fn get_is_started(&self) -> bool {
        self.is_started
    }

    fn reset_state(&mut self) {
        self.is_started = false;
    }

    fn get_release_count(&self) -> i32 {
        self.release_count
    }

    fn increment_release_count(&mut self) {
        self.release_count += 1;
    }
}

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
        // Initialize DAGStateManagers
        let mut dag_state_managers = vec![DAGStateManager::new(); self.dag_set.len()];

        // Initialize DAG id
        for (dag_id, dag) in self.dag_set.iter_mut().enumerate() {
            dag.set_dag_id(dag_id);
        }

        // Start scheduling
        let mut current_time = 0;
        let mut ready_queue: BTreeSet<NodeDataWrapper> = BTreeSet::new();
        let mut log = DAGSetSchedulerLog::new(&self.dag_set, self.processor.get_number_of_cores());
        let hyper_period = get_hyper_period(&self.dag_set);
        while current_time < hyper_period {
            // release DAGs
            for dag in self.dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if current_time
                    == dag.get_head_offset()
                        + dag.get_head_period().unwrap()
                            * dag_state_managers[dag_id].get_release_count()
                {
                    dag_state_managers[dag_id].release();
                    dag_state_managers[dag_id].increment_release_count();
                    log.write_dag_release_time(dag_id, current_time);
                }
            }

            // Start DAGs if there are free cores
            let mut idle_core_num = self.processor.get_idle_core_num();
            for (dag_id, manager) in dag_state_managers.iter_mut().enumerate() {
                if idle_core_num > 0 && !manager.get_is_started() && manager.get_is_released() {
                    manager.start();
                    idle_core_num -= 1;
                    // Add the source node to the ready queue
                    let dag = &self.dag_set[dag_id];
                    let source_node = &dag[dag.get_source_nodes()[0]];
                    ready_queue.insert(NodeDataWrapper::new(
                        source_node.id,
                        source_node.params.clone(),
                    ));
                };
            }

            // Allocate the nodes of each DAG
            for dag in self.dag_set.iter_mut() {
                let dag_id = dag.get_dag_id();
                if !dag_state_managers[dag_id].get_is_started() {
                    continue;
                }
                match self.processor.get_idle_core_index() {
                    Some(idle_core_index) => match ready_queue.pop_first() {
                        Some(ready_node) => self
                            .processor
                            .allocate_specific_core(idle_core_index, &ready_node.get_node_data()),
                        None => break,
                    },
                    None => break,
                };
            }

            current_time += 1;
        }
        todo!()
    }

    fn get_log(&self) -> DAGSetSchedulerLog {
        self.log.clone()
    }

    fn set_log(&mut self, log: DAGSetSchedulerLog) {
        self.log = log;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 150);
        dag.add_param(c2, "end_to_end_deadline", 50);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));
        let n1_0 = dag.add_node(create_node(4, "execution_time", 10));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(c0, n1_0, 1);
        dag.add_edge(n0_0, c2, 1);
        dag.add_edge(n1_0, c2, 1);

        dag
    }

    fn create_sample_dag2() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 20));
        let c2 = dag.add_node(create_node(2, "execution_time", 20));
        dag.add_param(c0, "period", 100);
        dag.add_param(c2, "end_to_end_deadline", 60);
        //nY_X is the Yth suc node of cX.
        let n0_0 = dag.add_node(create_node(3, "execution_time", 10));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_0, 1);
        dag.add_edge(n0_0, c2, 1);

        dag
    }

    #[test]
    fn test_dynfed_normal() {
        let dag = create_sample_dag();
        let dag2 = create_sample_dag2();
        let dag_set = vec![dag, dag2];

        let processor = HomogeneousProcessor::new(3);

        let mut global_edf_scheduler = GlobalEDFScheduler::new(&dag_set, &processor);
        let result = global_edf_scheduler.schedule();
    }
}
