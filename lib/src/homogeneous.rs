//! Homogeneous processor module. This module uses Core struct.
use crate::core::*;
use petgraph::graph::NodeIndex;

pub struct HomogeneousProcessor {
    cores: Vec<Core>,
}

impl HomogeneousProcessor {
    pub fn new(num_cores: usize) -> Self {
        let cores = vec![Core::default(); num_cores];
        Self { cores }
    }

    pub fn set_time_unit(&mut self, time_unit: f32) {
        for core in &mut self.cores {
            core.time_unit = time_unit;
        }
    }

    pub fn allocate(&mut self, core_id: usize, node_i: NodeIndex, exec_time: f32) -> bool {
        self.cores[core_id].allocate(node_i, exec_time)
    }

    pub fn process(&mut self) -> Vec<ProcessResult> {
        self.cores.iter_mut().map(|core| core.process()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_extension::NodeData;
    use petgraph::Graph;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_sample_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        dag.add_node(create_node(0, "execution_time", 2.0));
        dag.add_node(create_node(1, "execution_time", 3.0));
        dag
    }

    #[test]
    fn test_homogeneous_processor_new() {
        let processor = HomogeneousProcessor::new(2);
        assert_eq!(processor.cores.len(), 2);
        for core in &processor.cores {
            assert!(core.is_idle);
            assert_eq!(core.processing_node, None);
            assert_eq!(core.remain_proc_time, 0.0);
        }
    }

    #[test]
    fn test_set_time_unit_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        processor.set_time_unit(0.1);
        for core in &processor.cores {
            assert_eq!(core.time_unit, 0.1);
        }
    }

    #[test]
    fn test_homogeneous_processor_allocate_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_sample_dag();
        let n0 = dag.node_indices().next().unwrap();
        let n1 = dag.node_indices().nth(1).unwrap();

        assert!(processor.allocate(0, n0, dag[n0].params["execution_time"]));
        assert!(!processor.cores[0].is_idle);
        assert!(processor.cores[1].is_idle);
        assert!(processor.allocate(1, n1, dag[n1].params["execution_time"]));
    }

    #[test]
    fn test_homogeneous_processor_allocate_same_core() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_sample_dag();
        let n0 = dag.node_indices().next().unwrap();
        let n1 = dag.node_indices().nth(1).unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        assert!(!processor.allocate(0, n1, dag[n1].params["execution_time"]));
    }

    #[test]
    #[should_panic]
    fn test_homogeneous_processor_allocate_no_exist_core() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_sample_dag();
        let n0 = dag.node_indices().next().unwrap();

        processor.allocate(2, n0, dag[n0].params["execution_time"]);
    }

    #[test]
    fn test_homogeneous_processor_process_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_sample_dag();
        let n0 = dag.node_indices().next().unwrap();
        let n1 = dag.node_indices().nth(1).unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        processor.allocate(1, n1, dag[n1].params["execution_time"]);
        assert_eq!(
            processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Continue]
        );
        assert_eq!(processor.cores[0].remain_proc_time, 1.0);
        assert_eq!(processor.cores[1].remain_proc_time, 2.0);
    }

    #[test]
    fn test_homogeneous_processor_process_when_one_core_no_allocated() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_sample_dag();
        let n0 = dag.node_indices().next().unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        assert_eq!(
            processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Idle]
        );
        assert_eq!(
            processor.process(),
            vec![ProcessResult::Done, ProcessResult::Idle]
        );
        assert_eq!(
            processor.process(),
            vec![ProcessResult::Idle, ProcessResult::Idle]
        );
    }

    #[test]
    fn test_homogeneous_processor_process_when_processor_no_allocated() {
        let mut processor = HomogeneousProcessor::new(2);

        assert_eq!(
            processor.process(),
            vec![ProcessResult::Idle, ProcessResult::Idle]
        );
    }
}
