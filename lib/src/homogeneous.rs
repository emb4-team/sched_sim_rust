use crate::core::Core;
use crate::core::ProcessResult;
use petgraph::graph::NodeIndex;

pub struct HomogeneousProcessor {
    cores: Vec<Core>,
}

impl HomogeneousProcessor {
    pub fn new(num_cores: usize) -> Self {
        let cores = (0..num_cores)
            .map(|_| Core::default())
            .collect::<Vec<Core>>();
        Self { cores }
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

    fn create_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        dag.add_node(create_node(0, "execution_time", 2.0));
        dag.add_node(create_node(1, "execution_time", 3.0));
        dag
    }

    #[test]
    fn test_homogeneous_processor_new() {
        let processor = HomogeneousProcessor::new(2);
        assert_eq!(processor.cores.len(), 2);
        assert!(processor.cores[0].is_idle);
        assert!(processor.cores[1].is_idle);
        assert_eq!(processor.cores[0].processing_node, None);
        assert_eq!(processor.cores[1].processing_node, None);
        assert_eq!(processor.cores[0].remain_proc_time, 0.0);
        assert_eq!(processor.cores[1].remain_proc_time, 0.0);
    }

    #[test]
    fn test_homogeneous_processor_allocate_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_dag();
        let mut node_indices = dag.node_indices();
        let n0 = node_indices.next().unwrap();
        let n1 = node_indices.next().unwrap();

        assert!(processor.allocate(0, n0, dag[n0].params["execution_time"]));
        assert!(processor.cores[1].is_idle);
        assert!(processor.allocate(1, n1, dag[n1].params["execution_time"]));
        assert!(!processor.cores[0].is_idle);
    }

    #[test]
    fn test_homogeneous_processor_allocate_same_core() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_dag();
        let mut node_indices = dag.node_indices();
        let n0 = node_indices.next().unwrap();
        let n1 = node_indices.next().unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        assert!(!processor.allocate(0, n1, dag[n1].params["execution_time"]));
    }

    #[test]
    #[should_panic]
    fn test_homogeneous_processor_allocate_no_exist_core() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_dag();
        let mut node_indices = dag.node_indices();
        let n0 = node_indices.next().unwrap();

        processor.allocate(2, n0, dag[n0].params["execution_time"]);
    }

    #[test]
    fn test_homogeneous_processor_process_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_dag();
        let mut node_indices = dag.node_indices();
        let n0 = node_indices.next().unwrap();
        let n1 = node_indices.next().unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        processor.allocate(1, n1, dag[n1].params["execution_time"]);
        let result = processor.process();
        assert_eq!(
            result,
            vec![ProcessResult::Continue, ProcessResult::Continue]
        );
        assert_eq!(processor.cores[0].remain_proc_time, 1.0);
        assert_eq!(processor.cores[1].remain_proc_time, 2.0);
    }

    #[test]
    fn test_homogeneous_processor_process_when_one_core_no_allocated() {
        let mut processor = HomogeneousProcessor::new(2);
        let dag = create_dag();
        let mut node_indices = dag.node_indices();
        let n0 = node_indices.next().unwrap();

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        let mut result = processor.process();
        assert_eq!(result, vec![ProcessResult::Continue, ProcessResult::Idle]);
        result = processor.process();
        assert_eq!(result, vec![ProcessResult::Done, ProcessResult::Idle]);
        result = processor.process();
        assert_eq!(result, vec![ProcessResult::Idle, ProcessResult::Idle]);
    }

    #[test]
    fn test_homogeneous_processor_process_when_processor_no_allocated() {
        let mut processor = HomogeneousProcessor::new(2);

        let result = processor.process();
        assert_eq!(result, vec![ProcessResult::Idle, ProcessResult::Idle]);
    }
}
