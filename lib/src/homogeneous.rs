//! Homogeneous processor module. This module uses Core struct.
use crate::{core::*, graph_extension::NodeData};

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

    pub fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool {
        self.cores[core_id].allocate(node_data)
    }

    pub fn process(&mut self) -> Vec<ProcessResult> {
        self.cores.iter_mut().map(|core| core.process()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_extension::NodeData;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
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

        assert!(processor.allocate(0, create_node(0, "execution_time", 2.0)));
        assert!(!processor.cores[0].is_idle);
        assert!(processor.cores[1].is_idle);
        assert!(processor.allocate(1, create_node(1, "execution_time", 2.0)));
    }

    #[test]
    fn test_homogeneous_processor_allocate_same_core() {
        let mut processor = HomogeneousProcessor::new(2);
        processor.allocate(0, create_node(0, "execution_time", 2.0));

        assert!(!processor.allocate(0, create_node(0, "execution_time", 2.0)));
    }

    #[test]
    #[should_panic]
    fn test_homogeneous_processor_allocate_no_exist_core() {
        let mut processor = HomogeneousProcessor::new(2);

        processor.allocate(2, create_node(0, "execution_time", 2.0));
    }

    #[test]
    fn test_homogeneous_processor_process_normal() {
        let mut processor = HomogeneousProcessor::new(2);
        processor.allocate(0, create_node(0, "execution_time", 2.0));
        processor.allocate(1, create_node(0, "execution_time", 3.0));

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
        processor.allocate(0, create_node(0, "execution_time", 2.0));

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
