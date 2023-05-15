//! Homogeneous processor module. This module uses Core struct.
use crate::{core::Core, core::ProcessResult, graph_extension::NodeData, processor::ProcessorBase};

pub struct HomogeneousProcessor {
    pub cores: Vec<Core>,
}

impl ProcessorBase for HomogeneousProcessor {
    fn new(num_cores: usize) -> Self {
        Self {
            cores: vec![Core::default(); num_cores],
        }
    }

    fn set_time_unit(&mut self, time_unit: f32) {
        for core in &mut self.cores {
            core.time_unit = time_unit;
        }
    }

    fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool {
        self.cores[core_id].allocate(node_data)
    }

    fn process(&mut self) -> Vec<ProcessResult> {
        self.cores.iter_mut().map(|core| core.process()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::ProcessResult, graph_extension::NodeData, processor::ProcessorBase};
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_processor_new() {
        let homogeneous_processor = HomogeneousProcessor::new(2);
        assert_eq!(homogeneous_processor.cores.len(), 2);
        for core in &homogeneous_processor.cores {
            assert!(core.is_idle);
            assert_eq!(core.processing_node, None);
            assert_eq!(core.remain_proc_time, 0.0);
        }
    }

    #[test]
    fn test_set_time_unit_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.set_time_unit(0.1);
        for core in &homogeneous_processor.cores {
            assert_eq!(core.time_unit, 0.1);
        }
    }

    #[test]
    fn test_processor_allocate_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert!(homogeneous_processor.allocate(0, create_node(0, "execution_time", 2.0)));
        assert!(!homogeneous_processor.cores[0].is_idle);
        assert!(homogeneous_processor.cores[1].is_idle);
        assert!(homogeneous_processor.allocate(1, create_node(1, "execution_time", 2.0)));
    }

    #[test]
    fn test_processor_allocate_same_core() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate(0, create_node(0, "execution_time", 2.0));

        assert!(!homogeneous_processor.allocate(0, create_node(0, "execution_time", 2.0)));
    }

    #[test]
    #[should_panic]
    fn test_processor_allocate_no_exist_core() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        homogeneous_processor.allocate(2, create_node(0, "execution_time", 2.0));
    }

    #[test]
    fn test_processor_process_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate(0, create_node(0, "execution_time", 2.0));
        homogeneous_processor.allocate(1, create_node(0, "execution_time", 3.0));

        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Continue]
        );
        assert_eq!(homogeneous_processor.cores[0].remain_proc_time, 1.0);
        assert_eq!(homogeneous_processor.cores[1].remain_proc_time, 2.0);
    }

    #[test]
    fn test_processor_process_when_one_core_no_allocated() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate(0, create_node(0, "execution_time", 2.0));

        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Idle]
        );
        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Done, ProcessResult::Idle]
        );
        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Idle, ProcessResult::Idle]
        );
    }

    #[test]
    fn test_processor_process_when_processor_no_allocated() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Idle, ProcessResult::Idle]
        );
    }
}
