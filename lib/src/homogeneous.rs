//! Homogeneous processor module. This module uses Core struct.
use crate::{core::Core, core::ProcessResult, graph_extension::NodeData, processor::ProcessorBase};

#[derive(Clone)]
pub struct HomogeneousProcessor {
    pub cores: Vec<Core>,
}

impl ProcessorBase for HomogeneousProcessor {
    fn new(num_cores: usize) -> Self {
        Self {
            cores: vec![Core::default(); num_cores],
        }
    }

    fn allocate_specific_core(&mut self, core_id: usize, node_data: &NodeData) -> bool {
        self.cores[core_id].allocate(node_data)
    }

    fn process(&mut self) -> Vec<ProcessResult> {
        self.cores.iter_mut().map(|core| core.process()).collect()
    }

    fn get_number_of_cores(&self) -> usize {
        self.cores.len()
    }

    fn get_idle_core_index(&self) -> Option<usize> {
        for (index, core) in self.cores.iter().enumerate() {
            if core.is_idle {
                return Some(index);
            }
        }
        None
    }
}

impl HomogeneousProcessor {
    pub fn allocate_any_idle_core(&mut self, node_data: &NodeData) -> bool {
        if let Some(idle_core_i) = self.get_idle_core_index() {
            self.cores[idle_core_i].allocate(node_data)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::ProcessResult, graph_extension::NodeData, processor::ProcessorBase};
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
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
            assert_eq!(core.remain_proc_time, 0);
        }
    }

    #[test]
    fn test_processor_allocate_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert!(
            homogeneous_processor.allocate_specific_core(0, &create_node(0, "execution_time", 2))
        );
        assert!(!homogeneous_processor.cores[0].is_idle);
        assert!(homogeneous_processor.cores[1].is_idle);
        assert!(
            homogeneous_processor.allocate_specific_core(1, &create_node(1, "execution_time", 2))
        );
    }

    #[test]
    fn test_processor_allocate_same_core() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate_specific_core(0, &create_node(0, "execution_time", 2));

        assert!(
            !homogeneous_processor.allocate_specific_core(0, &create_node(0, "execution_time", 2))
        );
    }

    #[test]
    #[should_panic]
    fn test_processor_allocate_no_exist_core() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        homogeneous_processor.allocate_specific_core(3, &create_node(0, "execution_time", 2));
    }

    #[test]
    fn test_processor_allocate_idle_core_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert!(homogeneous_processor.allocate_any_idle_core(&create_node(0, "execution_time", 2)));
        assert!(!homogeneous_processor.cores[0].is_idle);
        assert!(homogeneous_processor.cores[1].is_idle);
        assert!(homogeneous_processor.allocate_any_idle_core(&create_node(1, "execution_time", 2)));
        assert!(!homogeneous_processor.cores[1].is_idle);
        assert!(!homogeneous_processor.allocate_any_idle_core(&create_node(
            2,
            "execution_time",
            2
        )));
    }

    #[test]
    fn test_processor_process_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate_specific_core(0, &create_node(0, "execution_time", 2));
        homogeneous_processor.allocate_specific_core(1, &create_node(0, "execution_time", 3));

        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Continue]
        );
        assert_eq!(homogeneous_processor.cores[0].remain_proc_time, 1);
        assert_eq!(homogeneous_processor.cores[1].remain_proc_time, 2);
    }

    #[test]
    fn test_processor_process_when_one_core_no_allocated() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        homogeneous_processor.allocate_specific_core(0, &create_node(0, "execution_time", 2));

        assert_eq!(
            homogeneous_processor.process(),
            vec![ProcessResult::Continue, ProcessResult::Idle]
        );
        assert_eq!(
            homogeneous_processor.process(),
            vec![
                ProcessResult::Done(create_node(0, "execution_time", 2)),
                ProcessResult::Idle
            ]
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

    #[test]
    fn test_processor_get_idle_core_index_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert_eq!(homogeneous_processor.get_idle_core_index(), Some(0));

        let n1 = create_node(0, "execution_time", 2);

        homogeneous_processor.allocate_specific_core(0, &n1);

        assert_eq!(homogeneous_processor.get_idle_core_index(), Some(1));

        homogeneous_processor.allocate_specific_core(1, &n1);

        assert_eq!(homogeneous_processor.get_idle_core_index(), None);
    }
}
