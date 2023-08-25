//! Homogeneous processor module. This module uses Core struct.
use crate::{core::Core, core::ProcessResult, graph_extension::NodeData, processor::ProcessorBase};

#[derive(Clone, Debug)]
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

    fn get_idle_core_num(&self) -> usize {
        self.cores.iter().filter(|core| core.get_is_idle()).count()
    }

    fn get_idle_core_index(&self) -> Option<usize> {
        for (index, core) in self.cores.iter().enumerate() {
            if core.get_is_idle() {
                return Some(index);
            }
        }
        None
    }

    fn preempt(&mut self, core_id: usize) -> Option<NodeData> {
        self.cores[core_id].preempt()
    }

    fn get_max_value_and_index(&self, key: &str) -> Option<(i32, usize)> {
        self.cores
            .iter()
            .enumerate()
            .filter_map(|(index, core)| {
                let node_data = core.get_processing_node().as_ref()?;
                let value = node_data.params.get(key)?;
                Some((*value, index))
            })
            .max_by_key(|&(value, _)| value)
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
    use std::collections::BTreeMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
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

    #[test]
    fn test_processor_get_idle_core_num_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        assert_eq!(homogeneous_processor.get_idle_core_num(), 2);

        let n1 = create_node(0, "execution_time", 2);

        homogeneous_processor.allocate_specific_core(0, &n1);

        assert_eq!(homogeneous_processor.get_idle_core_num(), 1);

        homogeneous_processor.allocate_specific_core(1, &n1);

        assert_eq!(homogeneous_processor.get_idle_core_num(), 0);
    }

    #[test]
    fn test_processor_preempt_execution_normal() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);

        let n0 = create_node(0, "execution_time", 2);
        let mut n1 = create_node(1, "execution_time", 2);

        homogeneous_processor.allocate_specific_core(0, &n0);
        homogeneous_processor.process();
        homogeneous_processor.allocate_specific_core(1, &n1);
        homogeneous_processor.process();

        assert_eq!(homogeneous_processor.preempt(0), None);

        n1 = homogeneous_processor.preempt(1).unwrap();

        assert_eq!(n1.params["execution_time"], 1);

        homogeneous_processor.allocate_specific_core(0, &n1);
        homogeneous_processor.process();

        assert_eq!(homogeneous_processor.preempt(0), None);
    }

    #[test]
    fn test_get_max_value_index() {
        let mut homogeneous_processor = HomogeneousProcessor::new(2);
        let n0 = create_node(0, "execution_time", 10);
        let mut n1 = create_node(1, "execution_time", 11);

        homogeneous_processor.allocate_specific_core(0, &n0);
        homogeneous_processor.allocate_specific_core(1, &n1);
        assert_eq!(
            homogeneous_processor.get_max_value_and_index("execution_time"),
            Some((11, 1))
        );

        n1 = homogeneous_processor.preempt(1).unwrap();
        assert_eq!(
            homogeneous_processor.get_max_value_and_index("execution_time"),
            Some((10, 0))
        );

        homogeneous_processor.allocate_specific_core(1, &n1);
        assert_eq!(
            homogeneous_processor.get_max_value_and_index("execution_time"),
            Some((11, 1))
        );
    }
}
