//! This module contains the definition of the core and the process result enum
use crate::core::ProcessResult::*;
use petgraph::graph::NodeIndex;

///enum to represent three types of states
///execution not possible because not allocate, execution in progress, execution finished
#[derive(Debug, PartialEq)]
pub enum ProcessResult {
    False,
    Continue,
    Done,
}

pub struct Core {
    is_idle: bool,
    processing_node: Option<NodeIndex>,
    remain_proc_time: i32,
}

impl Default for Core {
    fn default() -> Self {
        Self {
            is_idle: true,
            processing_node: None,
            remain_proc_time: 0,
        }
    }
}

///return bool since "panic!" would terminate
impl Core {
    pub fn allocate(&mut self, node_i: NodeIndex, exec_time: i32) -> bool {
        if !self.is_idle {
            println!("Core is already allocated to a node");
            return false;
        }
        self.is_idle = false;
        self.processing_node = Some(node_i);
        self.remain_proc_time = exec_time;
        true
    }

    pub fn process(&mut self) -> ProcessResult {
        if self.is_idle {
            return False;
        }
        self.remain_proc_time -= 1;
        if self.remain_proc_time == 0 {
            self.is_idle = true;
            self.processing_node = None;
            return Done;
        }
        Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_default_params() {
        let core = Core::default();
        assert!(core.is_idle);
        assert_eq!(core.processing_node, None);
        assert_eq!(core.remain_proc_time, 0);
    }

    #[test]
    fn test_core_allocate_normal() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10);
        assert!(!core.is_idle);
        assert_eq!(core.processing_node, Some(NodeIndex::new(0)));
        assert_eq!(core.remain_proc_time, 10);
    }

    #[test]
    fn test_core_allocate_already_allocated() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10);
        assert!(!core.allocate(NodeIndex::new(1), 10));
    }

    #[test]
    fn test_core_process_normal() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10);
        assert_eq!(core.process(), Continue);
        assert_eq!(core.remain_proc_time, 9);
        core.process();
        assert_eq!(core.remain_proc_time, 8);
    }

    #[test]
    fn test_core_process_no_allocated() {
        let mut core = Core::default();
        assert_eq!(core.process(), False);
    }

    #[test]
    fn test_core_process_when_finished() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 2);
        core.process();
        assert_eq!(core.process(), Done);
        assert!(core.is_idle);
        assert_eq!(core.processing_node, None);
        assert_eq!(core.remain_proc_time, 0);
    }
}
