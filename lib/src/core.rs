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
    idle: bool,
    process_node: Option<NodeIndex>,
    remain_proc_time: i32,
}

impl Default for Core {
    fn default() -> Self {
        Self {
            idle: true,
            process_node: None,
            remain_proc_time: 0,
        }
    }
}

///return bool since "panic!" would terminate
impl Core {
    pub fn allocate(&mut self, node_i: Option<NodeIndex>, exec_time: i32) -> bool {
        if !self.idle {
            return false;
        }
        self.idle = false;
        self.process_node = node_i;
        self.remain_proc_time = exec_time;
        true
    }

    pub fn process(&mut self) -> ProcessResult {
        if self.idle {
            return False;
        }
        self.remain_proc_time -= 1;
        if self.remain_proc_time == 0 {
            self.idle = true;
            self.process_node = None;
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
        assert!(core.idle);
        assert_eq!(core.process_node, None);
        assert_eq!(core.remain_proc_time, 0);
    }

    #[test]
    fn test_core_allocate_normal() {
        let mut core = Core::default();
        core.allocate(Some(NodeIndex::new(0)), 10);
        assert!(!core.idle);
        assert_eq!(core.process_node, Some(NodeIndex::new(0)));
        assert_eq!(core.remain_proc_time, 10);
    }

    #[test]
    fn test_core_allocate_already_allocated() {
        let mut core = Core::default();
        core.allocate(Some(NodeIndex::new(0)), 10);
        assert!(!core.allocate(Some(NodeIndex::new(1)), 10));
    }

    #[test]
    fn test_core_process_normal() {
        let mut core = Core::default();
        core.allocate(Some(NodeIndex::new(0)), 10);
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
        core.allocate(Some(NodeIndex::new(0)), 2);
        core.process();
        assert_eq!(core.process(), Done);
        assert!(core.idle);
        assert_eq!(core.process_node, None);
        assert_eq!(core.remain_proc_time, 0);
    }
}
