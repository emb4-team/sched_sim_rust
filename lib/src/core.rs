//! This module contains the definition of the core and the process result enum
use crate::{core::ProcessResult::*, graph_extension::NodeData};
use log::warn;
///enum to represent three types of states
///execution not possible because not allocate, execution in progress, execution finished
#[derive(Debug, PartialEq, Clone)]
pub enum ProcessResult {
    Idle,
    Continue,
    Done(NodeData),
}

#[derive(Clone)]
pub struct Core {
    pub is_idle: bool,
    pub processing_node: Option<NodeData>,
    pub remain_proc_time: i32,
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
    pub fn allocate(&mut self, node_data: &NodeData) -> bool {
        if !self.is_idle {
            warn!("Core is already allocated to a node");
            return false;
        }
        self.is_idle = false;
        self.processing_node = Some(node_data.clone());
        if let Some(exec_time) = node_data.params.get("execution_time") {
            self.remain_proc_time = *exec_time;
            true
        } else {
            warn!("Node {} does not have execution_time", node_data.id);
            false
        }
    }

    pub fn process(&mut self) -> ProcessResult {
        if self.is_idle {
            return Idle;
        }
        self.remain_proc_time -= 1;
        if self.remain_proc_time == 0 {
            self.is_idle = true;
            let finish_node_data = self.processing_node.clone().unwrap();
            self.processing_node = None;
            return Done(finish_node_data);
        }
        Continue
    }

    pub fn return_allocated_node_data(&mut self) -> Option<NodeData> {
        if self.is_idle {
            None
        } else {
            let mut node_data = self.processing_node.clone().unwrap();
            node_data
                .params
                .insert("execution_time".to_string(), self.remain_proc_time);
            self.processing_node = None;
            self.remain_proc_time = 0;
            self.is_idle = true;
            Some(node_data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

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
        core.allocate(&create_node(0, "execution_time", 10));
        assert!(!core.is_idle);
        assert_eq!(
            core.processing_node,
            Some(create_node(0, "execution_time", 10))
        );
        assert_eq!(core.remain_proc_time, 10);
    }

    #[test]
    fn test_core_allocate_already_allocated() {
        let mut core = Core::default();
        core.allocate(&create_node(0, "execution_time", 10));
        assert!(!core.allocate(&create_node(1, "execution_time", 10)));
    }

    #[test]
    fn test_core_allocate_node_no_has_execution_time() {
        let mut core = Core::default();
        assert!(!core.allocate(&create_node(0, "no_execution_time", 10)));
    }

    #[test]
    fn test_core_process_normal() {
        let mut core = Core::default();
        core.allocate(&create_node(0, "execution_time", 10));
        assert_eq!(core.process(), Continue);
        assert_eq!(core.remain_proc_time, 9);
        core.process();
        assert_eq!(core.remain_proc_time, 8);
    }

    #[test]
    fn test_core_process_no_allocated() {
        let mut core = Core::default();
        assert_eq!(core.process(), Idle);
    }

    #[test]
    fn test_core_process_when_finished() {
        let mut core = Core::default();
        core.allocate(&create_node(0, "execution_time", 2));
        core.process();
        assert_eq!(core.process(), Done(create_node(0, "execution_time", 2)));
        assert!(core.is_idle);
        assert_eq!(core.processing_node, None);
        assert_eq!(core.remain_proc_time, 0);
    }
}
