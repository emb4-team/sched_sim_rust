//! This module contains the definition of the core and the process result enum
use crate::{core::ProcessResult::*, graph_extension::NodeData};
use log::warn;
use petgraph::{graph::NodeIndex, Graph};

pub fn get_minimum_time_unit_from_dag_set(dag_set: &Vec<Graph<NodeData, f32>>) -> f32 {
    // Initial value is set to 1.0 and returned as is if no decimal point is found.
    let mut time_unit = 1.0;
    for dag in dag_set {
        for node_index in dag.node_indices() {
            let exec_time = &dag[node_index].params["execution_time"];
            // Check all nodes in dag_set because we need to use the same unit time in the processor
            if exec_time.fract() != 0.0 {
                // -2 to remove "0." from the string
                let decimal_count =
                    10u32.pow((exec_time.fract().abs().to_string().chars().count() - 2) as u32);
                let new_time_unit = 1.0 / decimal_count as f32;
                if new_time_unit < time_unit {
                    time_unit = new_time_unit;
                }
            }
        }
    }
    time_unit
}

///enum to represent three types of states
///execution not possible because not allocate, execution in progress, execution finished
#[derive(Debug, PartialEq)]
pub enum ProcessResult {
    Idle,
    Continue,
    Done,
}

#[derive(Clone)]
pub struct Core {
    pub is_idle: bool,
    pub processing_node: Option<NodeIndex>,
    pub remain_proc_time: f32,
    pub time_unit: f32,
}

impl Default for Core {
    fn default() -> Self {
        Self {
            is_idle: true,
            processing_node: None,
            remain_proc_time: 0.0,
            time_unit: 1.0,
        }
    }
}

///return bool since "panic!" would terminate
impl Core {
    pub fn allocate(&mut self, node_i: NodeIndex, exec_time: f32) -> bool {
        if !self.is_idle {
            warn!("Core is already allocated to a node");
            return false;
        }
        self.is_idle = false;
        self.processing_node = Some(node_i);
        self.remain_proc_time = exec_time;
        true
    }

    pub fn process(&mut self) -> ProcessResult {
        if self.is_idle {
            return Idle;
        }
        self.remain_proc_time -= self.time_unit;
        if self.remain_proc_time == 0.0 {
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
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_get_minimum_time_unit_from_dag_set() {
        let mut dag0 = Graph::<NodeData, f32>::new();
        dag0.add_node(create_node(0, "execution_time", 30.0));

        let mut dag_set = vec![dag0];
        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 1.0);

        let mut dag1 = Graph::<NodeData, f32>::new();
        dag1.add_node(create_node(1, "execution_time", 3.0));
        dag_set.push(dag1);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 1.0);

        let mut dag2 = Graph::<NodeData, f32>::new();
        dag2.add_node(create_node(2, "execution_time", 0.3));
        dag_set.push(dag2);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 0.1);

        let mut dag3 = Graph::<NodeData, f32>::new();
        dag3.add_node(create_node(3, "execution_time", 0.03));
        dag_set.push(dag3);

        assert_eq!(get_minimum_time_unit_from_dag_set(&dag_set), 0.01);
    }

    #[test]
    fn test_core_default_params() {
        let core = Core::default();
        assert!(core.is_idle);
        assert_eq!(core.processing_node, None);
        assert_eq!(core.remain_proc_time, 0.0);
    }

    #[test]
    fn test_core_allocate_normal() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10.0);
        assert!(!core.is_idle);
        assert_eq!(core.processing_node, Some(NodeIndex::new(0)));
        assert_eq!(core.remain_proc_time, 10.0);
    }

    #[test]
    fn test_core_allocate_already_allocated() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10.0);
        assert!(!core.allocate(NodeIndex::new(1), 10.0));
    }

    #[test]
    fn test_core_process_normal() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 10.0);
        assert_eq!(core.process(), Continue);
        assert_eq!(core.remain_proc_time, 9.0);
        core.process();
        assert_eq!(core.remain_proc_time, 8.0);
    }

    #[test]
    fn test_core_process_no_allocated() {
        let mut core = Core::default();
        assert_eq!(core.process(), Idle);
    }

    #[test]
    fn test_core_process_when_finished() {
        let mut core = Core::default();
        core.allocate(NodeIndex::new(0), 2.0);
        core.process();
        assert_eq!(core.process(), Done);
        assert!(core.is_idle);
        assert_eq!(core.processing_node, None);
        assert_eq!(core.remain_proc_time, 0.0);
    }
}
