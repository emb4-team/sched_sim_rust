use crate::{core::*, graph_extension::NodeData};

pub trait ProcessorBase {
    fn new(num_cores: usize) -> Self;
    fn allocate_specific_core(&mut self, core_id: usize, node_data: &NodeData) -> bool;
    fn process(&mut self) -> Vec<ProcessResult>;
    fn get_number_of_cores(&self) -> usize;
    fn get_idle_core_index(&self) -> Option<usize>;
    fn get_idle_core_num(&self) -> usize;
    fn preempt(&mut self, core_id: usize) -> Option<NodeData>;
    fn get_max_value_and_index(&self, key: &str) -> Option<(i32, usize)>;
}
