use crate::{core::*, graph_extension::NodeData};

pub trait ProcessorBase {
    fn new(num_cores: usize) -> Self;
    fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool;
    fn process(&mut self) -> Vec<ProcessResult>;
    fn get_number_of_cores(&self) -> usize;
    fn get_idle_core_index(&mut self) -> Option<usize>;
}
