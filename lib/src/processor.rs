use crate::{core::*, graph_extension::NodeData};

pub struct Processor {
    pub cores: Vec<Core>,
}

pub trait ProcessorBase {
    fn set_time_unit(&mut self, time_unit: f32);
    fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool;
    fn process(&mut self) -> Vec<ProcessResult>;
}

impl ProcessorBase for Processor {
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
