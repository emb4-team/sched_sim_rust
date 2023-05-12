use crate::{core::*, graph_extension::NodeData};

pub struct Processor {
    cores: Vec<Core>,
}

impl Processor {
    pub fn new(num_cores: usize) -> Self {
        let cores = vec![Core::default(); num_cores];
        Self { cores }
    }

    pub fn set_time_unit(&mut self, time_unit: f32) {
        for core in &mut self.cores {
            core.time_unit = time_unit;
        }
    }

    pub fn allocate(&mut self, core_id: usize, node_data: NodeData) -> bool {
        self.cores[core_id].allocate(node_data)
    }

    pub fn process(&mut self) -> Vec<ProcessResult> {
        self.cores.iter_mut().map(|core| core.process()).collect()
    }
}
