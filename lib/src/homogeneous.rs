use crate::core::Core;
use petgraph::graph::NodeIndex;

pub struct HomogeneousProcessor {
    cores: Vec<Core>,
}

impl HomogeneousProcessor {
    pub fn new(num_cores: usize) -> Self {
        let cores = (0..num_cores)
            .map(|core_id| Core::new(core_id as i32))
            .collect::<Vec<Core>>();
        Self { cores }
    }

    pub fn allocate(&mut self, core_id: usize, node_i: Option<NodeIndex>, exec_time: i32) {
        self.cores[core_id].allocate(node_i, exec_time);
    }

    pub fn process(&mut self) {
        for core in &mut self.cores {
            core.process();
        }
    }
}
