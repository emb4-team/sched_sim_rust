use crate::core::Core;
use petgraph::graph::NodeIndex;

pub struct HomogeneousProcessor {
    cores: Vec<Core>,
}

impl HomogeneousProcessor {
    pub fn new(num_cores: usize) -> Self {
        let cores = (0..num_cores)
            .map(|_| Core::default())
            .collect::<Vec<Core>>();
        Self { cores }
    }

    pub fn allocate(&mut self, core_id: usize, node_i: NodeIndex, exec_time: f32) {
        self.cores[core_id].allocate(node_i, exec_time);
    }

    pub fn process(&mut self) {
        for core in &mut self.cores {
            core.process();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_extension::NodeData;
    use petgraph::Graph;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_homogeneous_processor_allocate() {
        let mut dag = Graph::<NodeData, ()>::new();
        let mut processor = HomogeneousProcessor::new(2);
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 7.0));

        processor.allocate(0, n0, dag[n0].params["execution_time"]);
        processor.allocate(1, n1, dag[n1].params["execution_time"]);
    }
}
