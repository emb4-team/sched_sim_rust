use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f32;

/// custom node data structure for dag nodes (petgraph)
#[derive(Debug, Clone)]
pub struct NodeData {
    pub id: i32,
    pub params: HashMap<String, f32>,
}

impl NodeData {
    pub fn new(id: i32, key: String, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key, value);
        NodeData { id, params }
    }
}

pub trait GraphExt {
    fn add_dummy_source_node(&mut self);
    fn add_dummy_sink_node(&mut self);
    fn remove_dummy_source_node(&mut self);
    fn remove_dummy_sink_node(&mut self);
    fn calculate_earliest_start_times(&mut self) -> Vec<f32>;
    fn calculate_latest_start_times(&mut self) -> Vec<f32>;
    fn get_critical_paths(&mut self) -> Vec<Vec<NodeIndex>>;
}

impl GraphExt for Graph<NodeData, f32> {
    fn add_dummy_source_node(&mut self) {
        for node_index in self.node_indices() {
            if self[node_index].id == -1 {
                panic!("The dummy source node has already been added.");
            }
        }
        let dummy_node = self.add_node(NodeData::new(-1, "execution_time".to_owned(), 0.0));
        let nodes = self
            .node_indices()
            .filter(|&i| self.edges_directed(i, Incoming).next().is_none())
            .collect::<Vec<_>>();

        for node_index in nodes {
            if node_index != dummy_node {
                self.add_edge(dummy_node, node_index, 0.0);
            }
        }
    }
    fn add_dummy_sink_node(&mut self) {
        for node_index in self.node_indices() {
            if self[node_index].id == -2 {
                panic!("The dummy sink node has already been added.");
            }
        }
        let dummy_node = self.add_node(NodeData::new(-2, "execution_time".to_owned(), 0.0));
        let nodes = self
            .node_indices()
            .filter(|&i| self.edges_directed(i, Outgoing).next().is_none())
            .collect::<Vec<_>>();

        for node_index in nodes {
            if node_index != dummy_node {
                self.add_edge(node_index, dummy_node, 0.0);
            }
        }
    }
    fn remove_dummy_source_node(&mut self) {
        let node_to_remove = self
            .node_indices()
            .find(|&i| self[i].id == -1)
            .expect("Could not find dummy source node");

        // Remove all incoming edges to the dummy node
        let incoming_edges = self
            .edges_directed(node_to_remove, Incoming)
            .map(|e| e.id())
            .collect::<Vec<_>>();
        for edge_id in incoming_edges {
            self.remove_edge(edge_id);
        }

        // Remove the dummy node
        self.remove_node(node_to_remove);
    }
    fn remove_dummy_sink_node(&mut self) {
        let node_to_remove = self
            .node_indices()
            .find(|&i| self[i].id == -2)
            .expect("Could not find dummy sink node");

        // Remove all outgoing edges from the dummy node
        let outgoing_edges = self
            .edges_directed(node_to_remove, Outgoing)
            .map(|e| e.id())
            .collect::<Vec<_>>();
        for edge_id in outgoing_edges {
            self.remove_edge(edge_id);
        }

        // Remove the dummy node
        self.remove_node(node_to_remove);
    }
    /// Calculate the earliest start times for each node in the DAG.
    fn calculate_earliest_start_times(&mut self) -> Vec<f32> {
        let sorted_nodes = toposort(&*self, None).unwrap();
        let mut earliest_start_times = vec![0.0; self.node_count()];

        for node in sorted_nodes.iter() {
            let max_earliest_start_time = self
                .edges_directed(*node, Incoming)
                .map(|edge| {
                    let source_node = edge.source();
                    let exe_time = self[source_node].params["execution_time"];
                    earliest_start_times[source_node.index()] + exe_time
                })
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(0.0);

            earliest_start_times[node.index()] = max_earliest_start_time;
        }

        earliest_start_times
    }
    /// Calculate the latest start times for each node in the DAG.
    fn calculate_latest_start_times(&mut self) -> Vec<f32> {
        let earliest_start_times = self.calculate_earliest_start_times();
        let sorted_nodes = toposort(&*self, None).unwrap();
        let node_count = self.node_count();
        let mut latest_start_times = vec![f32::MAX; node_count];
        latest_start_times[node_count - 1] = earliest_start_times[node_count - 1];

        for &node in sorted_nodes.iter().rev() {
            let min_latest_start_time = self
                .edges_directed(node, Outgoing)
                .map(|edge| {
                    let target_node = edge.target();
                    let pre_exe_time = self[node].params["execution_time"];
                    latest_start_times[target_node.index()] - pre_exe_time
                })
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(earliest_start_times[node_count - 1]);

            latest_start_times[node.index()] = min_latest_start_time;
        }

        latest_start_times
    }
    /// Returns the critical path of a DAG
    /// Multiple critical paths are obtained using Breadth-First Search, BFS
    ///
    /// # Arguments
    ///
    /// * `dag` - dag object. each node contains execution time information.
    ///
    /// # Returns
    ///
    /// * `critical path` -containing the nodes in the critical path. Multiple critical paths may exist. so the return value is a vector of vectors.
    ///
    /// # Example
    ///
    /// ```
    /// use petgraph::Graph;
    /// use std::collections::HashMap;
    /// use lib::graph_ext::NodeData;
    /// use lib::graph_ext::GraphExt;
    ///
    /// let mut dag = Graph::<NodeData, f32>::new();
    /// let mut params = HashMap::new();
    /// params.insert("execution_time".to_string(), 1.0);
    /// let n0 = dag.add_node(NodeData { id: 0, params: params.clone() });
    /// let n1 = dag.add_node(NodeData { id: 1, params: params.clone() });
    /// dag.add_edge(n0, n1, 1.0);
    /// let critical_path = dag.get_critical_paths();
    /// println!("The critical path is: {:?}", critical_path);
    /// ```
    fn get_critical_paths(&mut self) -> Vec<Vec<NodeIndex>> {
        self.add_dummy_source_node();
        self.add_dummy_sink_node();
        println!("DAG: {:?}", self);
        let earliest_start_times = self.calculate_earliest_start_times();
        let latest_start_times = self.calculate_latest_start_times();
        let sorted_nodes = toposort(&*self, None).unwrap();
        let start_node = sorted_nodes[0];
        let mut critical_paths = Vec::new();
        let mut path_search_queue = VecDeque::new();
        path_search_queue.push_back((start_node, vec![start_node]));

        while let Some((node, mut critical_path)) = path_search_queue.pop_front() {
            let outgoing_edges = self.edges_directed(node, Outgoing);

            if outgoing_edges.clone().count() == 0 {
                critical_path.pop(); // Remove the dummy sink node
                critical_path.remove(0); // Remove the dummy source node
                critical_paths.push(critical_path);
            } else {
                for edge in outgoing_edges {
                    let target_node = edge.target();
                    if earliest_start_times[target_node.index()]
                        == latest_start_times[target_node.index()]
                    {
                        let mut current_critical_path = critical_path.clone();
                        current_critical_path.push(target_node);
                        path_search_queue.push_back((target_node, current_critical_path));
                    }
                }
            }
        }

        critical_paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_critical_paths_multiple() {
        fn create_node(id: i32, key: &str, value: f32) -> NodeData {
            let mut params = HashMap::new();
            params.insert(key.to_string(), value);
            NodeData { id, params }
        }
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 45.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 26.0));
        let n4 = dag.add_node(create_node(4, "execution_time", 44.0));
        let n5 = dag.add_node(create_node(5, "execution_time", 26.0));
        let n6 = dag.add_node(create_node(6, "execution_time", 26.0));
        let n7 = dag.add_node(create_node(7, "execution_time", 27.0));
        let n8 = dag.add_node(create_node(8, "execution_time", 43.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n1, n2, 1.0);
        dag.add_edge(n1, n3, 1.0);
        dag.add_edge(n1, n4, 1.0);
        dag.add_edge(n2, n5, 1.0);
        dag.add_edge(n3, n6, 1.0);
        dag.add_edge(n4, n7, 1.0);
        dag.add_edge(n5, n8, 1.0);
        dag.add_edge(n6, n8, 1.0);
        dag.add_edge(n7, n8, 1.0);

        let critical_path = dag.get_critical_paths();
        assert_eq!(critical_path.len(), 2);

        assert_eq!(
            critical_path[0]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![0_usize, 1_usize, 4_usize, 7_usize, 8_usize]
        );
        assert_eq!(
            critical_path[1]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![0_usize, 1_usize, 2_usize, 5_usize, 8_usize]
        );
    }
}
