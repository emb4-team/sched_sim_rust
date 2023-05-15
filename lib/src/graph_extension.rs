use log::warn;
use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f32;

const DUMMY_SOURCE_NODE_FLAG: f32 = -1.0;
const DUMMY_SINK_NODE_FLAG: f32 = -2.0;

/// custom node data structure for dag nodes (petgraph)
#[derive(Debug, Clone)]
pub struct NodeData {
    pub id: i32,
    pub params: HashMap<String, f32>,
}

impl NodeData {
    pub fn new(id: i32, params: &HashMap<String, f32>) -> NodeData {
        NodeData {
            id,
            params: params.clone(),
        }
    }
}

pub trait GraphExtension {
    fn add_dummy_source_node(&mut self) -> NodeIndex;
    fn add_dummy_sink_node(&mut self) -> NodeIndex;
    fn remove_dummy_source_node(&mut self);
    fn remove_dummy_sink_node(&mut self);
    fn get_critical_paths(&mut self) -> Vec<Vec<NodeIndex>>;
    fn get_source_nodes(&self) -> Vec<NodeIndex>;
    fn get_sink_nodes(&self) -> Vec<NodeIndex>;
    fn get_volume(&self) -> f32;
    fn get_total_wcet_from_nodes(&mut self, nodes: &[NodeIndex]) -> f32;
    fn get_end_to_end_deadline(&mut self) -> Option<f32>;
    fn get_pre_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_suc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_anc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_des_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_parallel_process_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
}

impl GraphExtension for Graph<NodeData, f32> {
    fn add_dummy_source_node(&mut self) -> NodeIndex {
        if let Some(dummy_source_node) = self.node_indices().find(|&i| {
            self[i]
                .params
                .get("dummy")
                .map_or(false, |&v| v == DUMMY_SOURCE_NODE_FLAG)
        }) {
            panic!(
                "The dummy source node has already been added. NodeIndex: {:?}",
                dummy_source_node
            );
        }
        let source_nodes = self.get_source_nodes();
        let dummy_source_i = self.add_node(NodeData::new(
            self.node_count() as i32,
            &HashMap::from([
                ("execution_time".to_string(), 0.0),
                ("dummy".to_string(), DUMMY_SOURCE_NODE_FLAG),
            ]),
        ));
        for source_i in source_nodes {
            self.add_edge(dummy_source_i, source_i, 0.0);
        }
        dummy_source_i
    }

    fn add_dummy_sink_node(&mut self) -> NodeIndex {
        if let Some(dummy_sink_node) = self.node_indices().find(|&i| {
            self[i]
                .params
                .get("dummy")
                .map_or(false, |&v| v == DUMMY_SINK_NODE_FLAG)
        }) {
            panic!(
                "The dummy sink node has already been added. NodeIndex: {:?}",
                dummy_sink_node
            );
        }
        let sink_nodes = self.get_sink_nodes();
        let dummy_sink_i = self.add_node(NodeData::new(
            self.node_count() as i32,
            &HashMap::from([
                ("execution_time".to_string(), 0.0),
                ("dummy".to_string(), DUMMY_SINK_NODE_FLAG),
            ]),
        ));
        for sink_i in sink_nodes {
            self.add_edge(sink_i, dummy_sink_i, 0.0);
        }
        dummy_sink_i
    }

    fn remove_dummy_source_node(&mut self) {
        if let Some(dummy_source_node) = self.node_indices().find(|&i| {
            self[i]
                .params
                .get("dummy")
                .map_or(false, |&v| v == DUMMY_SOURCE_NODE_FLAG)
        }) {
            self.remove_node(dummy_source_node);
        } else {
            panic!("The dummy source node does not exist.");
        }
    }

    fn remove_dummy_sink_node(&mut self) {
        if let Some(dummy_sink_node) = self.node_indices().find(|&i| {
            self[i]
                .params
                .get("dummy")
                .map_or(false, |&v| v == DUMMY_SINK_NODE_FLAG)
        }) {
            self.remove_node(dummy_sink_node);
        } else {
            panic!("The dummy sink node does not exist.");
        }
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
    /// use lib::graph_extension::NodeData;
    /// use lib::graph_extension::GraphExtension;
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
        /// Calculate the earliest start times for each node in the DAG.
        fn calculate_earliest_start_times(dag: &mut Graph<NodeData, f32>) -> Vec<f32> {
            let sorted_nodes = toposort(&*dag, None).unwrap();
            let mut earliest_start_times = vec![0.0; dag.node_count()];

            for node in sorted_nodes.iter() {
                let max_earliest_start_time = dag
                    .edges_directed(*node, Incoming)
                    .map(|edge| {
                        let source_node = edge.source();
                        let exe_time = dag[source_node].params["execution_time"];
                        earliest_start_times[source_node.index()] + exe_time
                    })
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(0.0);

                earliest_start_times[node.index()] = max_earliest_start_time;
            }
            assert!(
                !earliest_start_times.iter().any(|&time| time < 0.0),
                "The earliest start times should be non-negative."
            );
            earliest_start_times
        }

        /// Calculate the latest start times for each node in the DAG.
        fn calculate_latest_start_times(dag: &mut Graph<NodeData, f32>) -> Vec<f32> {
            let earliest_start_times = calculate_earliest_start_times(dag);
            let sorted_nodes = toposort(&*dag, None).unwrap();
            let node_count = dag.node_count();
            let mut latest_start_times = vec![f32::MAX; node_count];
            let sink_node_index = dag.get_sink_nodes();
            latest_start_times[sink_node_index[0].index()] =
                earliest_start_times[sink_node_index[0].index()];

            for &node in sorted_nodes.iter().rev() {
                let min_latest_start_time = dag
                    .edges_directed(node, Outgoing)
                    .map(|edge| {
                        let target_node = edge.target();
                        let pre_exe_time = dag[node].params["execution_time"];
                        latest_start_times[target_node.index()] - pre_exe_time
                    })
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(earliest_start_times[sink_node_index[0].index()]);

                latest_start_times[node.index()] = min_latest_start_time;
            }
            assert!(
                !latest_start_times.iter().any(|&time| time < 0.0),
                "The latest start times should be non-negative."
            );
            latest_start_times
        }

        self.add_dummy_sink_node();
        let start_node = self.add_dummy_source_node();
        let earliest_start_times = calculate_earliest_start_times(self);
        let latest_start_times = calculate_latest_start_times(self);
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
        self.remove_dummy_source_node();
        self.remove_dummy_sink_node();
        critical_paths
    }

    fn get_source_nodes(&self) -> Vec<NodeIndex> {
        self.node_indices()
            .filter(|&i| self.edges_directed(i, Incoming).next().is_none())
            .collect::<Vec<_>>()
    }

    fn get_sink_nodes(&self) -> Vec<NodeIndex> {
        self.node_indices()
            .filter(|&i| self.edges_directed(i, Outgoing).next().is_none())
            .collect::<Vec<_>>()
    }

    fn get_volume(&self) -> f32 {
        self.node_indices()
            .map(|node| {
                *self[node]
                    .params
                    .get("execution_time")
                    .unwrap_or_else(|| panic!("execution_time not found"))
            })
            .sum()
    }

    fn get_total_wcet_from_nodes(&mut self, nodes: &[NodeIndex]) -> f32 {
        nodes
            .iter()
            .map(|node| {
                self[*node]
                    .params
                    .get("execution_time")
                    .unwrap_or_else(|| panic!("execution_time not found"))
            })
            .sum()
    }

    fn get_end_to_end_deadline(&mut self) -> Option<f32> {
        self.node_indices()
            .find_map(|i| self[i].params.get("end_to_end_deadline").cloned())
            .or_else(|| {
                warn!("The end-to-end deadline does not exist.");
                None
            })
    }

    fn get_pre_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>> {
        //Since node indices are sequentially numbered, this is used to determine whether a node exists or not.
        if node_i.index() < self.node_count() {
            let pre_nodes = self
                .edges_directed(node_i, Incoming)
                .map(|edge| edge.source())
                .collect::<Vec<_>>();

            if pre_nodes.is_empty() {
                None
            } else {
                Some(pre_nodes)
            }
        } else {
            panic!("Node {:?} does not exist!", node_i);
        }
    }

    fn get_suc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>> {
        //Since node indices are sequentially numbered, this is used to determine whether a node exists or not.
        if node_i.index() < self.node_count() {
            let suc_nodes = self
                .edges_directed(node_i, Outgoing)
                .map(|edge| edge.target())
                .collect::<Vec<_>>();

            if suc_nodes.is_empty() {
                None
            } else {
                Some(suc_nodes)
            }
        } else {
            panic!("Node {:?} does not exist!", node_i);
        }
    }

    fn get_anc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>> {
        let mut anc_nodes = Vec::new();
        let mut search_queue = VecDeque::new();
        search_queue.push_back(node_i);

        while let Some(node) = search_queue.pop_front() {
            //If the target node does not exist, get_pre_node causes panic!
            for pre_node in self.get_pre_nodes(node).unwrap_or_default() {
                if !anc_nodes.contains(&pre_node) {
                    anc_nodes.push(pre_node);
                    search_queue.push_back(pre_node);
                }
            }
        }
        Some(anc_nodes).filter(|anc| !anc.is_empty())
    }

    fn get_des_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>> {
        let mut des_nodes = Vec::new();
        let mut search_queue = VecDeque::new();
        search_queue.push_back(node_i);

        while let Some(node) = search_queue.pop_front() {
            //If the target node does not exist, get_suc_node causes panic!
            for suc_node in self.get_suc_nodes(node).unwrap_or_default() {
                if !des_nodes.contains(&suc_node) {
                    des_nodes.push(suc_node);
                    search_queue.push_back(suc_node);
                }
            }
        }
        Some(des_nodes).filter(|des| !des.is_empty())
    }

    fn get_parallel_process_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>> {
        let parallel_process_nodes: Vec<_> = self
            .node_indices()
            .filter(|&node| {
                node != node_i
                    && !self
                        .get_anc_nodes(node)
                        .unwrap_or_default()
                        .contains(&node_i)
                    && !self
                        .get_des_nodes(node)
                        .unwrap_or_default()
                        .contains(&node_i)
            })
            .collect();

        if parallel_process_nodes.is_empty() {
            None
        } else {
            Some(parallel_process_nodes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_get_critical_paths_single() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 7.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 55.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 36.0));
        let n4 = dag.add_node(create_node(4, "execution_time", 54.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n1, n3, 1.0);
        dag.add_edge(n2, n4, 1.0);

        let critical_path = dag.get_critical_paths();
        assert_eq!(critical_path.len(), 1);

        assert_eq!(
            critical_path[0]
                .iter()
                .map(|node_index| node_index.index())
                .collect::<Vec<_>>(),
            vec![0_usize, 2_usize, 4_usize]
        );
    }

    #[test]
    fn test_get_critical_paths_multiple() {
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

    #[test]
    fn test_remove_dummy_node_check_whether_connected_edges_removed() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 45.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        dag.add_dummy_source_node();
        dag.add_dummy_sink_node();
        assert_eq!(dag.edge_count(), 5);
        dag.remove_dummy_source_node();
        assert_eq!(dag.edge_count(), 4);
        dag.remove_dummy_sink_node();
        assert_eq!(dag.edge_count(), 2);
    }

    #[test]
    #[should_panic]
    fn test_remove_dummy_node_no_exist() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        dag.remove_dummy_source_node();
        dag.remove_dummy_sink_node();
    }

    #[test]
    #[should_panic]
    fn test_add_dummy_node_duplication() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 45.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        dag.add_dummy_source_node();
        dag.add_dummy_source_node();
        dag.add_dummy_sink_node();
        dag.add_dummy_sink_node();
    }

    #[test]
    fn test_get_source_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        assert_eq!(
            dag.get_source_nodes(),
            vec![NodeIndex::new(0), NodeIndex::new(1), NodeIndex::new(2),]
        );
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        assert_eq!(dag.get_source_nodes(), vec![NodeIndex::new(0)]);
    }

    #[test]
    fn test_get_source_nodes_dummy_node() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        dag.add_dummy_source_node();
        assert_eq!(dag.get_source_nodes(), vec![NodeIndex::new(3)]);
    }

    #[test]
    fn test_get_sink_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        assert_eq!(
            dag.get_sink_nodes(),
            vec![NodeIndex::new(0), NodeIndex::new(1), NodeIndex::new(2)]
        );
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        assert_eq!(
            dag.get_sink_nodes(),
            vec![NodeIndex::new(1), NodeIndex::new(2)]
        );
    }

    #[test]
    fn test_get_sink_nodes_dummy_node() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        dag.add_dummy_sink_node();
        assert_eq!(dag.get_sink_nodes(), vec![NodeIndex::new(3)]);
    }

    #[test]
    fn test_add_dummy_node_integrity_for_id_and_node_index() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        let source_index = dag.add_dummy_source_node();
        let sink_index = dag.add_dummy_sink_node();

        assert!(dag[source_index].id == source_index.index() as i32);
        assert!(dag[sink_index].id == sink_index.index() as i32);
    }

    #[test]
    fn test_get_volume_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 6.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 5.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        assert_eq!(dag.get_volume(), 14.0);
    }

    #[test]
    #[should_panic]
    fn test_get_volume_node_no_includes_execution_time() {
        let mut dag = Graph::<NodeData, f32>::new();
        dag.add_node(create_node(0, "weight", 3.0));

        dag.get_volume();
    }

    #[test]
    fn test_get_total_wcet_from_nodes_any_given_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 7.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 55.0));

        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        let nodes0 = vec![n0, n1];
        let nodes1 = vec![n0, n2];

        assert_eq!(dag.get_total_wcet_from_nodes(&nodes0), 11.0);
        assert_eq!(dag.get_total_wcet_from_nodes(&nodes1), 59.0);
    }

    #[test]
    fn test_get_total_wcet_from_nodes_given_one_node() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let nodes0 = vec![n0];

        assert_eq!(dag.get_total_wcet_from_nodes(&nodes0), 4.0);
    }

    #[test]
    #[should_panic]
    fn test_get_total_wcet_from_nodes_node_no_includes_execution_time() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "weight", 3.0));

        let nodes = vec![n0];
        dag.get_total_wcet_from_nodes(&nodes);
    }

    #[test]
    fn test_get_end_to_end_deadline_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n1 = dag.add_node(NodeData {
            id: 1,
            params: {
                let mut params = HashMap::new();
                params.insert("execution_time".to_string(), 11.0);
                params.insert("end_to_end_deadline".to_string(), 25.0);
                params
            },
        });

        dag.add_edge(n0, n1, 1.0);

        assert_eq!(dag.get_end_to_end_deadline(), Some(25.0));
    }

    #[test]
    fn test_get_end_to_end_deadline_node_no_includes_end_to_end_deadline() {
        let mut dag = Graph::<NodeData, f32>::new();
        dag.add_node(create_node(0, "execution_time", 3.0));

        assert_eq!(dag.get_end_to_end_deadline(), None);
    }

    #[test]
    fn test_get_pre_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n1, n2, 1.0);
        dag.add_edge(n0, n2, 1.0);

        assert_eq!(dag.get_pre_nodes(n2), Some(vec![n0, n1]));
    }

    #[test]
    fn test_get_pre_nodes_single() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);

        assert_eq!(dag.get_pre_nodes(n1), Some(vec![n0]));
    }

    #[test]
    fn test_get_pre_nodes_no_exist_pre_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));

        assert_eq!(dag.get_pre_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_pre_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, f32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_pre_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_suc_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);

        assert_eq!(dag.get_suc_nodes(n0), Some(vec![n2, n1]));
    }

    #[test]
    fn test_get_suc_nodes_single() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);

        assert_eq!(dag.get_suc_nodes(n0), Some(vec![n1]));
    }

    #[test]
    fn test_get_suc_nodes_no_exist_suc_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));

        assert_eq!(dag.get_suc_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_suc_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, f32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_suc_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_anc_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n2, n3, 1.0);
        dag.add_edge(n1, n3, 1.0);

        assert_eq!(dag.get_anc_nodes(n3), Some(vec![n1, n2, n0]));
    }

    #[test]
    fn test_get_anc_nodes_single() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);

        assert_eq!(dag.get_anc_nodes(n1), Some(vec![n0]));
    }

    #[test]
    fn test_get_anc_nodes_no_exist_anc_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));

        assert_eq!(dag.get_anc_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_anc_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, f32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_anc_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_des_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n1, n3, 1.0);

        assert_eq!(dag.get_des_nodes(n0), Some(vec![n2, n1, n3]));
    }

    #[test]
    fn test_get_des_nodes_single() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0.0));
        dag.add_edge(n0, n1, 1.0);

        assert_eq!(dag.get_des_nodes(n0), Some(vec![n1]));
    }

    #[test]
    fn test_get_des_nodes_no_exist_des_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0.0));

        assert_eq!(dag.get_des_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_des_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, f32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_des_nodes(invalid_node), None);
    }

    #[test]
    fn get_parallel_process_nodes_normal() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "parallel_process", 0.0));
        let n1 = dag.add_node(create_node(1, "parallel_process", 0.0));
        let n2 = dag.add_node(create_node(2, "parallel_process", 0.0));
        let n3 = dag.add_node(create_node(3, "parallel_process", 0.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n1, n3, 1.0);

        assert_eq!(dag.get_parallel_process_nodes(n2), Some(vec![n1, n3]));
        assert_eq!(dag.get_parallel_process_nodes(n3), Some(vec![n2]));
    }

    #[test]
    fn get_parallel_process_nodes_no_exist_parallel_process_nodes() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "parallel_process", 0.0));

        assert_eq!(dag.get_parallel_process_nodes(n0), None);
    }
}
