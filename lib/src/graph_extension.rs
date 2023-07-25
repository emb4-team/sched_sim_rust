use log::warn;
use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction::{Incoming, Outgoing};
use std::cmp::Ord;
use std::collections::VecDeque;
use std::collections::{BTreeMap, HashMap};

const DUMMY_SOURCE_NODE_FLAG: i32 = -1;
const DUMMY_SINK_NODE_FLAG: i32 = -2;

/// custom node data structure for dag nodes (petgraph)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeData {
    pub id: i32,
    pub params: BTreeMap<String, i32>,
}

impl NodeData {
    pub fn new(id: i32, params: BTreeMap<String, i32>) -> NodeData {
        NodeData { id, params }
    }

    pub fn get_params_value(&self, key: &str) -> i32 {
        *self
            .params
            .get(key)
            .unwrap_or_else(|| panic!("The key does not exist. key: {}", key))
    }
}

pub trait GraphExtension {
    fn add_param(&mut self, node_i: NodeIndex, key: &str, value: i32);
    fn update_param(&mut self, node_i: NodeIndex, key: &str, value: i32);
    fn add_dummy_source_node(&mut self) -> NodeIndex;
    fn add_dummy_sink_node(&mut self) -> NodeIndex;
    fn remove_dummy_source_node(&mut self);
    fn remove_dummy_sink_node(&mut self);
    fn remove_nodes(&mut self, node_indices: &[NodeIndex]);
    fn calculate_earliest_start_times(&mut self);
    fn calculate_earliest_finish_times(&mut self);
    fn calculate_latest_start_times(&mut self);
    fn calculate_latest_finish_times(&mut self);
    fn get_critical_path(&mut self) -> Vec<NodeIndex>;
    fn get_non_critical_nodes(&self, critical_path: &[NodeIndex]) -> Option<Vec<NodeIndex>>;
    fn get_source_nodes(&self) -> Vec<NodeIndex>;
    fn get_sink_nodes(&self) -> Vec<NodeIndex>;
    fn get_volume(&self) -> i32;
    fn get_total_wcet_from_nodes(&self, nodes: &[NodeIndex]) -> i32;
    fn get_end_to_end_deadline(&self) -> Option<i32>;
    fn get_head_period(&self) -> Option<i32>;
    fn get_all_periods(&self) -> Option<HashMap<NodeIndex, i32>>;
    fn get_head_offset(&self) -> i32;
    fn get_pre_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_suc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_anc_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_des_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_parallel_process_nodes(&self, node_i: NodeIndex) -> Option<Vec<NodeIndex>>;
    fn get_dag_id(&self) -> usize;
    fn set_dag_id(&mut self, dag_id: usize);
    fn add_node_with_id_consistency(&mut self, node: NodeData) -> NodeIndex;
    fn is_node_ready(&self, node_i: NodeIndex) -> bool;
    fn increment_pre_done_count(&mut self, node_i: NodeIndex);
}

impl GraphExtension for Graph<NodeData, i32> {
    fn add_param(&mut self, node_i: NodeIndex, key: &str, value: i32) {
        let target_node = self.node_weight_mut(node_i).unwrap();
        if target_node.params.contains_key(key) {
            warn!("The key already exists. key: {}", key);
        } else {
            target_node.params.insert(key.to_string(), value);
        }
    }

    fn update_param(&mut self, node_i: NodeIndex, key: &str, value: i32) {
        let target_node = self.node_weight_mut(node_i).unwrap();
        if !target_node.params.contains_key(key) {
            warn!("The key no exists. key: {}", key);
        } else {
            target_node.params.insert(key.to_string(), value);
        }
    }

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
            BTreeMap::from([
                ("execution_time".to_string(), 0),
                ("dummy".to_string(), DUMMY_SOURCE_NODE_FLAG),
            ]),
        ));
        for source_i in source_nodes {
            self.add_edge(dummy_source_i, source_i, 0);
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
            BTreeMap::from([
                ("execution_time".to_string(), 0),
                ("dummy".to_string(), DUMMY_SINK_NODE_FLAG),
            ]),
        ));
        for sink_i in sink_nodes {
            self.add_edge(sink_i, dummy_sink_i, 0);
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

    fn remove_nodes(&mut self, node_indices: &[NodeIndex]) {
        for node_i in node_indices.iter().rev() {
            self.remove_node(*node_i);
        }
    }

    /// Calculate the earliest start times for each node in the DAG.
    fn calculate_earliest_start_times(&mut self) {
        let mut earliest_start_times = vec![0; self.node_count()];

        let sorted_nodes = toposort(&*self, None).unwrap();
        for node_i in sorted_nodes {
            let max_earliest_start_time = self
                .edges_directed(node_i, Incoming)
                .map(|edge| {
                    let source_node = edge.source();
                    let exe_time = self[source_node].params["execution_time"];
                    earliest_start_times[source_node.index()] + exe_time
                })
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(0);

            earliest_start_times[node_i.index()] = max_earliest_start_time;
            if self[node_i].params.contains_key("earliest_start_time") {
                self.update_param(node_i, "earliest_start_time", max_earliest_start_time);
            } else {
                self.add_param(node_i, "earliest_start_time", max_earliest_start_time);
            }
        }
        assert!(
            !earliest_start_times.iter().any(|&time| time < 0),
            "The earliest start times should be non-negative."
        );
    }

    fn calculate_earliest_finish_times(&mut self) {
        self.calculate_earliest_start_times();

        for node_i in self.node_indices() {
            let earliest_finish_time =
                self[node_i].params["earliest_start_time"] + self[node_i].params["execution_time"];
            if self[node_i].params.contains_key("earliest_finish_time") {
                self.update_param(node_i, "earliest_finish_time", earliest_finish_time);
            } else {
                self.add_param(node_i, "earliest_finish_time", earliest_finish_time);
            }
        }
    }

    /// Calculate the latest start times for each node in the DAG.
    fn calculate_latest_start_times(&mut self) {
        self.calculate_earliest_start_times();
        let sorted_nodes = toposort(&*self, None).unwrap();
        let mut latest_start_times = vec![i32::MAX; self.node_count()];
        let sink_node_index = self.get_sink_nodes();
        latest_start_times[sink_node_index[0].index()] =
            self[sink_node_index[0]].params["earliest_start_time"];

        for &node_i in sorted_nodes.iter().rev() {
            let min_latest_start_time = self
                .edges_directed(node_i, Outgoing)
                .map(|edge| {
                    let target_node = edge.target();
                    let pre_exe_time = self[node_i].params["execution_time"];
                    latest_start_times[target_node.index()] - pre_exe_time
                })
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(self[sink_node_index[0]].params["earliest_start_time"]);

            latest_start_times[node_i.index()] = min_latest_start_time;
            if self[node_i].params.contains_key("latest_start_time") {
                self.update_param(node_i, "latest_start_time", min_latest_start_time);
            } else {
                self.add_param(node_i, "latest_start_time", min_latest_start_time);
            }
        }

        assert!(
            !latest_start_times.iter().any(|&time| time < 0),
            "The latest start times should be non-negative."
        );
    }

    fn calculate_latest_finish_times(&mut self) {
        self.calculate_latest_start_times();

        for node_i in self.node_indices() {
            let latest_finish_time =
                self[node_i].params["latest_start_time"] + self[node_i].params["execution_time"];
            if self[node_i].params.contains_key("latest_finish_time") {
                self.update_param(node_i, "latest_finish_time", latest_finish_time);
            } else {
                self.add_param(node_i, "latest_finish_time", latest_finish_time);
            }
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
    /// use std::collections::BTreeMap;
    /// use std::collections::HashMap;
    /// use lib::graph_extension::NodeData;
    /// use lib::graph_extension::GraphExtension;
    ///
    /// let mut dag = Graph::<NodeData, i32>::new();
    /// let mut params = BTreeMap::new();
    /// params.insert("execution_time".to_string(), 1);
    /// let n0 = dag.add_node(NodeData { id: 0, params: params.clone() });
    /// let n1 = dag.add_node(NodeData { id: 1, params: params });
    /// dag.add_edge(n0, n1, 1);
    /// let critical_path = dag.get_critical_path();
    /// println!("The critical path is: {:?}", critical_path);
    /// ```
    fn get_critical_path(&mut self) -> Vec<NodeIndex> {
        self.add_dummy_sink_node();
        let start_node = self.add_dummy_source_node();
        self.calculate_earliest_start_times();
        self.calculate_latest_start_times();
        let mut path_search_queue = VecDeque::new();
        path_search_queue.push_back((start_node, vec![start_node]));
        let mut critical_path = Vec::new();

        while let Some((node, mut current_critical_path)) = path_search_queue.pop_front() {
            let outgoing_edges: Vec<_> = self.edges_directed(node, Outgoing).collect();

            if outgoing_edges.is_empty() {
                current_critical_path.pop(); // Remove the dummy sink node
                current_critical_path.remove(0); // Remove the dummy source node
                critical_path.push(current_critical_path);
            } else {
                for edge in outgoing_edges {
                    let target_node = edge.target();
                    if self[target_node].params["earliest_start_time"]
                        == self[target_node].params["latest_start_time"]
                    {
                        let mut new_critical_path = current_critical_path.clone();
                        new_critical_path.push(target_node);
                        path_search_queue.push_back((target_node, new_critical_path));
                    }
                }
            }
        }

        self.remove_dummy_source_node();
        self.remove_dummy_sink_node();
        if critical_path.len() > 1 {
            warn!("There are more than one critical paths.");
        }
        critical_path[0].clone()
    }

    fn get_non_critical_nodes(&self, critical_path: &[NodeIndex]) -> Option<Vec<NodeIndex>> {
        let mut no_critical_path_nodes = Vec::new();
        for node in self.node_indices() {
            if !critical_path.contains(&node) {
                no_critical_path_nodes.push(node);
            }
        }

        if no_critical_path_nodes.is_empty() {
            None
        } else {
            Some(no_critical_path_nodes)
        }
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

    fn get_volume(&self) -> i32 {
        self.node_indices()
            .map(|node| {
                *self[node]
                    .params
                    .get("execution_time")
                    .unwrap_or_else(|| panic!("execution_time not found"))
            })
            .sum()
    }

    fn get_total_wcet_from_nodes(&self, nodes: &[NodeIndex]) -> i32 {
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

    fn get_end_to_end_deadline(&self) -> Option<i32> {
        self.node_indices()
            .find_map(|i| match self[i].params.get("end_to_end_deadline") {
                Some(end_to_end_deadline) => Some(*end_to_end_deadline),
                None => {
                    warn!("The end-to-end deadline does not exist.");
                    None
                }
            })
    }

    fn get_head_period(&self) -> Option<i32> {
        let source_nodes = self.get_source_nodes();
        let periods: Vec<&i32> = source_nodes
            .iter()
            .filter_map(|&node_i| self[node_i].params.get("period"))
            .collect();

        if source_nodes.len() > 1 {
            warn!("Multiple source nodes found.");
        }
        if periods.len() > 1 {
            warn!("Multiple periods found. The first period is used.");
        }
        if periods.is_empty() {
            warn!("No period found.");
            return None;
        }
        Some(*periods[0])
    }

    fn get_all_periods(&self) -> Option<HashMap<NodeIndex, i32>> {
        let mut period_map = HashMap::new();
        for node in self.node_indices() {
            if let Some(period) = self[node].params.get("period") {
                period_map.insert(node, *period);
            }
        }
        if period_map.is_empty() {
            None
        } else {
            Some(period_map)
        }
    }

    fn get_head_offset(&self) -> i32 {
        let source_nodes = self.get_source_nodes();
        let offsets: Vec<&i32> = source_nodes
            .iter()
            .filter_map(|&node_i| self[node_i].params.get("offset"))
            .collect();
        if source_nodes.len() > 1 {
            warn!("Multiple source nodes found.");
        }
        if offsets.len() > 1 {
            warn!("Multiple offsets found. The first offset is used.");
        }
        if offsets.is_empty() {
            warn!("No offset found. 0 is used");
            0
        } else {
            *offsets[0]
        }
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

    fn get_dag_id(&self) -> usize {
        if self.node_indices().count() == 0 {
            panic!("Error: dag_id does not exist. Please use set_dag_id(dag_id: usize)");
        }
        self[NodeIndex::new(0)].params["dag_id"] as usize
    }

    fn set_dag_id(&mut self, dag_id: usize) {
        if self.node_indices().count() == 0 {
            panic!("No node found.");
        }
        for node_i in self.node_indices() {
            self.add_param(node_i, "dag_id", dag_id as i32);
        }
    }

    fn add_node_with_id_consistency(&mut self, node: NodeData) -> NodeIndex {
        let node_index = self.add_node(node);

        assert_eq!(
            node_index.index() as i32,
            self[node_index].id,
            "The add node id is different from NodeIndex."
        );

        node_index
    }

    fn is_node_ready(&self, node_i: NodeIndex) -> bool {
        let pre_nodes_count = self.get_pre_nodes(node_i).unwrap_or_default().len() as i32;
        let pre_done_nodes_count = self[node_i].params.get("pre_done_count").unwrap_or(&0);
        pre_nodes_count == *pre_done_nodes_count
    }

    fn increment_pre_done_count(&mut self, node_i: NodeIndex) {
        *self[node_i]
            .params
            .entry("pre_done_count".to_owned())
            .or_insert(0) += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test_add_param_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        dag.add_param(n0, "test", 1);
        assert_eq!(dag[n0].params.get("test").unwrap(), &1);
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &0);
    }

    #[test]
    fn test_add_param_duplicate() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &0);
        dag.add_param(n0, "execution_time", 1);
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &0);
    }

    #[test]
    fn test_update_param_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &0);
        dag.update_param(n0, "execution_time", 1);
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &1);
    }
    #[test]
    fn test_update_param_no_exist_params() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        dag.update_param(n0, "test", 1);
        assert_eq!(dag[n0].params.get("test"), None);
        assert_eq!(dag[n0].params.get("execution_time").unwrap(), &0);
    }

    #[test]
    fn test_calculate_earliest_start_times_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n4, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n4, n2, 1);

        dag.calculate_earliest_start_times();
        assert_eq!(dag[n0].params["earliest_start_time"], 0);
        assert_eq!(dag[n1].params["earliest_start_time"], 4);
        assert_eq!(dag[n2].params["earliest_start_time"], 58);
        assert_eq!(dag[n3].params["earliest_start_time"], 11);
        assert_eq!(dag[n4].params["earliest_start_time"], 4);
    }

    #[test]
    fn test_calculate_earliest_finish_times_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        dag.calculate_earliest_finish_times();
        assert_eq!(dag[n0].params["earliest_finish_time"], 4);
        assert_eq!(dag[n1].params["earliest_finish_time"], 11);
        assert_eq!(dag[n2].params["earliest_finish_time"], 59);
        assert_eq!(dag[n3].params["earliest_finish_time"], 47);
        assert_eq!(dag[n4].params["earliest_finish_time"], 113);
    }

    #[test]
    fn test_calculate_lasted_start_times_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);
        dag.add_dummy_sink_node();
        dag.add_dummy_source_node();

        dag.calculate_latest_start_times();
        dag.remove_dummy_sink_node();
        dag.remove_dummy_source_node();

        assert_eq!(dag[n0].params["latest_start_time"], 0);
        assert_eq!(dag[n1].params["latest_start_time"], 70);
        assert_eq!(dag[n2].params["latest_start_time"], 4);
        assert_eq!(dag[n3].params["latest_start_time"], 77);
        assert_eq!(dag[n4].params["latest_start_time"], 59);
    }

    #[test]
    fn test_calculate_lasted_finish_times_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);
        dag.add_dummy_sink_node();
        dag.add_dummy_source_node();

        dag.calculate_latest_finish_times();
        dag.remove_dummy_sink_node();
        dag.remove_dummy_source_node();

        assert_eq!(dag[n0].params["latest_finish_time"], 4);
        assert_eq!(dag[n1].params["latest_finish_time"], 77);
        assert_eq!(dag[n2].params["latest_finish_time"], 59);
        assert_eq!(dag[n3].params["latest_finish_time"], 113);
        assert_eq!(dag[n4].params["latest_finish_time"], 113);
    }

    #[test]
    fn test_get_critical_path_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        let critical_path = dag.get_critical_path();
        assert_eq!(critical_path.len(), 3);

        assert_eq!(critical_path, &[n0, n2, n4]);
    }

    #[test]
    fn test_get_non_critical_nodes_when_critical_path_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        let critical_path = dag.get_critical_path();
        let no_critical_path_nodes = dag.get_non_critical_nodes(&critical_path).unwrap();
        assert_eq!(no_critical_path_nodes.len(), 2);

        assert_eq!(no_critical_path_nodes, &[n1, n3]);
    }

    #[test]
    fn test_get_non_critical_nodes_no_exist() {
        let mut dag = Graph::<NodeData, i32>::new();
        let critical_path = dag.get_critical_path();
        let no_critical_path_nodes = dag.get_non_critical_nodes(&critical_path);
        assert_eq!(no_critical_path_nodes, None);
    }

    #[test]
    fn test_remove_dummy_node_check_whether_connected_edges_removed() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3));
        let n1 = dag.add_node(create_node(1, "execution_time", 6));
        let n2 = dag.add_node(create_node(2, "execution_time", 45));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

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
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        dag.remove_dummy_source_node();
        dag.remove_dummy_sink_node();
    }

    #[test]
    fn test_remove_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3));
        let n1 = dag.add_node(create_node(1, "execution_time", 6));
        let n2 = dag.add_node(create_node(2, "execution_time", 45));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        dag.remove_nodes(&[n1, n2]);
        assert_eq!(dag.node_count(), 1);
        assert_eq!(dag.edge_count(), 0);
        assert_eq!(dag[n0].id, 0);

        fn contains(dag: &Graph<NodeData, i32>, node: NodeIndex) -> bool {
            dag.node_indices().any(|i| i == node)
        }

        assert!(!contains(&dag, n1));
        assert!(!contains(&dag, n2));
    }

    #[test]
    #[should_panic]
    fn test_add_dummy_node_duplication() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3));
        let n1 = dag.add_node(create_node(1, "execution_time", 6));
        let n2 = dag.add_node(create_node(2, "execution_time", 45));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        dag.add_dummy_source_node();
        dag.add_dummy_source_node();
        dag.add_dummy_sink_node();
        dag.add_dummy_sink_node();
    }

    #[test]
    fn test_get_source_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        assert_eq!(
            dag.get_source_nodes(),
            vec![NodeIndex::new(0), NodeIndex::new(1), NodeIndex::new(2),]
        );
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        assert_eq!(dag.get_source_nodes(), vec![NodeIndex::new(0)]);
    }

    #[test]
    fn test_get_source_nodes_dummy_node() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        dag.add_dummy_source_node();
        assert_eq!(dag.get_source_nodes(), vec![NodeIndex::new(3)]);
    }

    #[test]
    fn test_get_sink_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        assert_eq!(
            dag.get_sink_nodes(),
            vec![NodeIndex::new(0), NodeIndex::new(1), NodeIndex::new(2)]
        );
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        assert_eq!(
            dag.get_sink_nodes(),
            vec![NodeIndex::new(1), NodeIndex::new(2)]
        );
    }

    #[test]
    fn test_get_sink_nodes_dummy_node() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        dag.add_dummy_sink_node();
        assert_eq!(dag.get_sink_nodes(), vec![NodeIndex::new(3)]);
    }

    #[test]
    fn test_add_dummy_node_integrity_for_id_and_node_index() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        let source_index = dag.add_dummy_source_node();
        let sink_index = dag.add_dummy_sink_node();

        assert!(dag[source_index].id == source_index.index() as i32);
        assert!(dag[sink_index].id == sink_index.index() as i32);
    }

    #[test]
    fn test_get_volume_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3));
        let n1 = dag.add_node(create_node(1, "execution_time", 6));
        let n2 = dag.add_node(create_node(2, "execution_time", 5));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        assert_eq!(dag.get_volume(), 14);
    }

    #[test]
    #[should_panic]
    fn test_get_volume_node_no_includes_execution_time() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "weight", 3));

        dag.get_volume();
    }

    #[test]
    fn test_get_total_wcet_from_nodes_any_given_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let n2 = dag.add_node(create_node(2, "execution_time", 55));

        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        let nodes0 = vec![n0, n1];
        let nodes1 = vec![n0, n2];

        assert_eq!(dag.get_total_wcet_from_nodes(&nodes0), 11);
        assert_eq!(dag.get_total_wcet_from_nodes(&nodes1), 59);
    }

    #[test]
    fn test_get_total_wcet_from_nodes_given_one_node() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let nodes0 = vec![n0];

        assert_eq!(dag.get_total_wcet_from_nodes(&nodes0), 4);
    }

    #[test]
    #[should_panic]
    fn test_get_total_wcet_from_nodes_node_no_includes_execution_time() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "weight", 3));

        let nodes = vec![n0];
        dag.get_total_wcet_from_nodes(&nodes);
    }

    #[test]
    fn test_get_end_to_end_deadline_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 3));
        let n1 = dag.add_node(NodeData {
            id: 1,
            params: {
                let mut params = BTreeMap::new();
                params.insert("execution_time".to_string(), 11);
                params.insert("end_to_end_deadline".to_string(), 25);
                params
            },
        });

        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_end_to_end_deadline(), Some(25));
    }

    #[test]
    fn test_get_end_to_end_deadline_node_no_includes_end_to_end_deadline() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 3));

        assert_eq!(dag.get_end_to_end_deadline(), None);
    }

    #[test]
    fn test_get_head_period_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "period", 3));
        let n1 = dag.add_node(create_node(0, "period", 4));

        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_head_period(), Some(3));
    }

    #[test]
    fn test_get_head_period_node_no_includes_period() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "weight", 3));

        assert_eq!(dag.get_head_period(), None);
    }

    #[test]
    fn test_get_all_periods_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "period", 3));
        let n1 = dag.add_node(create_node(0, "period", 4));

        dag.add_edge(n0, n1, 1);

        let mut expected_period_map = HashMap::new();
        expected_period_map.insert(n0, 3);
        expected_period_map.insert(n1, 4);
        assert_eq!(dag.get_all_periods(), Some(expected_period_map));
    }

    #[test]
    fn test_get_all_periods_node_no_includes_period() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 3));

        assert_eq!(dag.get_all_periods(), None);
    }

    #[test]
    fn test_get_offset_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "offset", 3));

        assert_eq!(dag.get_head_offset(), 3);
    }

    #[test]
    fn test_get_offset_multiple() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "offset", 3));
        dag.add_node(create_node(1, "offset", 2));

        assert_eq!(dag.get_head_offset(), 3);
    }

    #[test]
    fn test_get_offset_no_exist() {
        let dag = Graph::<NodeData, i32>::new();

        assert_eq!(dag.get_head_offset(), 0);
    }

    #[test]
    fn test_get_pre_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n1, n2, 1);
        dag.add_edge(n0, n2, 1);

        assert_eq!(dag.get_pre_nodes(n2), Some(vec![n0, n1]));
    }

    #[test]
    fn test_get_pre_nodes_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_pre_nodes(n1), Some(vec![n0]));
    }

    #[test]
    fn test_get_pre_nodes_no_exist_pre_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));

        assert_eq!(dag.get_pre_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_pre_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, i32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_pre_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_suc_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);

        assert_eq!(dag.get_suc_nodes(n0), Some(vec![n2, n1]));
    }

    #[test]
    fn test_get_suc_nodes_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_suc_nodes(n0), Some(vec![n1]));
    }

    #[test]
    fn test_get_suc_nodes_no_exist_suc_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));

        assert_eq!(dag.get_suc_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_suc_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, i32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_suc_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_anc_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        let n3 = dag.add_node(create_node(3, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n2, n3, 1);
        dag.add_edge(n1, n3, 1);

        assert_eq!(dag.get_anc_nodes(n3), Some(vec![n1, n2, n0]));
    }

    #[test]
    fn test_get_anc_nodes_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_anc_nodes(n1), Some(vec![n0]));
    }

    #[test]
    fn test_get_anc_nodes_no_exist_anc_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));

        assert_eq!(dag.get_anc_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_anc_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, i32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_anc_nodes(invalid_node), None);
    }

    #[test]
    fn test_get_des_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        let n2 = dag.add_node(create_node(2, "execution_time", 0));
        let n3 = dag.add_node(create_node(3, "execution_time", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);

        assert_eq!(dag.get_des_nodes(n0), Some(vec![n2, n1, n3]));
    }

    #[test]
    fn test_get_des_nodes_single() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        dag.add_edge(n0, n1, 1);

        assert_eq!(dag.get_des_nodes(n0), Some(vec![n1]));
    }

    #[test]
    fn test_get_des_nodes_no_exist_des_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));

        assert_eq!(dag.get_des_nodes(n0), None);
    }

    #[test]
    #[should_panic]
    fn test_get_des_nodes_no_exist_target_node() {
        let dag = Graph::<NodeData, i32>::new();
        let invalid_node = NodeIndex::new(999);

        assert_eq!(dag.get_des_nodes(invalid_node), None);
    }

    #[test]
    fn get_parallel_process_nodes_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "parallel_process", 0));
        let n1 = dag.add_node(create_node(1, "parallel_process", 0));
        let n2 = dag.add_node(create_node(2, "parallel_process", 0));
        let n3 = dag.add_node(create_node(3, "parallel_process", 0));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);

        assert_eq!(dag.get_parallel_process_nodes(n2), Some(vec![n1, n3]));
        assert_eq!(dag.get_parallel_process_nodes(n3), Some(vec![n2]));
    }

    #[test]
    fn get_parallel_process_nodes_no_exist_parallel_process_nodes() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "parallel_process", 0));

        assert_eq!(dag.get_parallel_process_nodes(n0), None);
    }

    #[test]
    fn test_get_dag_id_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "dag_id", 0));
        assert_eq!(dag.get_dag_id(), 0);
    }

    #[test]
    #[should_panic]
    fn test_get_dag_id_no_exist_node() {
        let dag = Graph::<NodeData, i32>::new();
        dag.get_dag_id();
    }

    #[test]
    fn test_set_dag_id_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node(create_node(0, "execution_time", 0));
        dag.add_node(create_node(1, "execution_time", 0));
        dag.set_dag_id(0);

        for node_i in dag.node_indices() {
            assert_eq!(dag[node_i].params["dag_id"], 0);
        }
    }

    #[test]
    #[should_panic]
    fn test_set_dag_id_no_exist_node() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.set_dag_id(0);
    }

    #[test]
    fn test_add_node_with_id_consistency_normal() {
        let mut dag = Graph::<NodeData, i32>::new();

        let n0 = dag.add_node_with_id_consistency(create_node(0, "execution_time", 3));
        let n1 = dag.add_node_with_id_consistency(create_node(1, "execution_time", 3));

        assert_eq!(dag[n0].id, 0);
        assert_eq!(dag[n1].id, 1);
    }

    #[test]
    #[should_panic]
    fn test_add_node_with_id_consistency_id_duplication() {
        let mut dag = Graph::<NodeData, i32>::new();
        dag.add_node_with_id_consistency(create_node(0, "execution_time", 3));
        dag.add_node_with_id_consistency(create_node(0, "execution_time", 3));
    }

    #[test]
    fn test_is_node_ready_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));
        let n1 = dag.add_node(create_node(1, "execution_time", 0));
        dag.add_edge(n0, n1, 1);

        assert!(dag.is_node_ready(n0));
        assert!(!dag.is_node_ready(n1));
        dag.add_param(n1, "pre_done_count", 1);
        assert!(dag.is_node_ready(n1));
    }

    #[test]
    fn increment_pre_done_count_normal() {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 0));

        dag.increment_pre_done_count(n0);
        assert_eq!(dag[n0].params["pre_done_count"], 1);
        dag.increment_pre_done_count(n0);
        assert_eq!(dag[n0].params["pre_done_count"], 2);
    }
}
