//! library for scheduling simulation.
extern crate petgraph;
extern crate yaml_rust;

use petgraph::graph::Graph;
use petgraph::prelude::*;
use std::collections::HashMap;
use std::fs;
use yaml_rust::YamlLoader;

/// custom node data structure for graph nodes (petgraph)
pub struct NodeData {
    pub id: u32,
    pub params: HashMap<String, u32>,
}

/// load yaml file and return a vector of Yaml objects
///
/// # Arguments
///
/// *  `path` - yaml file path
///
/// # Returns
///
/// *  `docs` - graph object (vector of Yaml objects)
///
/// # Example
///
/// ```
/// use lib::load_yaml;
/// let chainbase_yaml = load_yaml("../tests/sample_dags/chainbase.yaml"); //load chainbase.yaml
/// ```
///

pub fn load_yaml(path: &str) -> Vec<yaml_rust::Yaml> {
    let f = fs::read_to_string(path);
    let s = f.unwrap();
    YamlLoader::load_from_str(&s).unwrap()
}

/// load yaml file and return a graph object (petgraph)
///
/// # Arguments
///
/// *  `path` - yaml file path
///
/// # Returns
///
/// *  `graph` - graph object (petgraph)
///
/// # Example
///
/// ```
/// use lib::load_graph_from_yaml;
///
/// let graph = load_graph_from_yaml("../tests/sample_dags/chainbase.yaml"); //load chainbase.yaml as a graph object
/// let first_node = graph.node_indices().next().unwrap();
/// let first_edge = graph.edge_indices().next().unwrap();
///
/// let node_num = graph.node_count();
/// let edge_num = graph.edge_count();
/// let node_id = graph[first_node].id;
/// let edge_weight = graph[first_edge];
/// ```
///

pub fn load_graph_from_yaml(path: &str) -> Graph<NodeData, i32> {
    let path = path;
    let docs = load_yaml(path);
    let doc = &docs[0];

    let mut graph = Graph::<NodeData, i32>::new();

    // add nodes to graph
    for node in doc["nodes"].as_vec().unwrap() {
        let mut params = HashMap::new();
        let id = node["id"].as_i64().unwrap() as u32;

        // add node parameters to HashMap
        for (key, value) in node.as_hash().unwrap() {
            let key_str = key.as_str().unwrap();
            if key_str != "id" {
                params.insert(key_str.to_owned(), value.as_i64().unwrap() as u32);
            }
        }

        let _node_a = graph.add_node(NodeData { id, params });
    }

    // add edges to graph
    for link in doc["links"].as_vec().unwrap() {
        let source = link["source"].as_i64().unwrap() as u32;
        let target = link["target"].as_i64().unwrap() as u32;
        let mut communication_time = 0;

        if let Some(communication_time_value) = link["communication_time"].as_i64() {
            communication_time = communication_time_value as i32;
        }

        let mut source_node_index = NodeIndex::end();
        let mut target_node_index = NodeIndex::end();

        for node in graph.node_indices() {
            if graph[node].id == source {
                source_node_index = node;
            }
            if graph[node].id == target {
                target_node_index = node;
            }
            if source_node_index != NodeIndex::end() && target_node_index != NodeIndex::end() {
                break;
            }
        }
        graph.add_edge(source_node_index, target_node_index, communication_time);
    }

    graph
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_dag_yaml() {
        let chainbase_yaml = load_yaml("../tests/sample_dags/chainbase.yaml");
        let faninout_yaml = load_yaml("../tests/sample_dags/faninout.yaml");
        let gnp_yaml = load_yaml("../tests/sample_dags/Gnp.yaml");

        //check chainbase yaml file
        assert!(
            chainbase_yaml[0]["directed"].as_bool().unwrap(),
            "directed is expected to be true"
        ); // check directed
        assert!(
            !chainbase_yaml[0]["multigraph"].as_bool().unwrap(),
            "multigraph is expected to be false"
        ); // check multigraph
        assert_eq!(
            chainbase_yaml[0]["nodes"].as_vec().unwrap().len(),
            22,
            "number of nodes is expected to be 22"
        ); // check number of nodes
        assert_eq!(
            chainbase_yaml[0]["links"].as_vec().unwrap().len(),
            25,
            "number of edges is expected to be 25"
        ); // check number of edges

        // check faninout yaml file
        assert!(
            faninout_yaml[0]["directed"].as_bool().unwrap(),
            "directed is expected to be true"
        ); // check directed
        assert!(
            !faninout_yaml[0]["multigraph"].as_bool().unwrap(),
            "multigraph is expected to be false"
        ); // check multigraph
        assert_eq!(
            faninout_yaml[0]["nodes"].as_vec().unwrap().len(),
            20,
            "number of nodes is expected to be 20"
        ); // check number of nodes
        assert_eq!(
            faninout_yaml[0]["links"].as_vec().unwrap().len(),
            29,
            "number of edges is expected to be 29"
        ); // check number of edges

        //check gnp yaml file
        assert!(
            gnp_yaml[0]["directed"].as_bool().unwrap(),
            "directed is expected to be true"
        ); // check directed
        assert!(
            !gnp_yaml[0]["multigraph"].as_bool().unwrap(),
            "multigraph is expected to be false"
        ); // check multigraph
        assert_eq!(
            gnp_yaml[0]["nodes"].as_vec().unwrap().len(),
            70,
            "number of nodes is expected to be 70"
        ); // check number of nodes
        assert_eq!(
            gnp_yaml[0]["links"].as_vec().unwrap().len(),
            233,
            "number of edges is expected to be 233"
        ); // check number of edges
    }

    #[test]
    fn load_chainbase_graph_from_yaml() {
        let graph = load_graph_from_yaml("../tests/sample_dags/chainbase.yaml");
        let first_node = graph.node_indices().next().unwrap();
        let last_node = graph.node_indices().last().unwrap();
        let first_edge = graph.edge_indices().next().unwrap();
        let last_edge = graph.edge_indices().last().unwrap();

        assert_eq!(
            graph.node_count(),
            22,
            "number of nodes is expected to be 22"
        ); // check number of nodes
        assert_eq!(
            graph.edge_count(),
            25,
            "number of edges is expected to be 25"
        ); // check number of edges
        assert_eq!(
            graph[first_node].params.get("execution_time").unwrap(),
            &73,
            "first node execution time is expected to be 73"
        ); // check first node execution time
        assert_eq!(
            graph[last_node].params.get("execution_time").unwrap(),
            &2,
            "last node execution time is expected to be 2"
        ); // check last node execution time
        assert_eq!(graph[first_node].id, 0, "first node id is expected to be 0"); // check first node id
        assert_eq!(graph[last_node].id, 21, "last node id is expected to be 21"); // check last node id
        assert_eq!(
            graph[first_node].params.get("period").unwrap(),
            &50,
            "first node period is expected to be 50"
        ); // check first node period
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        ); // check first edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().1].id,
            1,
            "first edge target node id is expected to be 1"
        ); // check first edge target node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().0].id,
            21,
            "last edge source node id is expected to be 21"
        ); // check last edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().1].id,
            18,
            "last edge target node id is expected to be 18"
        ); // check last edge target node id
        assert_eq!(
            graph[first_edge], 0,
            "first edge weight is expected to be 0"
        ); // check first edge weight
        assert_eq!(graph[last_edge], 0, "last edge weight is expected to be 0");
        // check last edge weight
    }

    #[test]
    fn load_faninout_graph_from_yaml() {
        let graph = load_graph_from_yaml("../tests/sample_dags/faninout.yaml");
        let first_node = graph.node_indices().next().unwrap();
        let last_node = graph.node_indices().last().unwrap();
        let first_edge = graph.edge_indices().next().unwrap();
        let last_edge = graph.edge_indices().last().unwrap();

        assert_eq!(
            graph.node_count(),
            20,
            "number of nodes is expected to be 20"
        ); // check number of nodes
        assert_eq!(
            graph.edge_count(),
            29,
            "number of edges is expected to be 29"
        ); // check number of edges
        assert_eq!(
            graph[first_node].params.get("Weight").unwrap(),
            &4,
            "first node weight is expected to be 4"
        ); // check first node weight
        assert_eq!(
            graph[last_node].params.get("Weight").unwrap(),
            &1,
            "last node weight is expected to be 1"
        ); // check last node weight
        assert_eq!(
            graph[first_node].params.get("execution_time").unwrap(),
            &3,
            "first node execution time is expected to be 3"
        ); // check first node execution time
        assert_eq!(
            graph[last_node].params.get("execution_time").unwrap(),
            &43,
            "last node execution time is expected to be 43"
        ); // check last node execution time
        assert_eq!(graph[first_node].id, 0, "first node id is expected to be 0"); // check first node id
        assert_eq!(graph[last_node].id, 19, "last node id is expected to be 19"); // check last node id
        assert_eq!(
            graph[last_node].params.get("end_to_end_deadline").unwrap(),
            &402,
            "last node end to end deadline is expected to be 402"
        ); // check last node end to end deadline
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        ); // check first edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().1].id,
            1,
            "first edge target node id is expected to be 1"
        ); // check first edge target node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().0].id,
            18,
            "last edge source node id is expected to be 18"
        ); // check last edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().1].id,
            19,
            "last edge target node id is expected to be 19"
        ); // check last edge target node id
        assert_eq!(graph[first_edge], 11); // check first edge weight
        assert_eq!(graph[last_edge], 2); // check last edge weight
    }
    #[test]
    fn load_gnp_graph_from_yaml() {
        let graph = load_graph_from_yaml("../tests/sample_dags/Gnp.yaml");
        let first_node = graph.node_indices().next().unwrap();
        let last_node = graph.node_indices().last().unwrap();
        let first_edge = graph.edge_indices().next().unwrap();
        let last_edge = graph.edge_indices().last().unwrap();

        assert_eq!(
            graph.node_count(),
            70,
            "number of nodes is expected to be 70"
        ); // check number of nodes
        assert_eq!(
            graph.edge_count(),
            233,
            "number of edges is expected to be 233"
        ); // check number of edges
        assert_eq!(graph[first_node].id, 0, "first node id is expected to be 0"); // check first node id
        assert_eq!(graph[last_node].id, 69, "last node id is expected to be 69"); // check last node id
        assert_eq!(
            graph[first_node].params.get("Weight").unwrap(),
            &1,
            "first node weight is expected to be 1"
        ); // check first node weight
        assert_eq!(
            graph[last_node].params.get("Weight").unwrap(),
            &5,
            "last node weight is expected to be 5"
        ); // check last node weight
        assert_eq!(
            graph[first_node].params.get("execution_time").unwrap(),
            &34,
            "first node execution time is expected to be 34"
        ); // check first node execution time
        assert_eq!(
            graph[last_node].params.get("execution_time").unwrap(),
            &1,
            "last node execution time is expected to be 1"
        ); // check last node execution time
        assert_eq!(
            graph[first_node].params.get("offset").unwrap(),
            &4,
            "first node offset is expected to be 4"
        ); // check first node offset
        assert_eq!(
            graph[last_node].params.get("offset").unwrap(),
            &5,
            "last node offset is expected to be 5"
        ); // check last node offset
        assert_eq!(
            graph[first_node].params.get("period").unwrap(),
            &6000,
            "first node period is expected to be 6000"
        ); // check first node period
        assert_eq!(
            graph[last_node].params.get("period").unwrap(),
            &10,
            "last node period is expected to be 10"
        ); // check last node period
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        ); // check first edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(first_edge).unwrap().1].id,
            2,
            "first edge target node id is expected to be 2"
        ); // check first edge target node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().0].id,
            68,
            "last edge source node id is expected to be 68"
        ); // check last edge source node id
        assert_eq!(
            graph[graph.edge_endpoints(last_edge).unwrap().1].id,
            14,
            "last edge target node id is expected to be 14"
        ); // check last edge target node id
        assert_eq!(
            graph[first_edge], 0,
            "first edge weight is expected to be 0"
        ); // check first edge weight
        assert_eq!(graph[last_edge], 0, "last edge weight is expected to be 0");
        // check last edge weight
    }
}
