extern crate yaml_rust;
extern crate petgraph;

use petgraph::graph::Graph;
use petgraph::prelude::*; // for NodeIndex
// use yaml_rust::{YamlLoader, YamlEmitter};
use yaml_rust::{YamlLoader};
use std::fs;
use std::collections::HashMap;

// custom node data structure for graph nodes (petgraph) 
pub struct NodeData {
    pub id: u32,
    pub params: HashMap<String, u32>,
}

// load yaml file and return a vector of Yaml objects 
pub fn load_yaml(path: &str) -> Vec<yaml_rust::Yaml> {
    let f = fs::read_to_string(path);
    let s = f.unwrap().to_string();
    let docs = YamlLoader::load_from_str(&s).unwrap();
    docs
}

pub fn load_graph_from_yaml(path: &str) -> Graph::<NodeData, i32> {
    let path = path; // Path to YAML file
    let docs = load_yaml(&path); // Load YAML file
    let doc = &docs[0]; // Get the first YAML object

    // create a graph object 
    let mut graph = Graph::<NodeData, i32>::new();

    // add nodes to the graph
    for node in doc["nodes"].as_vec().unwrap() { 
        let mut params = HashMap::new(); // create a HashMap to store node parameters
        let id = node["id"].as_i64().unwrap() as u32; // get node id
    
        // add node parameters to the HashMap
        for (key, value) in node.as_hash().unwrap() {
            let key_str = key.as_str().unwrap();
            if key_str != "id" { // add all parameters except id
                //println!("key: {:?}, value: {:?}", key_str, value.as_i64().unwrap() as u32);
                params.insert(key_str.to_owned(), value.as_i64().unwrap() as u32); // add key and value to the HashMap
            }
        }
        
        // add node to the graph
        let _node_a = graph.add_node(NodeData { 
            id, 
            params, 
        });
    }



    // add edges to the graph
    for link in doc["links"].as_vec().unwrap() {
        let source = link["source"].as_i64().unwrap() as u32;
        let target = link["target"].as_i64().unwrap() as u32;
        let mut communication_time = 0; //どちらにせよ0になるため、ここで初期化しておく
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