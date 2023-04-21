extern crate yaml_rust;
extern crate petgraph;

use petgraph::graph::Graph;
use yaml_rust::{YamlLoader, YamlEmitter};
use std::fs;
use std::collections::HashMap;

// custom node data structure for graph nodes (petgraph) 
struct NodeData {
    id: u32,
    params: HashMap<String, u32>,
}

// load yaml file and return a vector of Yaml objects 
fn load_yaml(path: &str) -> Vec<yaml_rust::Yaml> {
    let f = fs::read_to_string(path);
    let s = f.unwrap().to_string();
    let docs = YamlLoader::load_from_str(&s).unwrap();
    docs
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "src/dag_1.yaml"; // Path to YAML file
    let docs = load_yaml(&path); // Load YAML file
    let doc = &docs[0]; // Get the first YAML object

    // create a graph object 
    let mut graph = Graph::<NodeData, i32>::new();

    for node in doc["nodes"].as_vec().unwrap() { 
        let mut params = HashMap::new(); // create a HashMap to store node parameters
        let id = node["id"].as_i64().unwrap() as u32; // get node id
    
        // add node parameters to the HashMap
        for (key, value) in node.as_hash().unwrap() {
            let key_str = key.as_str().unwrap();
            if key_str != "id" { // add all parameters except id
                params.insert(key_str.to_owned(), value.as_i64().unwrap() as u32); // add key and value to the HashMap
            }
        }
        
        // add node to the graph
        let node_a = graph.add_node(NodeData { 
            id, 
            params, 
        });

        /* 
        // start of test
        //print node id and weight 
        let node_a_data = graph.node_weight(node_a).unwrap(); // get node data, node_a is the node index
        let id = node_a_data.id;
        println!("node_a id: {:?}", id);
        let execution_time = node_a_data.params.get("Weight").unwrap(); // get weight
        println!("Weight: {:?}", execution_time);
        // print weight (if weight is not found, print "Weight is not found")
        match node_a_data.params.get("Weight") {
            Some(execution_time) => println!("Weight: {:?}", execution_time),
            None => println!("Weight is not found"),
        }
        // end of test
        */
    }

    Ok(())
} 