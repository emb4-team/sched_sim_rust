use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct DAG {
    directed: bool,
    graph: HashMap<String, String>,
    links: Vec<Edge>, 
    multigraph: bool,
    nodes: Vec<Nodes>,
}

#[derive(Debug, Deserialize)]
struct Edge {
    Transfer: u32,
    communication_time: u32,
    source: u32,
    target: u32,
}

#[derive(Debug, Deserialize)]
struct Nodes {
    Weight: u32,
    execution_time: u32,
    id: u32,
}

fn main() {
    let mut file = File::open("src/dag_0.yaml").expect("Unable to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).expect("Unable to read file");

    let config: DAG = serde_yaml::from_str(&content).expect("Unable to parse YAML");

    println!("Config: {:?}", config);
}