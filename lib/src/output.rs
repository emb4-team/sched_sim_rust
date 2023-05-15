use log::warn;
use petgraph::Graph;
use serde_derive::Serialize;
use serde_yaml;
use std::fs;
use std::io::Write;

use crate::graph_extension::NodeData;

#[derive(Serialize)]
struct SerializableGraph {
    nodes: Vec<NodeData>,
    edges: Vec<EdgeData>,
}

#[derive(Serialize)]
struct EdgeData {
    communication_time: f32,
    source: usize,
    target: usize,
}

fn graph_to_yaml(graph: &Graph<NodeData, f32>) -> Result<String, serde_yaml::Error> {
    let serializable_graph = SerializableGraph {
        nodes: graph
            .node_indices()
            .map(|index| graph[index].clone())
            .collect(),
        edges: graph
            .edge_indices()
            .map(|index| {
                let edge = graph.edge_endpoints(index).unwrap();
                EdgeData {
                    communication_time: *graph.edge_weight(index).unwrap(),
                    source: edge.0.index(),
                    target: edge.1.index(),
                }
            })
            .collect(),
    };

    serde_yaml::to_string(&serializable_graph)
}

pub fn create_yaml_file(folder_path: &str, file_name: &str) -> String {
    let file_path = format!("{}/{}.yaml", folder_path, file_name);
    print!("Create file: {}", file_path);
    if let Err(err) = fs::File::create(&file_path) {
        warn!("Failed to create file: {}", err);
    }
    file_path
}

pub fn graph_to_yaml_file(graph: &Graph<NodeData, f32>, file_path: &str) -> std::io::Result<()> {
    let serialized_graph = graph_to_yaml(graph).unwrap();
    let mut file = fs::File::create(file_path)?;
    file.write_all(serialized_graph.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    #[test]
    fn test() {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4.0));
        let n1 = dag.add_node(create_node(1, "execution_time", 7.0));
        dag.add_edge(n0, n1, 1.0);

        let file_path = create_yaml_file("../outputs", "test");
        let _ = graph_to_yaml_file(&dag, &file_path);
    }
}
