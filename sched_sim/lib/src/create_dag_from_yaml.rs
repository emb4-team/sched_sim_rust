//! Generate a petgraph DAG object from a yaml file

use std::fs;
use yaml_rust::Yaml;
use yaml_rust::YamlLoader;

use petgraph::graph::Graph;
use petgraph::prelude::*;
use std::collections::HashMap;

fn load_yaml(file_path: &str) -> Vec<yaml_rust::Yaml> {
    if !file_path.ends_with(".yaml") && !file_path.ends_with(".yml") {
        panic!("Invalid file type: {}", file_path);
    }
    let file_content = fs::read_to_string(file_path).unwrap();
    YamlLoader::load_from_str(&file_content).unwrap()
}

/// custom node data structure for dag nodes (petgraph)
pub struct NodeData {
    pub id: u32,
    pub params: HashMap<String, f32>,
}

/// load yaml file and return a dag object (petgraph)
///
/// # Arguments
///
/// *  `path` - yaml file path
///
/// # Returns
///
/// *  `dag` - dag object (petgraph)
///
/// # Example
///
/// ```
/// use lib::create_dag_from_yaml::create_dag_from_yaml;
///
/// let dag = create_dag_from_yaml("../tests/sample_dags/chain_base_format.yaml");
/// let first_node = dag.node_indices().next().unwrap();
/// let first_edge = dag.edge_indices().next().unwrap();
///
/// let node_num = dag.node_count();
/// let edge_num = dag.edge_count();
/// let node_id = dag[first_node].id;
/// let edge_weight = dag[first_edge];
/// ```
pub fn create_dag_from_yaml(file_path: &str) -> Graph<NodeData, f32> {
    let yaml_docs = load_yaml(file_path);
    let yaml_doc = &yaml_docs[0];

    let mut dag = Graph::<NodeData, f32>::new();

    // add nodes to dag
    for node in yaml_doc["nodes"].as_vec().unwrap() {
        let mut params = HashMap::new();
        let id = node["id"].as_i64().unwrap() as u32;

        // add node parameters to HashMap
        for (key, value) in node.as_hash().unwrap() {
            let key_str = key.as_str().unwrap();
            if key_str != "id" {
                match value {
                    Yaml::Integer(_i) => {
                        params.insert(key_str.to_owned(), value.as_i64().unwrap() as f32);
                    }
                    Yaml::Real(_r) => {
                        params.insert(key_str.to_owned(), value.as_f64().unwrap() as f32);
                    }
                    _ => {}
                }
            }
        }
        dag.add_node(NodeData { id, params });
    }

    // add edges to dag
    for link in yaml_doc["links"].as_vec().unwrap() {
        let source = link["source"].as_i64().unwrap() as usize;
        let target = link["target"].as_i64().unwrap() as usize;
        let mut communication_time = 0.0;
        match &link["communication_time"] {
            Yaml::Integer(communication_time_value) => {
                communication_time = *communication_time_value as f32;
            }
            Yaml::Real(communication_time_value) => {
                communication_time = communication_time_value.parse::<f32>().unwrap();
            }
            _ => {}
        }
        dag.add_edge(
            NodeIndex::new(source),
            NodeIndex::new(target),
            communication_time,
        );
    }
    dag
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_dag_from_yaml_normal_chain_base() {
        let dag = create_dag_from_yaml("../tests/sample_dags/chain_base_format.yaml");
        let first_node = dag.node_indices().next().unwrap();
        let last_node = dag.node_indices().last().unwrap();
        let first_edge = dag.edge_indices().next().unwrap();
        let last_edge = dag.edge_indices().last().unwrap();

        assert_eq!(dag.node_count(), 22, "number of nodes is expected to be 22");
        assert_eq!(
            dag[first_node].params.get("execution_time").unwrap(),
            &73.0,
            "first node execution time is expected to be 73.0"
        );
        assert_eq!(
            dag[last_node].params.get("execution_time").unwrap(),
            &2.0,
            "last node execution time is expected to be 2.0"
        );
        assert_eq!(dag[first_node].id, 0, "first node id is expected to be 0");
        assert_eq!(dag[last_node].id, 21, "last node id is expected to be 21");
        assert_eq!(
            dag[first_node].params.get("period").unwrap(),
            &50.0,
            "first node period is expected to be 50.0"
        );
        assert_eq!(dag.edge_count(), 25, "number of edges is expected to be 25");
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        );
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().1].id,
            1,
            "first edge target node id is expected to be 1"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().0].id,
            21,
            "last edge source node id is expected to be 21"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().1].id,
            18,
            "last edge target node id is expected to be 18"
        );
        assert_eq!(
            dag[first_edge], 0.0,
            "first edge weight is expected to be 0.0"
        );
        assert_eq!(
            dag[last_edge], 0.0,
            "last edge weight is expected to be 0.0"
        );
    }

    #[test]
    fn test_create_dag_from_yaml_normal_fan_in_fan_out() {
        let dag = create_dag_from_yaml("../tests/sample_dags/fan_in_fan_out_format.yaml");
        let first_node = dag.node_indices().next().unwrap();
        let last_node = dag.node_indices().last().unwrap();
        let first_edge = dag.edge_indices().next().unwrap();
        let last_edge = dag.edge_indices().last().unwrap();

        assert_eq!(dag.node_count(), 20, "number of nodes is expected to be 20");
        assert_eq!(
            dag[first_node].params.get("Weight").unwrap(),
            &4.0,
            "first node weight is expected to be 4.0"
        );
        assert_eq!(
            dag[last_node].params.get("Weight").unwrap(),
            &1.0,
            "last node weight is expected to be 1.0"
        );
        assert_eq!(
            dag[first_node].params.get("execution_time").unwrap(),
            &3.0,
            "first node execution time is expected to be 3.0"
        );
        assert_eq!(
            dag[last_node].params.get("execution_time").unwrap(),
            &43.0,
            "last node execution time is expected to be 43.0"
        );
        assert_eq!(dag.edge_count(), 29, "number of edges is expected to be 29");
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        );
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().1].id,
            1,
            "first edge target node id is expected to be 1"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().0].id,
            18,
            "last edge source node id is expected to be 18"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().1].id,
            19,
            "last edge target node id is expected to be 19"
        );
        assert_eq!(
            dag[first_edge], 11.0,
            "first edge weight is expected to be 11.0"
        );
        assert_eq!(
            dag[last_edge], 2.0,
            "last edge weight is expected to be 2.0"
        );
    }

    #[test]
    fn test_create_dag_from_yaml_normal_gnp() {
        let dag = create_dag_from_yaml("../tests/sample_dags/gnp_format.yaml");
        let first_node = dag.node_indices().next().unwrap();
        let last_node = dag.node_indices().last().unwrap();
        let first_edge = dag.edge_indices().next().unwrap();
        let last_edge = dag.edge_indices().last().unwrap();

        assert_eq!(dag.node_count(), 70, "number of nodes is expected to be 70");
        assert_eq!(dag[first_node].id, 0, "first node id is expected to be 0");
        assert_eq!(dag[last_node].id, 69, "last node id is expected to be 69");
        assert_eq!(
            dag[first_node].params.get("Weight").unwrap(),
            &1.0,
            "first node weight is expected to be 1.0"
        );
        assert_eq!(
            dag[last_node].params.get("Weight").unwrap(),
            &5.0,
            "last node weight is expected to be 5.0"
        );
        assert_eq!(
            dag[first_node].params.get("execution_time").unwrap(),
            &34.0,
            "first node execution time is expected to be 34.0"
        );
        assert_eq!(
            dag[last_node].params.get("execution_time").unwrap(),
            &1.0,
            "last node execution time is expected to be 1.0"
        );
        assert_eq!(
            dag[first_node].params.get("offset").unwrap(),
            &4.0,
            "first node offset is expected to be 4.0"
        );
        assert_eq!(
            dag[last_node].params.get("offset").unwrap(),
            &5.0,
            "last node offset is expected to be 5.0"
        );
        assert_eq!(
            dag[first_node].params.get("period").unwrap(),
            &6000.0,
            "first node period is expected to be 6000.0"
        );
        assert_eq!(
            dag[last_node].params.get("period").unwrap(),
            &10.0,
            "last node period is expected to be 10.0"
        );
        assert_eq!(
            dag.edge_count(),
            233,
            "number of edges is expected to be 233"
        );
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        );
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().1].id,
            2,
            "first edge target node id is expected to be 2"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().0].id,
            68,
            "last edge source node id is expected to be 68"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().1].id,
            14,
            "last edge target node id is expected to be 14"
        );
        assert_eq!(
            dag[first_edge], 0.0,
            "first edge weight is expected to be 0.0"
        );
        assert_eq!(
            dag[last_edge], 0.0,
            "last edge weight is expected to be 0.0"
        );
    }

    #[test]
    fn test_create_dag_from_yaml_normal_float_params() {
        let dag = create_dag_from_yaml("../tests/sample_dags/float_params.yaml");
        let first_node = dag.node_indices().next().unwrap();
        let last_node = dag.node_indices().last().unwrap();
        let first_edge = dag.edge_indices().next().unwrap();
        let last_edge = dag.edge_indices().last().unwrap();

        assert_eq!(dag.node_count(), 3, "number of nodes is expected to be 3");
        assert_eq!(
            dag[first_node].params.get("Weight").unwrap(),
            &4.1,
            "first node weight is expected to be 4.1"
        );
        assert_eq!(
            dag[last_node].params.get("Weight").unwrap(),
            &1.0,
            "last node weight is expected to be 1.0"
        );
        assert_eq!(
            dag[first_node].params.get("execution_time").unwrap(),
            &3.1,
            "first node execution time is expected to be 3.1"
        );
        assert_eq!(
            dag[last_node].params.get("execution_time").unwrap(),
            &43.0,
            "last node execution time is expected to be 43.0"
        );
        assert_eq!(dag.edge_count(), 2, "number of edges is expected to be 2");
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().0].id,
            0,
            "first edge source node id is expected to be 0"
        );
        assert_eq!(
            dag[dag.edge_endpoints(first_edge).unwrap().1].id,
            1,
            "first edge target node id is expected to be 1"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().0].id,
            1,
            "last edge source node id is expected to be 1"
        );
        assert_eq!(
            dag[dag.edge_endpoints(last_edge).unwrap().1].id,
            2,
            "last edge target node id is expected to be 19"
        );
        assert_eq!(
            dag[first_edge], 11.1,
            "first edge weight is expected to be 11.1"
        );
        assert_eq!(
            dag[last_edge], 2.0,
            "last edge weight is expected to be 2.0"
        );
    }

    #[test]
    #[should_panic]
    fn test_create_dag_from_yaml_disable_path() {
        let _dag = create_dag_from_yaml("../tests/sample_dags/disable_path.yaml");
    }

    #[test]
    #[should_panic]
    fn test_create_dag_from_yaml_disable_no_yaml() {
        let _dag = create_dag_from_yaml("../tests/sample_dags/no_yaml.tex");
    }

    #[test]
    #[should_panic]
    fn test_create_dag_from_yaml_disable_broken_link() {
        let _dag = create_dag_from_yaml("../tests/sample_dags/broken_link.yaml");
    }
}
