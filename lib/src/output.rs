use petgraph::Graph;
use serde_yaml;
use std::fs;
use std::io::{self, Write};

use crate::graph_extension::NodeData;

impl<N, E> Serialize for petgraph::Graph<N, E>
where
    N: Serialize,
    E: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Graphのシリアライズ処理を実装する
        // ノードやエッジをシリアライズする必要がある場合は、それぞれのフィールドに対してserdeの関数を呼び出すなどの処理を行う
        // シリアライズの結果をserializerに返す
    }
}

pub fn create_yaml_file(folder_path: &str, file_name: &str) -> std::io::Result<()> {
    let file_path = format!("{}/{}.yaml", folder_path, file_name);
    fs::File::create(file_path)?;
    Ok(())
}

fn graph_to_yaml_file(graph: &Graph<NodeData, f32>, file_path: &str) -> std::io::Result<()> {
    let serialized_graph = serde_yaml::to_string(graph)?;
    let mut file = fs::File::create(file_path)?;
    file.write_all(serialized_graph.as_bytes())?;
    Ok(())
}
