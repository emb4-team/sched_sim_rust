use clap::Parser;
use lib::load_graph_from_yaml;

// Parserを継承した構造体はArgの代わりに使用することが可能。
#[derive(Parser)]
#[clap(
    name = "sched_sim",
    author = "Yutaro kobayashi",
    version = "v1.0.0",
    about = "Application short description."
)]

struct AppArg {
    #[clap(short = 'd', long = "dag_file_path", required = true)]
    /// DAG file path
    file_name: String, 
}

fn main() {    
    let arg: AppArg = AppArg::parse();
    let graph = load_graph_from_yaml(&arg.file_name);
    

    //test code
    for node in graph.node_indices() {
        println!("Node {}: {:?} : {:?}", graph[node].id, node, graph[node].params);
    }

    //test code
    for edge in graph.edge_indices() {
        let source = graph.edge_endpoints(edge).unwrap().0;
        let target = graph.edge_endpoints(edge).unwrap().1;
        let weight = graph[edge];
        println!("Edge from Node {} to Node {} with weight {}", graph[source].id, graph[target].id, weight);
    }

}