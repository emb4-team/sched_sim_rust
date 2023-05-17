use chrono::Utc;
use clap::Parser;
use lib::dag_creator::*;
use lib::output_log::*;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;

mod federated;

/// Application descriptions and arguments definition using clap crate
#[derive(Parser)]
#[clap(
    name = "sched_sim",
    author = "Yutaro kobayashi",
    version = "v1.0.0",
    about = "Application short description."
)]

/// Application arguments definition using clap crate
struct AppArg {
    #[clap(short = 'f', long = "dag_file_path", required = false)]
    dag_file_path: Option<String>,
    #[clap(short = 'd', long = "dag_dir_path", required = false)]
    dag_dir_path: Option<String>,
    #[clap(short = 'c', long = "number_of_cores", required = true)]
    number_of_cores: usize,
    #[clap(short = 'o', long = "output_dir_path", default_value = "../outputs")]
    output_dir_path: String,
}

pub fn append_federate_result_to_file<T: Serialize>(file_path: &str, result: &T) {
    if let Ok(result_yaml) = serde_yaml::to_string(result) {
        if let Ok(mut file) = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
        {
            if let Err(err) = writeln!(file, "{}", result_yaml) {
                eprintln!("Failed to write to file: {}", err);
            }
        } else {
            eprintln!("Failed to open file: {}", file_path);
        }
    } else {
        eprintln!("Failed to serialize result to YAML");
    }
}

/// Application main function
fn main() {
    let arg: AppArg = AppArg::parse();
    if let Some(dag_dir_path) = arg.dag_dir_path {
        let number_of_cores = arg.number_of_cores;
        let dag_set = create_dag_set_from_dir(&dag_dir_path);
        let result = federated::federated(dag_set.clone(), number_of_cores);
        let date = Utc::now().format("%Y-%m-%d").to_string();
        let file_name = format!("{}-federated", date);
        let file_path = create_yaml_file(&arg.output_dir_path, &file_name);
        dump_dag_set_info_to_yaml(&file_path, dag_set);
        append_info_to_yaml(&file_path, number_of_cores.to_string().as_str());
        append_federate_result_to_file(&file_path, &result);
    }
}

#[cfg(test)]

mod tests {
    use chrono::DateTime;
    use lib::graph_extension::NodeData;
    use petgraph::Graph;

    use super::*;
    use std::collections::HashMap;

    fn create_node(id: i32, key: &str, value: f32) -> NodeData {
        let mut params = HashMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    fn create_high_utilization_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 4.0);
            params.insert("period".to_owned(), 10.0);
            dag.add_node(NodeData { id: 3, params })
        };
        let n1 = dag.add_node(create_node(1, "execution_time", 4.0));
        let n2 = dag.add_node(create_node(2, "execution_time", 3.0));
        let n3 = dag.add_node(create_node(3, "execution_time", 3.0));
        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag.add_edge(n0, n3, 1.0);

        dag
    }

    fn create_low_utilization_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let n0 = {
            let mut params = HashMap::new();
            params.insert("execution_time".to_owned(), 3.0);
            params.insert("period".to_owned(), 30.0);
            dag.add_node(NodeData { id: 2, params })
        };
        let n1 = dag.add_node(create_node(0, "execution_time", 3.0));
        let n2 = dag.add_node(create_node(1, "execution_time", 4.0));

        dag.add_edge(n0, n1, 1.0);
        dag.add_edge(n0, n2, 1.0);
        dag
    }

    fn create_period_exceeding_dag() -> Graph<NodeData, f32> {
        let mut dag = Graph::<NodeData, f32>::new();
        let mut params = HashMap::new();
        params.insert("execution_time".to_owned(), 20.0);
        params.insert("period".to_owned(), 10.0);
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    #[test]
    fn test_dump_federated_info_normal() {
        let number_of_cores = 40;
        let dag_set = vec![
            create_high_utilization_dag(),
            create_high_utilization_dag(),
            create_low_utilization_dag(),
        ];
        let result = federated::federated(dag_set.clone(), number_of_cores);
        let now: DateTime<Utc> = Utc::now();
        let date = now.format("%Y-%m-%d-%H-%M-%S").to_string();
        let file_name = format!("{}-federated", date);
        let file_path = create_yaml_file("../outputs", &file_name);
        dump_dag_set_info_to_yaml(&file_path, dag_set);
        dump_number_of_cores_to_yaml(&file_path, number_of_cores);
        append_federate_result_to_file(&file_path, &result);
    }
}
