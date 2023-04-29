use lib::dag_creator::create_dag_set_from_dir;
use lib::dag_handler::get_critical_paths;

pub fn federated(folder_path: &str) {
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut deadlines = vec![0.0; dag_set.len()];
    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let last_node_execution_time = dag[last_node].params["deadline"];
        let node_id = dag[last_node].id;
        deadlines[node_id as usize] = last_node_execution_time;
    }
}
