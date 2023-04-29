use lib::dag_creator::create_dag_set_from_dir;
use lib::dag_handler::get_critical_paths;

pub fn federated(folder_path: &str) {
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut deadlines: Vec<f32> = Vec::new();
    let mut sum_wects: Vec<f32> = Vec::new();
    let mut critical_path_wects: Vec<f32> = Vec::new();

    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let mut sum_wect = 0.0;
        deadlines.push(dag[last_node].params["end_to_end_deadline"]);

        for node in dag.node_indices() {
            sum_wect += dag[node].params["execution_time"];
        }
        sum_wects.push(sum_wect);

        let mut critical_paths = get_critical_paths(dag.clone());
        critical_paths[0].pop();
        critical_paths[0].remove(0);

        critical_path_wects.push(
            critical_paths[0]
                .iter()
                .map(|node| dag[*node].params["execution_time"])
                .sum(),
        );
    }
    println!("deadlines: {:?}", deadlines);
    println!("sum_wects: {:?}", sum_wects);
    println!("critical_path_wects: {:?}", critical_path_wects);
}
