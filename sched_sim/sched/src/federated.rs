use lib::dag_creator::create_dag_set_from_dir;
use lib::dag_handler::get_critical_paths;

pub fn federated(folder_path: &str) {
    let dag_set = create_dag_set_from_dir(folder_path);
    let mut deadlines: Vec<f32> = Vec::new();
    let mut sum_wects: Vec<f32> = Vec::new();
    for dag in dag_set {
        let last_node = dag.node_indices().last().unwrap();
        let deadline = dag[last_node].params["end_to_end_deadline"];
        deadlines.push(deadline);
        //全てのノードのexecution_timeの合計を計算
        let mut sum_wect = 0.0;
        for node in dag.node_indices() {
            sum_wect += dag[node].params["execution_time"];
        }
        sum_wects.push(sum_wect);
    }
    println!("deadlines: {:?}", deadlines);
    println!("sum_wects: {:?}", sum_wects);
}
