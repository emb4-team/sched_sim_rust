use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::Graph;
use std::collections::HashMap;

#[allow(dead_code, clippy::ptr_arg)] // TODO: remove
pub fn dynfed(dag_set: &mut Vec<Graph<NodeData, i32>>) {
    let mut current_time = 0;

    let hyper_period = 120; //TODO: get_hyper_period(dag_set);
    let mut dag_fifo = Vec::new();
    let mut dag_number = Vec::new();
    //let number_sub_job_cores: HashMap<usize, i32> = HashMap::new();
    //let number_dag_cores: HashMap<usize, i32> = HashMap::new();
    let mut is_dag_started: HashMap<usize, bool> = HashMap::new();

    for (i, dag) in dag_set.iter().enumerate() {
        dag_number[i] = dag;
        is_dag_started.insert(i, false);
    }

    while current_time < hyper_period {
        for dag in dag_number.iter() {
            if dag.get_head_period().unwrap() == current_time {
                dag_fifo.push(dag);
            }
        }
        //1つ目
        //2つ目
        //3つ目
        current_time += 1;
    }
}

#[cfg(test)]
mod tests {}
