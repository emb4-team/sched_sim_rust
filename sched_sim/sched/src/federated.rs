use lib::dag_creator::create_dag_set_from_dir;

pub fn federated() {
    let dag_set = create_dag_set_from_dir("../tests/sample_dags/multiple_yaml_files");
}
