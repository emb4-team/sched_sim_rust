use clap::Parser;
use lib::dag_creator::*;
mod federated;
use lib::output::*;

/// Application description and arguments definition using clap crate
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
}

/// Application main function
fn main() {
    let arg: AppArg = AppArg::parse();
    create_yaml_file("../outputs", "federated");
    if let Some(dag_dir_path) = arg.dag_dir_path {
        let dag_set = create_dag_set_from_dir(&dag_dir_path);
        federated::federated(dag_set, arg.number_of_cores);
    }
}
