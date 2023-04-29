use algo::federated::*;
use clap::Parser;
use lib::dag_creator::*;

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
}

/// Application main function
fn main() {
    let arg: AppArg = AppArg::parse();
    if let Some(dag_file_path) = arg.dag_file_path {
        let _dag = create_dag_from_yaml(&dag_file_path);
    } else if let Some(dag_dir_path) = arg.dag_dir_path {
        let _dag_set = create_dag_set_from_dir(&dag_dir_path);
        federated(&dag_dir_path, 10);
    }
}
