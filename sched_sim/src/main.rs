use clap::Parser;
use lib::dag_creator::create_dag_from_yaml;
use lib::dag_creator::create_task_from_folder;

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
    #[clap(short = 'd', long = "dag_file_path", required = true)]
    dag_file_path: String,
}

/// Application main function
fn main() {
    let arg: AppArg = AppArg::parse();
    create_dag_from_yaml(&arg.dag_file_path);
}
