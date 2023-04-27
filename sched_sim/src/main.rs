use clap::Parser;

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
    let _arg: AppArg = AppArg::parse();
}
