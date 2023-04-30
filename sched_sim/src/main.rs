use clap::Parser;
use lib::dag_creator::*;
use processors::homogeneous::HomogeneousProcessor;

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
    // 4コアを持つホモジニアスプロセッサを作成
    let mut processor = HomogeneousProcessor::new(4);

    // タスクを割り当てる
    processor.allocate(0, 42, 5); // コア0にノード42を割り当て、実行時間を5に設定
    processor.allocate(1, 10, 3); // コア1にノード10を割り当て、実行時間を3に設定

    // 5タイムステップ分、タスクの処理を進める
    for _ in 0..5 {
        processor.process();
    }
    let arg: AppArg = AppArg::parse();
    if let Some(dag_file_path) = arg.dag_file_path {
        let _dag = create_dag_from_yaml(&dag_file_path);
    } else if let Some(dag_dir_path) = arg.dag_dir_path {
        let _dag_set = create_dag_set_from_dir(&dag_dir_path);
    }
}
