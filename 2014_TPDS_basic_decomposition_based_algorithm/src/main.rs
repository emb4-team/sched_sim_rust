mod decomposition;
mod handle_segment;
use clap::Parser;

use decomposition::decompose;
/*use lib::homogeneous::HomogeneousProcessor;

use lib::processor::ProcessorBase;
use lib::scheduler::DAGSchedulerBase;*/
use lib::dag_creator::*;
//use log::warn;

#[derive(Parser)]
#[clap(name = "CPC_Model_Based_Algorithm", version = "1.0")]
struct ArgParser {
    ///Path to DAG file.
    #[clap(short = 'f', long = "dag_file_path", required = true)]
    dag_file_path: String,
    ///Number of processing cores.
    #[clap(short = 'c', long = "number_of_cores", required = true)]
    number_of_cores: usize,
    ///Path to output directory.
    #[clap(short = 'o', long = "output_dir_path", default_value = "../outputs")]
    output_dir_path: String,
}

fn main() {
    let arg: ArgParser = ArgParser::parse();

    let mut dag = create_dag_from_yaml(&arg.dag_file_path);
    decompose(&mut dag);

    //let homogeneous_processor = HomogeneousProcessor::new(arg.number_of_cores);
    /*let mut fixed_priority_scheduler = FixedPriorityScheduler::new(&dag, &homogeneous_processor);
    let (schedule_length, _) = fixed_priority_scheduler.schedule();

    let result = (schedule_length as f32) < dag.get_head_period().unwrap() as f32;
    let file_path = fixed_priority_scheduler.dump_log(&arg.output_dir_path, "basic_decomposition");
    */

    /*
    dump_dag_scheduler_result_to_yaml(
        &file_path,
        schedule_length,
        arg.ratio_deadline_to_period,
        result,
    );
    */
}
