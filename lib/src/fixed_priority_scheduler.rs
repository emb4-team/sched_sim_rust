use crate::{homogeneous, processor::ProcessorBase};
use petgraph::graph::NodeIndex;

fn fixed_priority_scheduler(
    processor: &mut homogeneous::HomogeneousProcessor,
    running: &mut Option<NodeIndex>,
    task_list: Vec<NodeIndex>,
) {
    // Add newly ready processes to the ready queue.

    // If the running process is done, remove it.

    // If there is no running process, choose one from the ready queue.

    // If there is a running process, run it for one time unit.

    // If there are no processes left, we are done.

    // Advance the time.
}
