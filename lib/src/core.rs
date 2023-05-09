use petgraph::graph::NodeIndex;

pub struct Core {
    core_id: i32,
    idle: bool,
    process_node: Option<NodeIndex>,
    remain_proc_time: i32,
}

impl Core {
    pub fn new(core_id: i32) -> Self {
        Self {
            core_id,
            idle: true,
            process_node: None,
            remain_proc_time: 0,
        }
    }

    pub fn allocate(&mut self, node_i: Option<NodeIndex>, exec_time: i32) {
        self.idle = false;
        self.process_node = node_i;
        self.remain_proc_time = exec_time;
    }

    pub fn process(&mut self) {
        if !self.idle {
            self.remain_proc_time -= 1;
            if self.remain_proc_time == 0 {
                self.idle = true;
                self.process_node = None;
            }
        }
    }
}
