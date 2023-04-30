pub struct Core {
    core_id: i32,
    idle: bool,
    proc_node: i32,
    remain_proc_time: i32,
}

impl Core {
    pub fn new(core_id: i32) -> Self {
        Self {
            core_id,
            idle: true,
            proc_node: -1,
            remain_proc_time: 0,
        }
    }

    pub fn allocate(&mut self, node_i: i32, exec_time: i32) {
        self.idle = false;
        self.proc_node = node_i;
        self.remain_proc_time = exec_time;
        println!(
            "Core {} is allocated to node {} for {} cycles",
            self.core_id, node_i, exec_time
        );
    }

    pub fn process(&mut self) {
        if !self.idle {
            self.remain_proc_time -= 1;
            if self.remain_proc_time == 0 {
                self.idle = true;
                self.proc_node = -1;
            }
        }
    }
}
