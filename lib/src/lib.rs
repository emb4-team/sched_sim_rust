pub mod core;
pub mod dag_creator;
pub mod dag_scheduler;
pub mod dag_set_scheduler;
pub mod fixed_priority_scheduler;
pub mod global_edf_scheduler;
pub mod graph_extension;
pub mod homogeneous;
pub mod log;
pub mod processor;
pub mod util;

#[cfg(any(test, feature = "test-helpers"))]
pub mod tests_helper {
    use crate::{
        graph_extension::{GraphExtension, NodeData},
        util::load_yaml,
    };
    use petgraph::Graph;
    use std::{collections::BTreeMap, fs::remove_file, path::PathBuf};
    use yaml_rust::Yaml;

    #[macro_export]
    macro_rules! assert_yaml_value {
        ($yaml:expr, $path_str:expr, f64, $expected:expr) => {{
            let val = $crate::extract_yaml_value!($yaml, $path_str);
            assert_eq!(val.as_f64().unwrap(), $expected);
        }};
        ($yaml:expr, $path_str:expr, i64, $expected:expr) => {{
            let val = $crate::extract_yaml_value!($yaml, $path_str);
            assert_eq!(val.as_i64().unwrap(), $expected);
        }};
        ($yaml:expr, $path_str:expr, str, $expected:expr) => {{
            let val = $crate::extract_yaml_value!($yaml, $path_str);
            assert_eq!(val.as_str().unwrap(), $expected);
        }};
    }

    #[macro_export]
    macro_rules! assert_yaml_values_for_prefix {
        ($yaml:expr, $prefix:expr, [$(($path_str:expr, $typ:ident, $expected:expr)),*]) => {
            $(
                let path = format!("{}.{}", $prefix, $path_str);
                $crate::assert_yaml_value!($yaml, &path, $typ, $expected);
            )*
        };
    }

    #[macro_export]
    macro_rules! extract_yaml_value {
        ($yaml:expr, $path_str:expr) => {{
            let segments: Vec<&str> = $path_str
                .split(|c| c == '.' || c == '[' || c == ']')
                .filter(|s| !s.is_empty())
                .collect();
            let mut temp_yaml = $yaml;
            for segment in &segments {
                if let Ok(index) = segment.parse::<usize>() {
                    temp_yaml = &temp_yaml[index];
                } else {
                    temp_yaml = &temp_yaml[*segment];
                }
            }
            temp_yaml
        }};
    }

    pub fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    pub fn common_sched_dump_test<F, A>(dump_fn: F, asserts: A)
    where
        F: FnOnce() -> PathBuf,
        A: FnOnce(&Yaml),
    {
        let file_path = dump_fn();

        let yaml_docs = load_yaml(file_path.to_str().unwrap());
        let yaml_doc = &yaml_docs[0];
        asserts(yaml_doc);

        remove_file(&file_path).unwrap();
    }

    #[derive(Clone)]
    struct NodeSpec {
        id: usize,
        exe_time: i32,
    }
    #[derive(Clone)]
    struct EdgeSpec {
        from: usize,
        to: usize,
    }

    fn create_dag(
        root_exec_time: i32,
        root_period: Option<i32>,
        node_specs: Option<&[NodeSpec]>,
        edge_specs: Option<&[EdgeSpec]>,
    ) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();

        let root_params = {
            let mut params = BTreeMap::new();
            params.insert("execution_time".to_owned(), root_exec_time);
            if let Some(period) = root_period {
                params.insert("period".to_owned(), period);
            }
            params
        };

        let n0 = dag.add_node(NodeData {
            id: 0,
            params: root_params,
        });

        if let Some(node_params) = node_specs {
            let mut nodes = vec![n0];

            for spec in node_params {
                let n = dag.add_node(create_node(spec.id as i32, "execution_time", spec.exe_time));
                nodes.push(n);
            }

            for spec in edge_specs.unwrap() {
                dag.add_edge(nodes[spec.from], nodes[spec.to], 1);
            }
        }

        dag
    }

    // 2014_ECRTS_federated_original
    pub fn create_simple_dag(root_exec_time: i32, period: Option<i32>) -> Graph<NodeData, i32> {
        create_dag(root_exec_time, period, None, None)
    }

    pub fn create_high_utilization_dag() -> Graph<NodeData, i32> {
        let node_specs = [
            NodeSpec { id: 1, exe_time: 4 },
            NodeSpec { id: 2, exe_time: 3 },
            NodeSpec { id: 3, exe_time: 3 },
        ];
        let edge_specs = [
            EdgeSpec { from: 0, to: 1 },
            EdgeSpec { from: 0, to: 2 },
            EdgeSpec { from: 0, to: 3 },
        ];
        create_dag(4, Some(10), Some(&node_specs), Some(&edge_specs))
    }

    pub fn create_low_utilization_dag() -> Graph<NodeData, i32> {
        let node_specs = [
            NodeSpec { id: 1, exe_time: 3 },
            NodeSpec { id: 2, exe_time: 4 },
        ];
        let edge_specs = [EdgeSpec { from: 0, to: 1 }, EdgeSpec { from: 0, to: 2 }];
        create_dag(3, Some(30), Some(&node_specs), Some(&edge_specs))
    }

    // 2014_TPDS_basic_decomposition_based_algorithm
    pub fn create_dag_for_segment(
        period: i32,
        is_duplicates_segment: bool,
    ) -> Graph<NodeData, i32> {
        let node_specs = [
            NodeSpec { id: 1, exe_time: 7 },
            NodeSpec {
                id: 2,
                exe_time: if is_duplicates_segment { 7 } else { 55 },
            },
            NodeSpec {
                id: 3,
                exe_time: 36,
            },
            NodeSpec {
                id: 4,
                exe_time: 54,
            },
        ];

        let edge_specs = [
            EdgeSpec { from: 0, to: 1 },
            EdgeSpec { from: 0, to: 2 },
            EdgeSpec { from: 1, to: 3 },
            EdgeSpec { from: 2, to: 4 },
        ];

        create_dag(4, Some(period), Some(&node_specs), Some(&edge_specs))
    }

    // 2020_RTSS_cpc_model_based_algorithm
    pub fn create_dag_for_cpc() -> Graph<NodeData, i32> {
        let node_specs = [
            NodeSpec {
                id: 1,
                exe_time: 10,
            },
            NodeSpec {
                id: 2,
                exe_time: 10,
            },
            NodeSpec { id: 3, exe_time: 2 },
            NodeSpec { id: 4, exe_time: 2 },
            NodeSpec { id: 5, exe_time: 3 },
            NodeSpec { id: 6, exe_time: 1 },
            NodeSpec { id: 7, exe_time: 1 },
            NodeSpec { id: 8, exe_time: 3 },
        ];

        let edge_specs = [
            EdgeSpec { from: 0, to: 1 },
            EdgeSpec { from: 1, to: 2 },
            EdgeSpec { from: 0, to: 3 },
            EdgeSpec { from: 3, to: 2 },
            EdgeSpec { from: 0, to: 4 },
            EdgeSpec { from: 4, to: 6 },
            EdgeSpec { from: 0, to: 5 },
            EdgeSpec { from: 5, to: 6 },
            EdgeSpec { from: 5, to: 7 },
            EdgeSpec { from: 6, to: 8 },
            EdgeSpec { from: 7, to: 8 },
            EdgeSpec { from: 8, to: 2 },
        ];

        create_dag(10, None, Some(&node_specs), Some(&edge_specs))
    }

    // 2021_RTCSA_dynfed
    pub fn create_sample_dag_custom(
        exec_time_offset: i32,
        period: i32,
        deadline: i32,
        add_extra_node: bool,
    ) -> Graph<NodeData, i32> {
        let base_nodes = vec![
            NodeSpec {
                id: 1,
                exe_time: 20 + exec_time_offset,
            },
            NodeSpec {
                id: 2,
                exe_time: 20 + exec_time_offset,
            },
            NodeSpec {
                id: 3,
                exe_time: 10 + exec_time_offset,
            },
        ];

        let extra_node = vec![NodeSpec {
            id: 4,
            exe_time: 10 + exec_time_offset,
        }];

        let base_edges = vec![
            EdgeSpec { from: 0, to: 1 },
            EdgeSpec { from: 1, to: 2 },
            EdgeSpec { from: 0, to: 3 },
            EdgeSpec { from: 3, to: 2 },
        ];

        let extra_edges = vec![EdgeSpec { from: 0, to: 4 }, EdgeSpec { from: 4, to: 2 }];

        let node_specs = if add_extra_node {
            [base_nodes, extra_node].concat()
        } else {
            base_nodes
        };

        let edge_specs = if add_extra_node {
            [base_edges, extra_edges].concat()
        } else {
            base_edges
        };

        let mut dag = create_dag(
            10 + exec_time_offset,
            Some(period),
            Some(&node_specs),
            Some(&edge_specs),
        );
        if let Some(node) = dag.node_indices().nth(2) {
            dag.add_param(node, "end_to_end_deadline", deadline);
        }

        dag
    }
}
