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
    use crate::graph_extension::{GraphExtension, NodeData};
    use crate::util::{create_yaml, load_yaml};
    use petgraph::Graph;
    use std::path::Path;
    use std::{collections::BTreeMap, fs::remove_file};
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
                assert_yaml_value!($yaml, &path, $typ, $expected);
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

    pub fn common_yaml_test<F, A>(test_name: &str, dump_fn: F, asserts: A)
    where
        F: FnOnce(&Path),
        A: FnOnce(&Yaml),
    {
        let file_path_str = create_yaml("../lib/tests", test_name);
        let file_path = Path::new(&file_path_str);
        dump_fn(file_path);

        let yaml_docs = load_yaml(file_path.to_str().unwrap());
        let yaml_doc = &yaml_docs[0];

        asserts(yaml_doc);

        remove_file(file_path).unwrap();
    }

    pub fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    pub fn create_simple_graph(exec_time: i32, period: Option<i32>) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let mut params = BTreeMap::new();
        params.insert("execution_time".to_owned(), exec_time);
        if let Some(p) = period {
            params.insert("period".to_owned(), p);
        }
        dag.add_node(NodeData { id: 0, params });
        dag
    }

    // 2014_ECRTS_federated_original
    pub fn create_high_utilization_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = {
            let mut params = BTreeMap::new();
            params.insert("execution_time".to_owned(), 4);
            params.insert("period".to_owned(), 10);
            dag.add_node(NodeData { id: 3, params })
        };
        let n1 = dag.add_node(create_node(1, "execution_time", 4));
        let n2 = dag.add_node(create_node(2, "execution_time", 3));
        let n3 = dag.add_node(create_node(3, "execution_time", 3));
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n0, n3, 1);

        dag
    }

    pub fn create_low_utilization_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = {
            let mut params = BTreeMap::new();
            params.insert("execution_time".to_owned(), 3);
            params.insert("period".to_owned(), 30);
            dag.add_node(NodeData { id: 2, params })
        };
        let n1 = dag.add_node(create_node(0, "execution_time", 3));
        let n2 = dag.add_node(create_node(1, "execution_time", 4));

        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag
    }

    // 2014_TPDS_basic_decomposition_based_algorithm
    pub fn create_dag_for_segment(
        period: i32,
        is_duplicates_segment: bool,
    ) -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        let n0 = dag.add_node(create_node(0, "execution_time", 4));
        let n1 = dag.add_node(create_node(1, "execution_time", 7));
        let execution_time_n2 = if is_duplicates_segment { 7 } else { 55 };
        let n2 = dag.add_node(create_node(2, "execution_time", execution_time_n2));
        let n3 = dag.add_node(create_node(3, "execution_time", 36));
        let n4 = dag.add_node(create_node(4, "execution_time", 54));

        dag.add_param(n0, "period", period);
        dag.add_edge(n0, n1, 1);
        dag.add_edge(n0, n2, 1);
        dag.add_edge(n1, n3, 1);
        dag.add_edge(n2, n4, 1);

        dag
    }

    // 2020_RTSS_cpc_model_based_algorithm
    pub fn create_dag_for_cpc() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 10));
        let c1 = dag.add_node(create_node(1, "execution_time", 10));
        let c2 = dag.add_node(create_node(2, "execution_time", 10));

        //create non-critical node.
        //No distinction is made because of the complexity.
        let n3 = dag.add_node(create_node(3, "execution_time", 2));
        let n4 = dag.add_node(create_node(4, "execution_time", 2));
        let n5 = dag.add_node(create_node(5, "execution_time", 3));
        let n6 = dag.add_node(create_node(6, "execution_time", 1));
        let n7 = dag.add_node(create_node(7, "execution_time", 1));
        let n8 = dag.add_node(create_node(8, "execution_time", 3));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);

        //create non-critical path edges
        dag.add_edge(c0, n3, 1);
        dag.add_edge(n3, c2, 1);
        dag.add_edge(c0, n4, 1);
        dag.add_edge(n4, n6, 1);
        dag.add_edge(c0, n5, 1);
        dag.add_edge(n5, n6, 1);
        dag.add_edge(n5, n7, 1);
        dag.add_edge(n6, n8, 1);
        dag.add_edge(n7, n8, 1);
        dag.add_edge(n8, c2, 1);

        dag
    }
}
