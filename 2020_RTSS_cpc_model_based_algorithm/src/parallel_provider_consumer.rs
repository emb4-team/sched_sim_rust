//! CPC model construction.
//! Paper Information
//! -----------------
//! Title: DAG Scheduling and Analysis on Multiprocessor Systems: Exploitation of Parallelism and Dependency
//! Authors: Shuai Zhao, Xiaotian Dai, Iain Bate, Alan Burns, Wanli Chang
//! Conference: RTSS 2020
//! -----------------
use std::collections::{BTreeMap, HashSet, VecDeque};

use lib::graph_extension::*;
use petgraph::graph::{Graph, NodeIndex};

/// See the second paragraph of IV. A. Concurrent provider and consumer model for a detailed explanation.
/// Algorithm 1: Step1 identifying capacity providers.
/// capacity provider is a sub paths of the critical path
pub fn get_providers(
    dag: &Graph<NodeData, i32>,
    critical_path: &[NodeIndex],
) -> Vec<Vec<NodeIndex>> {
    let mut deque_critical_path: VecDeque<NodeIndex> = critical_path.iter().copied().collect();
    let mut providers: Vec<Vec<NodeIndex>> = Vec::new();
    while !deque_critical_path.is_empty() {
        let mut provider = vec![deque_critical_path.pop_front().unwrap()];
        while !deque_critical_path.is_empty()
            && dag.get_pre_nodes(deque_critical_path[0]).unwrap().len() == 1
        {
            provider.push(deque_critical_path.pop_front().unwrap());
        }
        providers.push(provider);
    }
    providers
}

/// Algorithm 1: Step2 identifying capacity consumers.
/// Capacity consumers represent specific non-critical nodes.
/// F_consumers is a consumer set that can be simultaneous executed capacity providers, and whose execution delays the start of the next capacity providers.
pub fn get_f_consumers(
    dag: &mut Graph<NodeData, i32>,
    critical_path: &[NodeIndex],
) -> BTreeMap<Vec<NodeIndex>, Vec<NodeIndex>> {
    let providers = get_providers(dag, critical_path);
    let mut f_consumers: BTreeMap<Vec<NodeIndex>, Vec<NodeIndex>> = BTreeMap::new();
    let mut non_critical_nodes: HashSet<_> = dag
        .get_non_critical_nodes(critical_path)
        .unwrap()
        .into_iter()
        .collect();

    for p_i in 0..providers.len() - 1 {
        let mut f_consumer = Vec::new();
        for next_provider_node in providers[p_i + 1].iter() {
            let anc_nodes = dag.get_anc_nodes(*next_provider_node).unwrap();
            for anc_node in anc_nodes {
                if non_critical_nodes.remove(&anc_node) {
                    f_consumer.push(anc_node);
                }
            }
        }
        f_consumers.insert(providers[p_i].clone(), f_consumer);
    }

    f_consumers
}

/// G_consumers is a consumer set belongs to the consumer set of the later providers, but can run in parallel with the capacity provider.
/// Commented out because it is used only for the priority decision algorithm, rules of α-β pair analysis, Lemma, and equations, and is not involved in this simulator implementation.
/// However, since there is a possibility that analytical α-β pair analysis will be implemented in the future, it has not been removed.
/*
pub fn get_g_consumers(
    mut dag: Graph<NodeData, i32>,
    critical_path: Vec<NodeIndex>,
) -> BTreeMap<Vec<NodeIndex>, Vec<NodeIndex>> {
    let mut providers = get_providers(&dag, critical_path);
    let f_consumers = get_f_consumers(&mut dag, critical_path);
    let mut g_consumers: BTreeMap<Vec<NodeIndex>, Vec<NodeIndex>> = BTreeMap::new();
    let mut non_critical_nodes = dag.get_non_critical_nodes(critical_path).unwrap();
    while !providers.is_empty() {
        let provider = providers.remove(0);
        // Influenced by concurrency availability only from the last critical node
        let latest_critical_node = provider.last().unwrap();
        let parallel_process_node = dag
            .get_parallel_process_nodes(*latest_critical_node)
            .unwrap_or(vec![]);
        // A non-critical node not belonging to the current consumer that can run concurrently with the last critical node
        let provider_clone = provider.clone(); // Clone the provider here
        let filtered_nodes: Vec<NodeIndex> = parallel_process_node
            .iter()
            .filter(|&node_index| {
                !f_consumers
                    .get(&provider_clone)
                    .unwrap()
                    .contains(node_index)
            })
            .filter(|&node_index| non_critical_nodes.contains(node_index))
            .cloned()
            .collect();
        g_consumers.insert(provider, filtered_nodes);
        non_critical_nodes.retain(|&node_index| {
            !f_consumers
                .get(&provider_clone)
                .unwrap()
                .contains(&node_index)
        });
    }

    g_consumers
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_node(id: i32, key: &str, value: i32) -> NodeData {
        let mut params = BTreeMap::new();
        params.insert(key.to_string(), value);
        NodeData { id, params }
    }

    ///DAG in Figure 2 (b) of the paper
    fn create_sample_dag() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();
        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 1));
        let c1 = dag.add_node(create_node(1, "execution_time", 1));
        let c2 = dag.add_node(create_node(2, "execution_time", 1));
        let c3 = dag.add_node(create_node(3, "execution_time", 1));
        let c4 = dag.add_node(create_node(4, "execution_time", 1));
        //nY_X is the Yth preceding node of cX.
        let n0_2 = dag.add_node(create_node(5, "execution_time", 0));
        let n1_2 = dag.add_node(create_node(6, "execution_time", 0));
        let n0_3 = dag.add_node(create_node(7, "execution_time", 0));
        let n1_3 = dag.add_node(create_node(8, "execution_time", 0));
        let n2_3 = dag.add_node(create_node(9, "execution_time", 0));
        let n0_4 = dag.add_node(create_node(10, "execution_time", 0));
        let n1_4 = dag.add_node(create_node(11, "execution_time", 0));
        let n2_4 = dag.add_node(create_node(12, "execution_time", 0));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);
        dag.add_edge(c2, c3, 1);
        dag.add_edge(c3, c4, 1);

        //create non-critical path edges
        dag.add_edge(c0, n0_2, 1);
        dag.add_edge(n0_2, c2, 1);
        dag.add_edge(c0, n1_2, 1);
        dag.add_edge(n1_2, c2, 1);
        dag.add_edge(c0, n0_3, 1);
        dag.add_edge(n0_3, c3, 1);
        dag.add_edge(c1, n1_3, 1);
        dag.add_edge(n1_3, c3, 1);
        dag.add_edge(c1, n2_3, 1);
        dag.add_edge(n2_3, c3, 1);
        dag.add_edge(n0_3, n0_4, 1);
        dag.add_edge(n0_4, c4, 1);
        dag.add_edge(n1_3, n1_4, 1);
        dag.add_edge(n1_4, c4, 1);
        dag.add_edge(n2_3, n2_4, 1);
        dag.add_edge(n2_4, c4, 1);

        dag
    }

    fn create_sample_dag_not_consolidated() -> Graph<NodeData, i32> {
        let mut dag = Graph::<NodeData, i32>::new();

        //cX is the Xth critical node.
        let c0 = dag.add_node(create_node(0, "execution_time", 3));
        let c1 = dag.add_node(create_node(1, "execution_time", 1));
        let c2 = dag.add_node(create_node(2, "execution_time", 3));
        //nY_X is the Yth preceding node of cX.
        let n0_1 = dag.add_node(create_node(3, "execution_time", 2));
        let n0_2 = dag.add_node(create_node(4, "execution_time", 1));
        //Independent Node
        dag.add_node(create_node(5, "execution_time", 2));

        //create critical path edges
        dag.add_edge(c0, c1, 1);
        dag.add_edge(c1, c2, 1);
        //create non-critical path edges
        dag.add_edge(n0_1, c1, 1);
        dag.add_edge(n0_1, n0_2, 1);
        dag.add_edge(n0_2, c2, 1);

        dag
    }

    #[test]
    fn test_get_providers_normal() {
        let mut dag = create_sample_dag();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, &critical_path);
        assert_eq!(providers.len(), 4);

        assert_eq!(providers[0][0].index(), 0);
        assert_eq!(providers[0][1].index(), 1);
        assert_eq!(providers[1][0].index(), 2);
        assert_eq!(providers[2][0].index(), 3);
        assert_eq!(providers[3][0].index(), 4);
    }

    #[test]
    fn test_get_providers_dag_not_consolidated() {
        let mut dag = create_sample_dag_not_consolidated();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, &critical_path);
        assert_eq!(providers.len(), 3);

        assert_eq!(providers[0][0].index(), 0);
        assert_eq!(providers[1][0].index(), 1);
        assert_eq!(providers[2][0].index(), 2);
    }

    #[test]
    fn test_get_f_consumers_normal() {
        let mut dag = create_sample_dag();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, &critical_path);
        let f_consumers = get_f_consumers(&mut dag, &critical_path);

        assert_eq!(f_consumers.len(), 3);
        assert_eq!(f_consumers[&providers[0]].len(), 2);
        assert_eq!(f_consumers[&providers[1]].len(), 3);
        assert_eq!(f_consumers[&providers[2]].len(), 3);

        assert_eq!(f_consumers[&providers[0]][0].index(), 6);
        assert_eq!(f_consumers[&providers[0]][1].index(), 5);
        assert_eq!(f_consumers[&providers[1]][0].index(), 9);
        assert_eq!(f_consumers[&providers[1]][1].index(), 8);
        assert_eq!(f_consumers[&providers[1]][2].index(), 7);
        assert_eq!(f_consumers[&providers[2]][0].index(), 12);
        assert_eq!(f_consumers[&providers[2]][1].index(), 11);
        assert_eq!(f_consumers[&providers[2]][2].index(), 10);
    }

    #[test]
    fn test_get_f_consumers_dag_not_consolidated() {
        let mut dag = create_sample_dag_not_consolidated();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, &critical_path);
        let f_consumers = get_f_consumers(&mut dag, &critical_path);

        assert_eq!(f_consumers.len(), 2);
        assert_eq!(f_consumers[&providers[0]].len(), 1);
        assert_eq!(f_consumers[&providers[1]].len(), 1);

        assert_eq!(f_consumers[&providers[0]][0].index(), 3);
        assert_eq!(f_consumers[&providers[1]][0].index(), 4);
    }
    /*
    #[test]
    fn test_get_g_consumers_normal() {
        let dag = create_sample_dag();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, critical_path);
        let g_consumers = get_g_consumers(dag, critical_path);

        assert_eq!(g_consumers.len(), 4);
        assert_eq!(g_consumers[&providers[0]].len(), 2);
        assert_eq!(g_consumers[&providers[1]].len(), 3);
        assert_eq!(g_consumers[&providers[2]].len(), 0);
        assert_eq!(g_consumers[&providers[3]].len(), 0);

        assert_eq!(g_consumers[&providers[0]][0].index(), 7);
        assert_eq!(g_consumers[&providers[0]][1].index(), 10);
        assert_eq!(g_consumers[&providers[1]][0].index(), 10);
        assert_eq!(g_consumers[&providers[1]][1].index(), 11);
        assert_eq!(g_consumers[&providers[1]][2].index(), 12);
    }
    */
}
