//! CPC model construction.
//! Paper Information
//! -----------------
//! Title: DAG Scheduling and Analysis on Multiprocessor Systems: Exploitation of Parallelism and Dependency
//! Authors: Shuai Zhao, Xiaotian Dai, Iain Bate, Alan Burns, Wanli Chang
//! Conference: RTSS 2020
//! -----------------
use lib::graph_extension::{GraphExtension, NodeData};
use petgraph::graph::{Graph, NodeIndex};
use std::collections::{BTreeMap, HashSet, VecDeque};

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
    use lib::tests_helper::create_dag_for_cpc;

    fn common_test_setup_for_cpc() -> (Graph<NodeData, i32>, Vec<NodeIndex>, Vec<Vec<NodeIndex>>) {
        let mut dag = create_dag_for_cpc();
        let critical_path = dag.get_critical_path();
        let providers = get_providers(&dag, &critical_path);
        (dag, critical_path, providers)
    }

    #[test]
    fn test_get_providers_normal() {
        let (_, _, providers) = common_test_setup_for_cpc();

        assert_eq!(providers.len(), 2);
        assert_eq!(providers[0][0].index(), 0);
        assert_eq!(providers[0][1].index(), 1);
        assert_eq!(providers[1][0].index(), 2);
    }

    #[test]
    fn test_get_f_consumers_normal() {
        let (mut dag, critical_path, providers) = common_test_setup_for_cpc();
        let f_consumers = get_f_consumers(&mut dag, &critical_path);

        let expected_indices = [8, 3, 7, 6, 5, 4];
        assert_eq!(f_consumers[&providers[0]].len(), 6);
        for (i, &expected) in expected_indices.iter().enumerate() {
            assert_eq!(f_consumers[&providers[0]][i].index(), expected);
        }
    }
}
