use std::collections::{HashMap, HashSet};

use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
};

pub struct ControllerContext {
    // offline_partition_cnt: u32,
    pub shuttingdown_broker_ids: HashSet<u32>,
    pub live_brokers: HashSet<BrokerInfo>,
    live_broker_epochs: HashMap<u32, u128>,
    pub epoch: u128,
    pub epoch_zk_version: i32,
    pub all_topics: HashSet<String>,
    pub partitions_being_reassigned: HashSet<TopicPartition>,
    topic_ids: HashMap<String, u32>,
    topic_names: HashMap<u32, String>,
    partition_assignments: HashMap<String, HashMap<u32, ReplicaAssignment>>,
    partition_leadership_info: HashMap<TopicPartition, (LeaderAndIsr, u128)>,
    // replica_states: HashMap<>,
    topics_to_be_deleted: HashSet<String>,
}

impl ControllerContext {
    pub fn live_or_shutting_down_broker_ids(&self) -> HashSet<u32> {
        HashSet::from_iter(self.live_broker_epochs.keys().map(|&id| id))
    }

    pub fn live_or_shutting_down_brokers(&self) -> HashSet<BrokerInfo> {
        self.live_brokers.clone()
    }

    pub fn live_or_shutting_down_broker(&self, broker_id: u32) -> Option<BrokerInfo> {
        for broker in self.live_or_shutting_down_brokers() {
            if broker.id == broker_id {
                return Some(broker);
            }
        }

        None
    }

    pub fn add_topic_id(&mut self, topic: String, id: u32) {
        if !self.all_topics.contains(&topic) {
            // maybe an error?
            return;
        }

        match self.topic_ids.get(&topic) {
            Some(existing_id) => {
                if existing_id.clone() == id {
                    return;
                }
            }
            None => {}
        }

        match self.topic_names.get(&id) {
            Some(existing_topic) => {
                if existing_topic.to_string() == topic {
                    return;
                }
            }
            None => {}
        }
        self.topic_ids.insert(topic.clone(), id.clone());
        self.topic_names.insert(id, topic);
    }

    pub fn partition_replica_assignment(&self, partition: TopicPartition) -> Vec<u32> {
        match self.partition_assignments.get(&partition.topic) {
            Some(partition_map) => match partition_map.get(&partition.partition) {
                Some(replica_assignment) => match replica_assignment.partitions.get(&partition) {
                    Some(replicas) => replicas.clone(),
                    None => Vec::new(),
                },
                None => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    pub fn set_live_brokers(&mut self, broker_and_epochs: HashMap<BrokerInfo, u128>) {
        self.clear_live_brokers();
        self.add_live_brokers(broker_and_epochs);
    }

    fn clear_live_brokers(&mut self) {
        self.live_brokers.clear();
        self.live_broker_epochs.clear();
    }

    pub fn add_live_brokers(&mut self, broker_and_epochs: HashMap<BrokerInfo, u128>) {
        let _: Vec<()> = broker_and_epochs
            .into_iter()
            .map(|(broker, epoch)| {
                self.live_brokers.insert(broker.clone());
                self.live_broker_epochs.insert(broker.id, epoch);
            })
            .collect();
    }

    // pub fn remove_live_brokers(&self, broker_ids: Vec<u32>) {
    //     todo!();
    // }

    pub fn set_all_topics(&mut self, topics: HashSet<String>) {
        self.all_topics.clear();
        self.all_topics.extend(topics);
    }

    pub fn update_partition_full_replica_assignment(
        &mut self,
        partition: TopicPartition,
        new_assignment: ReplicaAssignment,
    ) {
        match self.partition_assignments.get_mut(&partition.topic) {
            Some(assignments) => {
                *assignments.get_mut(&partition.partition).unwrap() = new_assignment;
                // TODO: updatePreferredReplicaImbalanceMetric?
            }
            None => {}
        }
    }

    pub fn clear_partition_leadership_info(&mut self) {
        self.partition_leadership_info.clear()
    }

    pub fn all_partitions(&self) -> Vec<TopicPartition> {
        let mut partitions: Vec<TopicPartition> = Vec::new();
        for topic in self.partition_assignments.keys() {
            for partition in self.partition_assignments.get(topic).unwrap().keys() {
                partitions.push(TopicPartition {
                    topic: topic.to_string(),
                    partition: *partition,
                })
            }
        }

        partitions
    }

    pub fn put_partition_leadership_info(
        &mut self,
        partition: TopicPartition,
        leader_isr: LeaderAndIsr,
        epoch: u128,
    ) {
        match self.partition_leadership_info.get_mut(&partition) {
            Some(info) => {
                *info = (leader_isr, epoch);
                // TODO: updatePreferredReplicaImbalanceMetric?
            }
            None => {}
        }
    }

    pub fn update_broker_metadata(&mut self, old: BrokerInfo, new: BrokerInfo) {
        self.live_brokers.remove(&old);
        self.live_brokers.insert(new);
    }

    pub fn reset_context(&mut self) {
        self.topics_to_be_deleted.clear();
        // self.topics_with_deletion_started.clear();
        // self.topics_with_ineligible_for_deletion.clear()
        self.shuttingdown_broker_ids.clear();
        self.epoch = 0;
        self.epoch_zk_version = 0;
        // self.clear_topic_state();
        self.clear_live_brokers();
    }

    fn clear_topic_state(&self) {
        todo!();
    }
}
