use std::collections::{HashMap, HashSet};

use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
};

pub struct ControllerContext {
    offline_partition_cnt: u32,
    pub shuttingdown_broker_ids: HashSet<u32>,
    pub live_brokers: HashSet<u32>,
    live_broker_epochs: HashMap<u32, u128>,
    pub epoch: u128,
    pub epoch_zk_version: i32,
    pub all_topics: HashSet<String>,
    pub partitions_being_reassigned: HashSet<TopicPartition>,
    topic_ids: HashMap<String, u32>,
    topic_names: HashMap<u32, String>,
    partition_assignments: HashMap<String, HashMap<u32, ReplicaAssignment>>,
    // replica_states: HashMap<>,
    topics_to_be_deleted: HashSet<String>,
}

impl ControllerContext {
    pub fn add_topic_id(&self, topic: String, id: u32) {
        todo!();
    }

    pub fn partition_replica_assignment(&self, t_partition: TopicPartition) -> Vec<u32> {
        todo!();
    }

    pub fn set_live_brokers(&self, broker_and_epochs: HashMap<BrokerInfo, u128>) {
        todo!();
    }

    fn clear_live_brokers(&self) {
        todo!();
    }

    pub fn add_live_brokers(&self, broker_and_epochs: HashMap<BrokerInfo, u128>) {
        todo!();
    }

    pub fn remove_live_brokers(&self, broker_ids: Vec<u32>) {
        todo!();
    }

    pub fn set_all_topics(&self, topics: HashSet<String>) {
        todo!();
    }

    pub fn update_partition_full_replica_assignment(
        &self,
        t_partition: TopicPartition,
        new_assignment: ReplicaAssignment,
    ) {
        todo!();
    }

    pub fn clear_partition_leadership_info(&self) {
        todo!();
    }

    pub fn all_partitions(&self) -> Vec<TopicPartition> {
        todo!()
    }

    pub fn put_partition_leadership_info(
        &self,
        partition: TopicPartition,
        leader_isr: LeaderAndIsr,
        epoch: u128,
    ) {
        todo!();
    }
}
