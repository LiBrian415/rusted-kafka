use std::{collections::{HashMap, HashSet}};

use crate::common::topic_partition::{ReplicaAssignment, TopicPartition};

pub struct ControllerContext {
    offline_partition_cnt: u32,
    shuttingdown_broker_ids: HashSet<u32>,
    pub live_brokers: HashSet<u32>,
    live_broker_epochs: HashMap<u32, u128>,
    pub epoch: u128,
    pub epoch_zk_version: i32,
    all_topics: HashSet<String>,
    topic_ids: HashMap<String, u32>,
    topic_names: HashMap<u32, String>,
    partition_assignments: HashMap<String, HashMap<u32, ReplicaAssignment>>,
    // replica_states: HashMap<>,
    topics_to_be_deleted: HashSet<String>,
}

impl ControllerContext {
    pub fn add_topic_id(topic: String, id: u32) {
        todo!();
    }

    pub fn partition_replica_assignment(t_partition: TopicPartition) -> Vec<u32> {
        todo!();
    }

    // pub fn set_live_brokers(broker_and_epochs: HashMap<Broker, u128>) {
    //     todo!();
    // }

    fn clear_live_brokers() {
        todo!();
    }

    // pub fn add_live_brokers(broker_and_epochs: HashMap<Broker, u128>) {
    //     todo!();
    // }

    pub fn remove_live_brokers(broker_ids: Vec<u32>) {

    }
}