use std::collections::{HashMap, HashSet};

use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
};

pub struct ControllerContext {
    // offline_partition_cnt: u32,
    pub shuttingdown_broker_ids: HashSet<u32>,
    pub live_brokers: HashSet<BrokerInfo>,
    live_broker_epochs: HashMap<u32, i64>, // topic id -> epoch
    pub epoch: u128,
    pub epoch_zk_version: i32,
    pub all_topics: HashSet<String>,
    pub partitions_being_reassigned: HashSet<TopicPartition>,
    topic_ids: HashMap<String, u32>,   // topic name -> topic id
    topic_names: HashMap<u32, String>, // topic id -> topic name
    partition_assignments: HashMap<String, ReplicaAssignment>, // topic name -> assignment
    partition_leadership_info: HashMap<TopicPartition, (LeaderAndIsr, u128)>, // topicPartition -> (leaderAndIsr, epoch)
    // replica_states: HashMap<>,
    topics_to_be_deleted: HashSet<String>,
}

impl ControllerContext {
    pub fn init() -> ControllerContext {
        todo!();
    }

    pub fn live_broker_ids(&self) -> Vec<u32> {
        let ids: HashSet<u32> = self.live_broker_epochs.keys().cloned().collect();
        ids.difference(&self.shuttingdown_broker_ids)
            .cloned()
            .collect::<Vec<u32>>()
    }
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
            Some(partition_map) => match partition_map.partitions.get(&partition) {
                Some(replicas) => replicas.clone(),
                None => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    pub fn set_live_brokers(&mut self, broker_and_epochs: HashMap<BrokerInfo, i64>) {
        self.clear_live_brokers();
        self.add_live_brokers(broker_and_epochs);
    }

    fn clear_live_brokers(&mut self) {
        self.live_brokers.clear();
        self.live_broker_epochs.clear();
    }

    pub fn add_live_brokers(&mut self, broker_and_epochs: HashMap<BrokerInfo, i64>) {
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
                *assignments = new_assignment;
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
            for partition in self
                .partition_assignments
                .get(topic)
                .unwrap()
                .partitions
                .keys()
            {
                partitions.push(partition.clone());
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

    pub fn clear_topic_state(&mut self) {
        self.all_topics.clear();
        self.topic_ids.clear();
        self.topic_names.clear();
        self.partition_assignments.clear();
        self.partition_leadership_info.clear();
        self.partitions_being_reassigned.clear();
    }

    // pub fn remove_topic(&self, topic: String) {
    //     todo!();
    // }

    pub fn remove_live_brokers(&mut self, broker_ids: Vec<u32>) {
        self.live_brokers
            .retain(|broker| !broker_ids.contains(&broker.id));
        self.live_broker_epochs
            .retain(|id, _| !broker_ids.contains(&id));
    }

    pub fn replicas_for_partition(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> HashSet<(TopicPartition, u32)> {
        // return TopicPartition, replica
        let mut result_replicas: HashSet<(TopicPartition, u32)> = HashSet::new();
        for partition in partitions {
            let replica_assignment = self.partition_assignments.get(&partition.topic);
            if replica_assignment.is_none() {
                continue;
            }

            let assignment = replica_assignment.unwrap().clone().partitions;
            let replicas = assignment.get(&partition);
            if replicas.is_none() {
                continue;
            }

            for replica in replicas.unwrap() {
                result_replicas.insert((partition.clone(), replica.clone()));
            }
        }

        result_replicas
    }

    pub fn partitions_with_leaders(&self) -> HashSet<TopicPartition> {
        self.partition_leadership_info.keys().cloned().collect()
    }

    pub fn replicas_on_brokers(&self, broker_ids: HashSet<u32>) -> HashSet<(TopicPartition, u32)> {
        let mut result_replicas: HashSet<(TopicPartition, u32)> = HashSet::new();
        for id in broker_ids {
            for (_, assignment) in self.partition_assignments.iter() {
                for (partition, replicas) in assignment.partitions.iter() {
                    for replica in replicas {
                        if replica.clone() == id {
                            result_replicas.insert((partition.clone(), replica.clone()));
                        }
                    }
                }
            }
        }

        result_replicas
    }

    pub fn partitions_with_offline_leader(&mut self) -> HashSet<TopicPartition> {
        let mut partition_leadership_info = self.partition_leadership_info.clone();
        partition_leadership_info.retain(|partition, leader_and_isr| {
            !self.is_replica_online(leader_and_isr.0.leader, partition.clone())
        });

        self.partition_leadership_info = partition_leadership_info.clone();

        partition_leadership_info.keys().cloned().collect()
    }

    pub fn is_replica_online(&self, broker_id: u32, _partition: TopicPartition) -> bool {
        self.live_broker_ids().contains(&broker_id)
    }
}
