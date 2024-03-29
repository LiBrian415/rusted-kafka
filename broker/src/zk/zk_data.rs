use bincode;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicIdReplicaAssignment, TopicPartition},
};

#[derive(Serialize, Deserialize)]
pub struct ControllerInfo {
    broker_id: u32,
}

impl ControllerInfo {
    fn init(broker_id: u32) -> ControllerInfo {
        ControllerInfo { broker_id }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ControllerEpoch {
    epoch: u128,
}

impl ControllerEpoch {
    fn init(epoch: u128) -> ControllerEpoch {
        ControllerEpoch { epoch }
    }
}

pub struct ControllerZNode {}
impl ControllerZNode {
    pub fn path() -> String {
        "/controller".to_string()
    }

    pub fn encode(controller_id: u32) -> Vec<u8> {
        let ci = ControllerInfo::init(controller_id);
        serde_json::to_vec(&ci).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> u32 {
        let ci: ControllerInfo = serde_json::from_slice(data).unwrap();
        ci.broker_id
    }
}

pub struct ControllerEpochZNode {}
impl ControllerEpochZNode {
    pub fn path() -> String {
        "/controller_epoch".to_string()
    }

    pub fn encode(epoch: u128) -> Vec<u8> {
        let ce = ControllerEpoch::init(epoch);
        serde_json::to_vec(&ce).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> u128 {
        let ce: ControllerEpoch = serde_json::from_slice(data).unwrap();
        ce.epoch
    }
}

pub struct BrokersZNode {}
impl BrokersZNode {
    pub fn path() -> String {
        "/broker".to_string()
    }
}

pub struct BrokerIdsZNode {}
impl BrokerIdsZNode {
    pub fn path() -> String {
        format!("{}/ids", BrokersZNode::path())
    }
}

pub struct BrokerIdZNode {}
impl BrokerIdZNode {
    pub fn path(id: u32) -> String {
        format!("{}/{}", BrokerIdsZNode::path(), id)
    }

    pub fn encode(broker_info: &BrokerInfo) -> Vec<u8> {
        serde_json::to_vec(broker_info).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> BrokerInfo {
        serde_json::from_slice::<BrokerInfo>(data).unwrap()
    }
}

pub struct TopicsZNode {}
impl TopicsZNode {
    pub fn path() -> String {
        format!("{}/topics", BrokersZNode::path())
    }
}

pub struct TopicZNode {}
impl TopicZNode {
    pub fn path(topic: &str) -> String {
        format!("{}/{}", TopicsZNode::path(), topic)
    }

    pub fn encode(replica_assignment: ReplicaAssignment) -> Vec<u8> {
        bincode::serialize(&replica_assignment).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> ReplicaAssignment {
        bincode::deserialize::<ReplicaAssignment>(data).unwrap()
    }

    pub fn decode_with_topic(topic: String, data: &Vec<u8>) -> TopicIdReplicaAssignment {
        let replica_assignment = bincode::deserialize::<ReplicaAssignment>(data).unwrap();
        let partitions: Vec<TopicPartition> =
            replica_assignment.partitions.keys().cloned().collect();

        let mut assignment: HashMap<TopicPartition, ReplicaAssignment> = HashMap::new();
        for partition in partitions {
            let mut new_replica_assignment = replica_assignment.clone();
            new_replica_assignment
                .partitions
                .retain(|k, _| *k == partition);
            new_replica_assignment
                .adding_replicas
                .retain(|k, _| *k == partition);
            new_replica_assignment
                .removing_replicas
                .retain(|k, _| *k == partition);
            assignment.insert(partition, new_replica_assignment);
        }

        TopicIdReplicaAssignment {
            topic: topic,
            assignment: assignment,
        }
    }
}

pub struct TopicPartitionsZNode {}
impl TopicPartitionsZNode {
    pub fn path(topic: &str) -> String {
        format!("{}/partitions", TopicZNode::path(topic))
    }
}

pub struct TopicPartitionZNode {}
impl TopicPartitionZNode {
    pub fn path(topic: &str, partition: u32) -> String {
        format!("{}/{}", TopicPartitionsZNode::path(topic), partition)
    }
}

pub struct TopicPartitionStateZNode {}
impl TopicPartitionStateZNode {
    pub fn path(topic: &str, partition: u32) -> String {
        format!("{}/state", TopicPartitionZNode::path(topic, partition))
    }

    pub fn encode(leader_and_isr: LeaderAndIsr) -> Vec<u8> {
        serde_json::to_vec(&leader_and_isr).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> LeaderAndIsr {
        serde_json::from_slice::<LeaderAndIsr>(data).unwrap()
    }
}

pub struct TopicPartitionOffsetZNode {}
impl TopicPartitionOffsetZNode {
    pub fn path(topic: &str, partition: u32) -> String {
        format!("{}/offset", TopicPartitionZNode::path(topic, partition))
    }

    pub fn encode() -> Vec<u8> {
        vec![1]
    }
}

pub struct IsrChangeNotificationZNode {}
impl IsrChangeNotificationZNode {
    pub fn path() -> String {
        format!("/isr_change_notification").to_string()
    }
}

pub struct IsrChangeNotificationSequenceZNode {}
impl IsrChangeNotificationSequenceZNode {
    pub fn path(seq_num: String) -> String {
        format!(
            "{}/isr_change_{}",
            IsrChangeNotificationZNode::path(), // TODO: need to check
            seq_num
        )
        .to_string()
    }

    pub fn encode(partitions: HashSet<TopicPartition>) -> Vec<u8> {
        serde_json::to_vec(&partitions).unwrap()
    }

    pub fn decode(data: &Vec<u8>) -> HashSet<TopicPartition> {
        serde_json::from_slice::<HashSet<TopicPartition>>(data).unwrap()
    }

    pub fn seq_num(path: String) -> String {
        let prefix = "/isr_change_notification/isr_change_".to_string();
        path[prefix.len()..].to_string()
    }
}

pub struct PersistentZkPaths {
    pub paths: Vec<String>,
}

impl PersistentZkPaths {
    pub fn init() -> PersistentZkPaths {
        let mut paths: Vec<String> = Vec::new();
        paths.push(BrokerIdsZNode::path());
        paths.push(TopicsZNode::path());

        PersistentZkPaths { paths: paths }
    }
}

#[cfg(test)]
mod path_tests {
    use crate::zk::zk_data::{
        BrokerIdZNode, BrokerIdsZNode, BrokersZNode, ControllerEpochZNode, ControllerZNode,
        TopicPartitionOffsetZNode, TopicPartitionStateZNode, TopicPartitionZNode,
        TopicPartitionsZNode, TopicZNode, TopicsZNode,
    };

    #[test]
    fn controller_path() {
        assert_eq!(ControllerZNode::path(), "/controller");
    }

    #[test]
    fn controller_epoch_path() {
        assert_eq!(ControllerEpochZNode::path(), "/controller_epoch");
    }

    #[test]
    fn brokers_path() {
        assert_eq!(BrokersZNode::path(), "/broker");
    }

    #[test]
    fn broker_ids_path() {
        assert_eq!(BrokerIdsZNode::path(), "/broker/ids");
    }

    #[test]
    fn broker_id_path() {
        assert_eq!(BrokerIdZNode::path(10), "/broker/ids/10");
    }

    #[test]
    fn topics_path() {
        assert_eq!(TopicsZNode::path(), "/broker/topics");
    }

    #[test]
    fn topic_path() {
        assert_eq!(TopicZNode::path("tmp"), "/broker/topics/tmp");
    }

    #[test]
    fn topic_partitions_path() {
        assert_eq!(
            TopicPartitionsZNode::path("tmp"),
            "/broker/topics/tmp/partitions"
        );
    }

    #[test]
    fn topic_partition_path() {
        assert_eq!(
            TopicPartitionZNode::path("tmp", 7),
            "/broker/topics/tmp/partitions/7"
        );
    }

    #[test]
    fn topic_partition_state_path() {
        assert_eq!(
            TopicPartitionStateZNode::path("tmp", 7),
            "/broker/topics/tmp/partitions/7/state"
        );
    }

    #[test]
    fn topic_partition_offset_path() {
        assert_eq!(
            TopicPartitionOffsetZNode::path("tmp", 7),
            "/broker/topics/tmp/partitions/7/offset"
        );
    }
}
