use zookeeper::ZooKeeper;

use super::zk_watcher::KafkaZkHandlers;

pub struct KafkaZkClient {
    client: ZooKeeper,
    handlers: KafkaZkHandlers
}

impl KafkaZkClient {

    pub fn init() -> KafkaZkClient {
        todo!();
    }

    /// Ensures that the follow paths exist:
    ///  - BrokersIdZNode
    ///  - TopicsZNode
    pub fn createTopLevelPaths() {
        todo!();
    }

    // Controller

    pub fn registerControllerAndIncrementControllerEpoch() {
        todo!();
    }

    pub fn getControllerId() {
        todo!();
    }
    
    pub fn getControllerEpoch() {
        todo!();
    }

    // Broker

    pub fn registerBroker() {
        todo!();
    }

    pub fn getBroker() {
        todo!();
    }

    pub fn getAllBrokers() {
        todo!();
    }
    
    // Topic + Partition

    pub fn getAllTopics() {
        todo!();
    }

    pub fn createTopicPartitions() {
        todo!();
    }

    pub fn getPartitionsForTopics() {
        todo!();
    }

    pub fn getReplicaAssignmentForTopics() {
        todo!();
    }

    pub fn setReplicaAssignmentForTopics() {
        todo!();
    }

    pub fn setLeaderAndIsr() {
        todo!();
    }

    pub fn getTopicPartitionOffset() {
        todo!();
    }

    pub fn setTopicPartitionOffset() {
        todo!();
    }

    // ZooKeeper

    pub fn getChildren() {
        todo!();
    }

    pub fn registerZNodeChangeHandler() {
        todo!();
    }

    pub fn unregisterZNodeChangeHandler() {
        todo!();
    }

    pub fn registerZNodeChildChangeHandler() {
        todo!();
    }

    pub fn unregisterZNodeChildChangeHandler() {
        todo!();
    }
}
