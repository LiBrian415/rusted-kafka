use zookeeper::ZooKeeper;

use super::zk_watcher::{KafkaZkHandlers, ZkChangeHandler, ZkChildChangeHandler};

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

    pub fn register_znode_change_handler(&self, handler: Box<dyn ZkChangeHandler>) {
        self.handlers.register_znode_change_handler(handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_change_handler(path);
    }

    pub fn register_znode_child_change_handler(&self, handler: Box<dyn ZkChildChangeHandler>) {
        self.handlers.register_znode_child_change_handler(handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_child_change_handler(path);
    }

}
