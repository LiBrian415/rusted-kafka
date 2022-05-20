pub struct ControllerZNode {}
impl ControllerZNode {
    pub fn path() -> String {
        "/controller".to_string()
    }
}

pub struct ControllerEpochZNode {}
impl ControllerEpochZNode {
    pub fn path() -> String {
        "/controller_epoch".to_string()
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
}

pub struct TopicsZNode {}
impl TopicsZNode{
    pub fn path() -> String {
        format!("{}/topics", BrokersZNode::path())
    }
}

pub struct TopicZNode {}
impl TopicZNode {
    pub fn path(topic: &str) -> String {
        format!("{}/{}", TopicsZNode::path(), topic)
    }
}

pub struct TopicPartitionsZNode{}
impl TopicPartitionsZNode {
    pub fn path(topic: &str) -> String {
        format!("{}/partitions", TopicZNode::path(topic))
    }
}

pub struct TopicPartitionZNode{}
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
}

pub struct TopicPartitionOffsetZNode {}
impl TopicPartitionOffsetZNode {
    pub fn path(topic: &str, partition: u32) -> String {
        format!("{}/offset", TopicPartitionZNode::path(topic, partition))
    }
}


#[cfg(test)]
mod path_tests {
    use crate::zk::zk_data::{ControllerZNode, ControllerEpochZNode, BrokersZNode, BrokerIdsZNode, BrokerIdZNode, TopicsZNode, TopicZNode, TopicPartitionsZNode, TopicPartitionZNode, TopicPartitionStateZNode, TopicPartitionOffsetZNode};

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
        assert_eq!(TopicPartitionsZNode::path("tmp"), "/broker/topics/tmp/partitions");
    }

    #[test]
    fn topic_partition_path() {
        assert_eq!(TopicPartitionZNode::path("tmp", 7), "/broker/topics/tmp/partitions/7");
    }

    #[test]
    fn topic_partition_state_path() {
        assert_eq!(TopicPartitionStateZNode::path("tmp", 7), "/broker/topics/tmp/partitions/7/state");
    }

    #[test]
    fn topic_partition_offset_path() {
        assert_eq!(TopicPartitionOffsetZNode::path("tmp", 7), "/broker/topics/tmp/partitions/7/offset");
    }
}