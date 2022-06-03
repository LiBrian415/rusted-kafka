use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    pub fn init(topic: &str, partition: u32) -> TopicPartition {
        TopicPartition {
            topic: topic.to_string(),
            partition,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ReplicaAssignment {
    pub partitions: HashMap<TopicPartition, Vec<u32>>,
    pub adding_replicas: HashMap<TopicPartition, Vec<u32>>,
    pub removing_replicas: HashMap<TopicPartition, Vec<u32>>,
}

impl ReplicaAssignment {
    pub fn init(
        partitions: HashMap<TopicPartition, Vec<u32>>,
        adding_replicas: HashMap<TopicPartition, Vec<u32>>,
        removing_replicas: HashMap<TopicPartition, Vec<u32>>,
    ) -> ReplicaAssignment {
        ReplicaAssignment {
            partitions,
            adding_replicas,
            removing_replicas,
        }
    }

    pub fn is_being_reassigned(&self) -> bool {
        !self.adding_replicas.is_empty() || !self.removing_replicas.is_empty()
    }
}

#[derive(Clone)]
pub struct TopicIdReplicaAssignment {
    pub topic: String,
    pub assignment: HashMap<TopicPartition, ReplicaAssignment>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LeaderAndIsr {
    pub leader: u32,
    pub isr: Vec<u32>,
    pub controller_epoch: u128,
    pub leader_epoch: u128,
}

impl LeaderAndIsr {
    pub fn init(
        leader: u32,
        isr: Vec<u32>,
        controller_epoch: u128,
        leader_epoch: u128,
    ) -> LeaderAndIsr {
        LeaderAndIsr {
            leader,
            isr,
            controller_epoch,
            leader_epoch,
        }
    }
}
