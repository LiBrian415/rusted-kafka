use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{Arc, RwLock},
};

use crate::{
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    zk::zk_client::KafkaZkClient,
};

use super::log_manager::LogManager;

/// The ReplicaManager is in-charge of managing the replica
/// information of partitions that it's in-charge of. Namely,
/// it should maintain a local view of the isr of the partitions
/// that it's the leader of as well as the offset of the partitions
/// that it's the follower of.
///
/// To track the isr of the followers of its partitions, it maintains
/// a offset tracker of followers for each partition. If they fall
/// within or outside a range, they'll be considered in/out of the isr
/// set.
pub struct ReplicaManager {
    log_manager: Arc<LogManager>,
    zk_client: Arc<KafkaZkClient>,
    partition_manager: PartitionManager,
}

impl ReplicaManager {
    pub fn init(log_manager: Arc<LogManager>, zk_client: Arc<KafkaZkClient>) -> ReplicaManager {
        ReplicaManager {
            log_manager,
            zk_client,
            partition_manager: PartitionManager::init(),
        }
    }

    /// Startup background tasks:
    ///  - highwatermark checkpoint
    ///  - isr change update
    pub fn startup(&self) {}

    /// Note: When the leader changes for replica set,
    /// then use this method to handle the change.
    pub fn become_leader_or_follower(&self) {}

    /// To make the broker a leader, it'll add/update the PartitionState
    /// to reflect this.
    pub fn make_leader(&self) {}

    /// To make the broker a follower, it'll perform the following
    /// steps:
    /// 1) change the partition_state to follow (Note: make sure that the new leader is alive)
    /// 2) stop any ongoing fetcher threads for the partition
    /// 3) truncate the log to the last high watermark
    /// 4) add a watcher
    /// 5) start a fetch event for the new leader
    pub fn make_follower(&self) {}

    /// Fetches the desired messages from the TopicPartitions.
    ///
    /// lib - If GC is added, then for the same reasons as log_manager,
    /// this should also return the first offset of the retrieved messages.
    pub async fn fetch_messages(
        &self,
        replica_id: Option<u32>,
        fetch_max_bytes: u64,
        partitions: Vec<(TopicPartition, u64)>,
    ) -> Vec<(TopicPartition, Vec<u8>)> {
        let limit_per_part = fetch_max_bytes / partitions.len() as u64;
        let mut result = Vec::new();

        for (partition, start) in &partitions {
            let log = self.log_manager.get_log(partition);
            if let Some(log) = log {
                let messages = (*log).fetch_messages(*start, limit_per_part, replica_id.is_none());
                result.push((partition.clone(), messages));
            }
        }

        if let Some(replica_id) = replica_id {
            for (partition, start) in &partitions {
                if let Some(partition_state) = self.partition_manager.get_partition_state(partition)
                {
                    (*partition_state).update_follower_offset(replica_id, *start);
                }
            }
        }

        result
    }

    pub async fn append_messages(
        &self,
        required_acks: i8,
        entries_per_partitions: Vec<(TopicPartition, Vec<u8>)>,
    ) {
        if ReplicaManager::is_valid_ack(required_acks) {
            for (partition, messages) in entries_per_partitions {
                let log = self.log_manager.get_log(&partition);
                if let Some(log) = log {
                    log.append_messages(messages);
                }
            }

            // Create response -> rep
        } else {
            // Return some error
        }
    }

    /// Check if all nodes in the isr ack'd the
    /// replica.
    fn is_valid_ack(required_acks: i8) -> bool {
        required_acks == -1 || required_acks == 0 || required_acks == 1
    }
}

struct PartitionManager {
    partitions: RwLock<HashMap<TopicPartition, Arc<PartitionState>>>,
}

impl PartitionManager {
    // TODO: should also start an isr task
    fn init() -> PartitionManager {
        PartitionManager {
            partitions: RwLock::new(HashMap::new()),
        }
    }

    fn get_partition_state(&self, topic_partition: &TopicPartition) -> Option<Arc<PartitionState>> {
        let r = self.partitions.read().unwrap();
        match (*r).get(topic_partition) {
            Some(partition_state) => Some(partition_state.clone()),
            None => None,
        }
    }

    fn create_partition() {}
}

struct PartitionState {
    leader: RwLock<Option<BrokerInfo>>,
    followers_state: RwLock<HashMap<u32, u64>>,
    isr: RwLock<HashSet<u32>>,
}

impl PartitionState {
    fn init_leader(isr: Vec<u32>) -> PartitionState {
        PartitionState {
            leader: RwLock::new(None),
            followers_state: RwLock::new(HashMap::new()),
            isr: RwLock::new(HashSet::from_iter(isr)),
        }
    }

    fn init_follower(leader: BrokerInfo) -> PartitionState {
        PartitionState {
            leader: RwLock::new(Some(leader)),
            followers_state: RwLock::new(HashMap::new()),
            isr: RwLock::new(HashSet::new()),
        }
    }

    fn update_follower_offset(&self, follower_id: u32, offset: u64) {
        let mut w = self.followers_state.write().unwrap();
        (*w).insert(follower_id, offset);
    }
}

pub type RmResult<T> = Result<T, Box<(dyn Error + Send + Sync)>>;
