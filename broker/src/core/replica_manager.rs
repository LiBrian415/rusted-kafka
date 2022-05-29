use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::task::JoinHandle;

use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    zk::zk_client::KafkaZkClient,
};

use super::{
    ack_manager::AckManager,
    background::{isr_update::isr_update_task, watermark_checkpoint::watermark_cp_task},
    err::{ReplicaError, ReplicaResult},
    log_manager::LogManager,
    partition_manager::PartitionManager,
};

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
    broker_id: u32,
    log_manager: Arc<LogManager>,
    zk_client: Arc<KafkaZkClient>,
    partition_manager: Arc<PartitionManager>,
    ack_manager: Arc<AckManager>,

    _isr_update_task: JoinHandle<()>,
    _watermark_cp_task: JoinHandle<()>,
}

impl ReplicaManager {
    pub fn init(
        broker_id: u32,
        log_manager: Arc<LogManager>,
        zk_client: Arc<KafkaZkClient>,
    ) -> ReplicaManager {
        let partition_manager = Arc::new(PartitionManager::init(log_manager.clone()));
        let ack_manager = Arc::new(AckManager::init(log_manager.clone()));

        ReplicaManager {
            broker_id,
            log_manager: log_manager.clone(),
            zk_client: zk_client.clone(),
            partition_manager: partition_manager.clone(),
            ack_manager: ack_manager.clone(),

            _isr_update_task: isr_update_task(
                broker_id,
                partition_manager.clone(),
                ack_manager.clone(),
                zk_client.clone(),
            ),
            _watermark_cp_task: watermark_cp_task(partition_manager.clone()),
        }
    }

    /// Used to handle replica metadata changes and assignment
    /// from the controller.
    pub fn update_leader_or_follower(
        &self,
        leader_set: HashMap<TopicPartition, LeaderAndIsr>,
        follower_set: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> ReplicaResult<()> {
        for (topic_partition, leader_and_isr) in leader_set {
            // Wrong broker
            if leader_and_isr.leader != self.broker_id {
                return Err(Box::new(ReplicaError::Unknown(
                    "Invalid request leader_and_isr request".to_string(),
                )));
            }

            self.make_leader(topic_partition, leader_and_isr)?;
        }

        for (topic_partition, leader_and_isr) in follower_set {
            self.make_follower(topic_partition, leader_and_isr)?;
        }

        Ok(())
    }

    /// To make the broker a leader, it'll add/update the PartitionState
    /// to reflect this.
    pub fn make_leader(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> ReplicaResult<()> {
        if self
            .partition_manager
            .set_leader_partition(topic_partition.clone(), leader_and_isr)
        {
            Ok(())
        } else {
            Err(Box::new(ReplicaError::StaleEpoch(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    /// To make the broker a follower, it'll perform the following
    /// steps:
    /// 1) change the partition_state to follow (Note: make sure that the new leader is alive)
    /// 2) stop any ongoing fetcher threads for the partition
    /// 3) truncate the log to the last high watermark
    /// 4) add a watcher
    /// 5) start a fetch event for the new leader
    pub fn make_follower(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> ReplicaResult<()> {
        // 1)
        // Make sure that the leader actually exists
        let leader_info = self.zk_client.get_broker(leader_and_isr.leader)?;
        if leader_info.is_none() {
            return Err(Box::new(ReplicaError::InvalidLeader(leader_and_isr.leader)));
        }
        let leader_info = leader_info.unwrap();

        // change the partition_state to follow
        if !self.partition_manager.set_follower_partition(
            topic_partition.clone(),
            leader_info,
            leader_and_isr,
        ) {
            return Err(Box::new(ReplicaError::StaleEpoch(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )));
        }

        // 2)

        // 3)
        self.log_manager
            .get_or_create_log(&topic_partition)
            .truncate();

        // 4)

        // 5)
        Ok(())
    }

    /// Fetches the desired messages from the TopicPartitions. Alongside
    /// the messages, it also returns the current high_watermark.
    ///
    /// lib - If GC is added, then for the same reasons as log_manager,
    /// this should also return the first offset of the retrieved messages.
    pub async fn fetch_messages(
        &self,
        replica_id: Option<u32>,
        fetch_max_bytes: u64,
        partitions: Vec<(TopicPartition, u64)>,
    ) -> Vec<(TopicPartition, (Vec<u8>, u64))> {
        let limit_per_part = fetch_max_bytes / partitions.len() as u64;
        let mut result = Vec::new();

        for (partition, start) in &partitions {
            let log = self.log_manager.get_log(partition);
            if let Some(log) = log {
                let messages = log.fetch_messages(*start, limit_per_part, replica_id.is_none());
                let watermark = log.get_high_watermark();
                if messages.len() > 0 {
                    result.push((partition.clone(), (messages, watermark)));
                }
            }
        }

        let curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        if let Some(replica_id) = replica_id {
            for (partition, start) in &partitions {
                if let Some(partition_state) = self.partition_manager.get_partition_state(partition)
                {
                    partition_state.update_follower_fetch(replica_id, curr_time);
                    partition_state.update_follower_offset(replica_id, *start);

                    let min_isr_ack = partition_state.get_isr_ack();
                    let max_isr_ack = partition_state.get_max_isr_ack();

                    if let Some(ack_handler) = self.ack_manager.get_handler(partition) {
                        ack_handler.notify(min_isr_ack, max_isr_ack)
                    }
                }
            }
        }

        result
    }

    /// Only allow appends to 1 partition, this simplifies the logic as we'll only need to
    /// wait for 1 partition to be properly ack'd
    pub async fn append_messages(
        &self,
        required_acks: i8,
        topic_partition: TopicPartition,
        messages: Vec<u8>,
    ) -> ReplicaResult<()> {
        if ReplicaManager::is_valid_ack(required_acks) {
            // Only append_messages if we're actually the leader
            if self.partition_manager.is_leader(&topic_partition) {
                if required_acks != 0 {
                    // Depending on required_acks decide how long we'll have to wait
                    // See observer pattern: https://github.com/lpxxn/rust-design-pattern/blob/master/behavioral/observer.rs
                    // See notify: https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html

                    let ack_handler = self.ack_manager.get_or_create_handler(&topic_partition)?;
                    let notify = ack_handler.append_messages(messages, required_acks);

                    // Send notification
                    let _ = self.zk_client.set_topic_partition_offset(
                        &topic_partition.topic,
                        topic_partition.partition,
                        None,
                    )?;

                    // Wait for notification of ack
                    notify.notified().await;
                } else {
                    let _ = self.append_local(&topic_partition, messages)?;
                }

                Ok(())
            } else {
                Err(Box::new(ReplicaError::BrokerIsNotLeader(
                    topic_partition.topic.clone(),
                    topic_partition.partition,
                )))
            }
        } else {
            // Return some error
            Err(Box::new(ReplicaError::InvalidRequiredAck(required_acks)))
        }
    }

    /// Check if all nodes in the isr ack'd the
    /// replica.
    fn is_valid_ack(required_acks: i8) -> bool {
        required_acks == -1 || required_acks == 0 || required_acks == 1
    }

    /// Used by the ReplicaFetcher to get the leader of
    /// a partition
    pub fn get_leader_info(&self, topic_partition: &TopicPartition) -> ReplicaResult<BrokerInfo> {
        if let Some(leader_info) = self.partition_manager.get_leader_info(topic_partition) {
            Ok(leader_info)
        } else {
            Err(Box::new(ReplicaError::MissingPartition(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    // Append the messages to the local log for the topic partition
    pub fn append_local(
        &self,
        topic_partition: &TopicPartition,
        messages: Vec<u8>,
    ) -> ReplicaResult<u64> {
        let log = self.log_manager.get_log(topic_partition);
        if let Some(log) = log {
            Ok(log.append_messages(messages))
        } else {
            Err(Box::new(ReplicaError::MissingLog(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }
}
