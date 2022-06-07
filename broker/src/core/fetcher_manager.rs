use std::{
    error::Error,
    sync::{Arc, RwLock},
};

use tokio::runtime::Runtime;
use tonic::Streaming;

use crate::{
    broker::ConsumerOutput, common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient,
};

use super::{
    kafka_client::KafkaClient,
    replica_manager::{self, ReplicaManager},
};

pub struct ReplicaFetcherManager {
    zk_client: Arc<KafkaZkClient>,
    replica_manager: RwLock<Option<Arc<ReplicaManager>>>,
}

fn deserialize(messages: Vec<u8>) -> (String, usize) {
    // eprintln!("raw message {:?}", String::from_utf8(messages.clone()));
    let len = u32::from_be_bytes(messages[0..4].try_into().unwrap()) as usize + 4;
    let l = std::cmp::min(len, messages.len());
    let mut padded: Vec<u8> = vec![0; 4];
    padded.append(&mut messages[4..l].to_vec());
    (String::from_utf8(padded).unwrap(), len)
}

impl ReplicaFetcherManager {
    pub fn init(zk_client: Arc<KafkaZkClient>) -> Self {
        return Self {
            zk_client,
            replica_manager: RwLock::new(None),
        };
    }

    pub fn fetch(&self, topic_partition: TopicPartition, offset: u64) {
        // TODO need code to make sure for a given topic/partition we don't create multiple threads to fetch
        self.create_fetcher_thread(topic_partition, offset);
    }

    pub fn set_replica_manager(&self, replica_manager: Arc<ReplicaManager>) {
        let mut write = self
            .replica_manager
            .write()
            .expect("[fetcher] [set_replica_manager] replica_manager write guard");
        *write = Some(replica_manager);
    }

    fn get_replica_manager(&self) -> Arc<ReplicaManager> {
        let read = self
            .replica_manager
            .read()
            .expect("[fetcher] [get_replica_manager] replica_manager read guard");
        read.as_ref()
            .expect("[fetcher] [get_replica_manager] No replica_manager set")
            .clone()
    }

    fn create_fetcher_thread(&self, topic_partition: TopicPartition, offset: u64) {
        let fetch_zk_client = self.zk_client.clone();
        let fetch_replica_manager = self.get_replica_manager();
        let task = tokio::spawn(async move {
            println!("[fetcher] [fetcher_task] fetcher task started");
            // 1) get leader
            let leader_info = match fetch_replica_manager.get_leader_info(&topic_partition) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[fetcher] [fetcher_task] get leader info error {:?}", e);
                    return;
                }
            };
            let leader_client = KafkaClient::new(leader_info.hostname, leader_info.port);

            // 2) fetch from leader
            let mut iter = match leader_client
                .consume(
                    topic_partition.topic.clone(),
                    topic_partition.partition,
                    offset,
                    128,
                )
                .await
            {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("[fetcher] [fetcher_task] consume error from leader {:?}", e);
                    return;
                }
            };
            while let Some(res) = iter
                .message()
                .await
                .expect("[fetcher] [fetcher_task] iter fetch error from leader")
            {
                let ConsumerOutput { messages } = res;
                println!(
                    "[fetcher] [fetcher_task] fetcher fetched message {:?}",
                    deserialize(messages.clone())
                );
                // 3) call log manager to append log
                match fetch_replica_manager.append_local(&topic_partition, messages) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[fetcher] [fetcher_task] failed to append to replica manager local {:?}", e);
                    }
                };
            }
            // 4) re-set zk watch
            match fetch_zk_client
                .get_topic_partition_offset(&topic_partition.topic, topic_partition.partition)
            {
                Ok(_) => {}
                Err(e) => {
                    eprintln!(
                        "[fetcher] [fetcher_task] failed to reset zk client watch {:?}",
                        e
                    );
                    return;
                }
            };

            println!("[fetcher] [fetcher_task] fetcher task finished");
        });
    }
}
