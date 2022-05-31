use std::{error::Error, sync::Arc};

use crate::{
    broker::ConsumerOutput, common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient,
};

use super::{
    kafka_client::KafkaClient,
    replica_manager::{self, ReplicaManager},
};

pub struct ReplicaFetcherManager {
    replica_manager: Arc<ReplicaManager>,
}

async fn fetcher_task(
    replica_manager: Arc<ReplicaManager>,
    topic_partition: TopicPartition,
    offset: u64,
) -> Result<(), Box<(dyn Error + Send + Sync)>> {
    // 1) get leader
    let leader_info = replica_manager.get_leader_info(&topic_partition)?;
    let leader_client = KafkaClient::new(leader_info.hostname, leader_info.port);

    // 2) fetch from leader
    let mut iter = leader_client
        .consume(topic_partition.topic, topic_partition.partition, offset)
        .await?;
    while let Some(res) = iter.message().await? {
        let ConsumerOutput { messages, end } = res;
        // 3) call log manager to append log
        // log.append_messages(messages);
        println!("{:?}, {:?}", messages, end);
        // 4) repeat 2 if still data
    }
    // 5) re-set zk watch
    Ok(())
}

impl ReplicaFetcherManager {
    pub fn init(replica_manager: Arc<ReplicaManager>) -> Self {
        return Self { replica_manager };
    }

    pub async fn fetch(
        &self,
        topic_partition: TopicPartition,
        offset: u64,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        // TODO need code to make sure for a given topic/partition we don't create multiple threads to fetch
        self.create_fetcher_thread(topic_partition, offset).await?;
        Ok(())
    }

    async fn create_fetcher_thread(
        &self,
        topic_partition: TopicPartition,
        offset: u64,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let task = tokio::spawn(fetcher_task(
            self.replica_manager.clone(),
            topic_partition,
            offset,
        ));
        task.await?;
        Ok(())
    }
}
