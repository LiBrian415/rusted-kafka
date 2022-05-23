use std::{error::Error, sync::Arc};

use crate::{common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient};

use super::log_manager::LogManager;

pub struct ReplicaFetcherManager {
    zkClient: Arc<KafkaZkClient>,
    logManager: Arc<LogManager>,
}

async fn fetcherTask(
    zkClient: Arc<KafkaZkClient>,
    logManager: Arc<LogManager>,
    tp: TopicPartition,
) {
    // 1) TODO: get leader
    // 2) TODO: fetch from leader
    // 3) call log manager to append log
    // 4) repeat 2 if still data
    // 5) re-set zk watch
}

impl ReplicaFetcherManager {
    pub fn init(zkClient: Arc<KafkaZkClient>, logManager: Arc<LogManager>) -> Self {
        return Self {
            zkClient,
            logManager,
        };
    }

    pub async fn fetch(&self, path: String) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        // TODO need code to make sure for a given topic/partition we don't create multiple threads to fetch
        let tp = TopicPartition::init("", 0);
        self.createFetcherThread(tp).await?;
        Ok(())
    }

    async fn createFetcherThread(
        &self,
        tp: TopicPartition,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let task = tokio::spawn(fetcherTask(
            self.zkClient.clone(),
            self.logManager.clone(),
            tp,
        ));
        task.await?;
        Ok(())
    }
}
