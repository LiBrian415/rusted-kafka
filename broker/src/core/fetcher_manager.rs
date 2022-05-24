use std::{error::Error, sync::Arc};

use crate::{
    broker::ConsumerOutput, common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient,
};

use super::{kafka_client::KafkaClient, log_manager::LogManager};

pub struct ReplicaFetcherManager {
    zkClient: Arc<KafkaZkClient>,
    logManager: Arc<LogManager>,
}

async fn fetcherTask(
    zkClient: Arc<KafkaZkClient>,
    logManager: Arc<LogManager>,
    topic: String,
    partition: u32,
) -> Result<(), Box<(dyn Error + Send + Sync)>> {
    // 1) get leader
    let client = KafkaClient::new(format!("http://{}", "dummy"));

    // get local log manager and current offset
    let log = match logManager.get_log(&TopicPartition::init(topic.as_str(), partition)) {
        Some(l) => l,
        None => {
            return Err(Box::<dyn Error + Send + Sync>::from(
                "No log found for topic/partition",
            ));
        }
    };
    let offset = 0;

    // 2) fetch from leader
    let mut iter = client.consume(topic, partition, offset).await?;
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
    pub fn init(zkClient: Arc<KafkaZkClient>, logManager: Arc<LogManager>) -> Self {
        return Self {
            zkClient,
            logManager,
        };
    }

    pub async fn fetch(&self, path: String) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        // TODO need code to make sure for a given topic/partition we don't create multiple threads to fetch
        self.createFetcherThread("".to_owned(), 0).await?;
        Ok(())
    }

    async fn createFetcherThread(
        &self,
        topic: String,
        partition: u32,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let task = tokio::spawn(fetcherTask(
            self.zkClient.clone(),
            self.logManager.clone(),
            topic,
            partition,
        ));
        task.await?;
        Ok(())
    }
}
