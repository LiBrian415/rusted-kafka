use std::error::Error;

use tokio::sync::Mutex;
use tonic::{transport::Channel, Streaming};

use crate::{
    broker::{
        broker_client::BrokerClient, ConsumerInput, ConsumerOutput, CreateInput,
        LeaderAndIsr as RpcLeaderAndIsr, ProducerInput, TopicPartition as RpcTopicPartition,
        TopicPartitionLeaderInput, TopicPartitions,
    },
    common::topic_partition::{LeaderAndIsr, TopicPartition},
};

pub struct KafkaClient {
    addr: String,
    client: Mutex<Option<BrokerClient<Channel>>>,
}

impl KafkaClient {
    pub fn new(host: String, port: String) -> Self {
        let addr = format!("http://{}:{}", host, port);
        Self {
            addr,
            client: Mutex::new(None),
        }
    }

    async fn connect(&self) -> Result<BrokerClient<Channel>, Box<(dyn Error + Send + Sync)>> {
        let mut guard = self.client.lock().await;
        match guard.as_ref() {
            Some(client) => Ok(client.clone()),
            None => {
                let client = BrokerClient::connect(self.addr.clone()).await?;
                *guard = Some(client.clone());
                Ok(client)
            }
        }
    }

    pub async fn topic_partition_leader(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        let TopicPartition { topic, partition } = topic_partition;
        let LeaderAndIsr {
            leader,
            isr,
            leader_epoch,
            controller_epoch,
        } = leader_and_isr;
        client
            .topic_partition_leader(TopicPartitionLeaderInput {
                topic_partition: Some(RpcTopicPartition { topic, partition }),
                leader_and_isr: Some(RpcLeaderAndIsr {
                    leader,
                    isr,
                    leader_epoch: leader_epoch as u64,
                    controller_epoch: controller_epoch as u64,
                }),
            })
            .await?;
        Ok(())
    }

    pub async fn create(
        &self,
        topic_partitions: Vec<(String, u32)>,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        let topic_partitions = topic_partitions
            .iter()
            .map(|(topic, partitions)| TopicPartitions {
                topic: topic.to_owned(),
                partitions: partitions.to_owned(),
            })
            .collect();
        client.create(CreateInput { topic_partitions }).await?;
        Ok(())
    }

    pub async fn produce(
        &self,
        topic: String,
        partition: u32,
        messages: Vec<u8>,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        client
            .produce(ProducerInput {
                topic,
                partition,
                messages,
            })
            .await?;
        Ok(())
    }

    pub async fn consume(
        &self,
        topic: String,
        partition: u32,
        offset: u64,
    ) -> Result<Streaming<ConsumerOutput>, Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        let resp = client
            .consume(ConsumerInput {
                topic,
                partition,
                offset,
            })
            .await?;
        Ok(resp.into_inner())
    }
}
