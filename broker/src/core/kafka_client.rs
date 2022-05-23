use std::error::Error;

use tokio::sync::Mutex;
use tonic::{transport::Channel, Streaming};

use crate::broker::{
    broker_client::BrokerClient, ConsumerInput, ConsumerOutput, ProducerInput, Void,
};

pub struct KafkaClient {
    addr: String,
    client: Mutex<Option<BrokerClient<Channel>>>,
}

impl KafkaClient {
    pub fn new(addr: String) -> Self {
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

    async fn produce(
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

    async fn consume(
        &self,
        topic: String,
        partition: u32,
    ) -> Result<Streaming<ConsumerOutput>, Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        let resp = client.consume(ConsumerInput { topic, partition }).await?;
        Ok(resp.into_inner())
    }

    async fn clock(&self) -> Result<Void, Box<(dyn Error + Send + Sync)>> {
        let mut client = self.connect().await?;
        todo!();
    }
}
