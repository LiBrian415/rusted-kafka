use std::{
    net::SocketAddr,
    sync::{mpsc::Sender, Arc},
};

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use tonic::{transport::Server, Status};

use crate::{
    broker::{
        broker_server::{Broker, BrokerServer},
        ConsumerInput, ConsumerOutput, ProducerInput, Void,
    },
    zk::zk_client::KafkaZkClient,
};

use super::{fetcher_manager::ReplicaFetcherManager, log_manager::LogManager};

fn send_ready(sender: Option<Sender<bool>>, ready: bool) -> Option<()> {
    let tx = sender?;
    tx.send(ready).expect("Sending ready failed.");
    Some(())
}

async fn wait_shutdown(receiver: Option<Receiver<()>>) {
    match receiver {
        Some(mut rx) => {
            rx.recv().await;
        }
        None => {
            let (_tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
            rx.recv().await;
        }
    }
}

pub struct KafkaServer;
struct BrokerStream {
    logManager: Arc<LogManager>,
}

impl KafkaServer {
    pub async fn startup(
        addr: SocketAddr,
        ready: Option<Sender<bool>>,
        shutdown: Option<Receiver<()>>,
    ) -> Result<(), tonic::transport::Error> {
        // let zkClient = Arc::new(KafkaZkClient::init());

        let logManager = Arc::new(LogManager::init());
        // let fetchManager = Arc::new(ReplicaFetcherManager::init(logManager));

        let svc = BrokerServer::new(BrokerStream { logManager });
        let server = Server::builder().add_service(svc);

        let ready_clone = ready.clone();
        let shutdown_rx = async {
            send_ready(ready_clone, true);
            wait_shutdown(shutdown).await;
        };

        server
            .serve_with_shutdown(addr, shutdown_rx)
            .await
            .map_err(|e| {
                send_ready(ready, false);
                e
            })
    }
}

#[async_trait]
impl Broker for BrokerStream {
    type consumeStream = tokio_stream::wrappers::ReceiverStream<Result<ConsumerOutput, Status>>;

    async fn produce(
        &self,
        input: tonic::Request<ProducerInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        todo!();
    }

    async fn consume(
        &self,
        input: tonic::Request<ConsumerInput>,
    ) -> Result<tonic::Response<Self::consumeStream>, tonic::Status> {
        todo!();
    }

    async fn clock(&self, _: tonic::Request<Void>) -> Result<tonic::Response<Void>, tonic::Status> {
        todo!();
    }
}
