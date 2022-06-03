use std::{
    collections::HashMap,
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
    sync::{mpsc::Sender, Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::mpsc::{self, Receiver};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Response, Status};

use crate::{
    broker::{
        broker_server::{Broker, BrokerServer},
        ConsumerInput, ConsumerOutput, CreateInput, ProducerInput, Void,
    },
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    controller::controller_worker::ControllerWorker,
    zk::{zk_client::KafkaZkClient, zk_watcher::KafkaZkHandlers},
};

use super::{
    fetcher_manager::ReplicaFetcherManager, log_manager::LogManager,
    replica_manager::ReplicaManager,
};

fn parse_socket(addr: String) -> Result<SocketAddr, Box<(dyn Error + Send + Sync)>> {
    match addr.to_socket_addrs() {
        Ok(mut iter) => match iter.next() {
            Some(a) => Ok(a),
            // Not sure how to handle no dns available for socket from given domain so just parse and error out
            None => match addr.parse() {
                Ok(a) => Ok(a),
                Err(e) => return Err(Box::new(e)),
            },
        },
        Err(e) => return Err(Box::new(e)),
    }
}

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
    zk_client: Arc<KafkaZkClient>,
    controller: ControllerWorker,
    replica_manager: Arc<ReplicaManager>,
}

impl KafkaServer {
    pub async fn startup(
        addrs: Vec<String>,
        this: usize,
        ready: Option<Sender<bool>>,
        shutdown: Option<Receiver<()>>,
    ) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        let addr = parse_socket(addrs[this].to_owned())?;

        let zk_client = Arc::new(KafkaZkClient::init(
            "localhost:2181",
            Duration::from_secs(3),
        )?);
        zk_client.cleanup();
        zk_client.create_top_level_paths();

        // Start controller
        let broker_info = BrokerInfo::init(addrs[this].as_str(), "7777", this as u32);
        let broker_epoch = 0;
        let controller = ControllerWorker::startup(zk_client.clone(), broker_info, broker_epoch);
        controller.activate();

        let log_manager = Arc::new(LogManager::init());
        let replica_manager = Arc::new(ReplicaManager::init(
            0,
            None,
            log_manager,
            zk_client.clone(),
        ));

        let svc = BrokerServer::new(BrokerStream {
            zk_client,
            controller,
            replica_manager,
        });
        let server = Server::builder().add_service(svc);

        let ready_clone = ready.clone();
        let shutdown_rx = async {
            send_ready(ready_clone, true);
            wait_shutdown(shutdown).await;
        };

        let server_res = server.serve_with_shutdown(addr, shutdown_rx).await;
        // Cleanup any background thread tasks here
        // End cleanup
        match server_res {
            Ok(()) => Ok(()),
            Err(e) => {
                send_ready(ready, false);
                Err(Box::new(e))
            }
        }
    }
}

#[async_trait]
impl Broker for BrokerStream {
    type consumeStream = tokio_stream::wrappers::ReceiverStream<Result<ConsumerOutput, Status>>;

    async fn create(
        &self,
        request: tonic::Request<CreateInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let CreateInput { topics } = request.into_inner();

        match self.zk_client.create_new_topic(topics.clone()) {
            Ok(()) => match self.zk_client.get_partitions_for_topics(topics.clone()) {
                Ok(topic_partitions) => {
                    for topic in topics {
                        if let Some(partitions) = topic_partitions.get(&topic) {
                            for partition in partitions {
                                let tp = TopicPartition::init(&topic, partition.to_owned());
                                // self.replica_manager.make_leader(tp, leader_and_isr);
                            }
                        }
                    }
                    Ok(Response::new(Void {}))
                }
                Err(e) => Err(tonic::Status::unknown(e.to_string())),
            },
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn produce(
        &self,
        request: tonic::Request<ProducerInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let ProducerInput {
            topic,
            partition,
            messages,
        } = request.into_inner();

        let tp = TopicPartition::init(topic.as_str(), partition);

        // self.replica_manager.make_leader(tp, leader_and_isr);

        match self.replica_manager.append_messages(-1, tp, messages).await {
            Ok(()) => Ok(Response::new(Void {})),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn consume(
        &self,
        request: tonic::Request<ConsumerInput>,
    ) -> Result<tonic::Response<Self::consumeStream>, tonic::Status> {
        let ConsumerInput {
            topic,
            partition,
            offset,
        } = request.into_inner();

        let tp = TopicPartition::init(topic.as_str(), partition);

        let fetch_max_bytes = 128;

        todo!();
        // let (tx, rx) = mpsc::channel(fetch_max_bytes as usize);

        // let consume_log_manager = self.log_manager.clone();

        // tokio::spawn(async move {
        //     if let Some(log) = consume_log_manager.get_log(&tp) {
        //         let messages = log.fetch_messages(offset, fetch_max_bytes, true);
        //         match tx
        //             .send(Result::<_, Status>::Ok(ConsumerOutput {
        //                 messages,
        //                 end: true,
        //             }))
        //             .await
        //         {
        //             Ok(_) => {
        //                 // item (server response) was queued to be send to client
        //             }
        //             Err(_item) => {
        //                 // output_stream was build from rx and both are dropped
        //             }
        //         }
        //     }
        // });

        // let output_stream = ReceiverStream::new(rx);
        // Ok(Response::new(output_stream as Self::consumeStream))
    }

    async fn delete_all(
        &self,
        request: tonic::Request<Void>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let Void {} = request.into_inner();

        self.zk_client.cleanup();

        Ok(Response::new(Void {}))
    }
}
