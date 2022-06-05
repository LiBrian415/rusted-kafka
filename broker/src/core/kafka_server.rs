use core::time;
use std::{
    collections::HashMap,
    error::Error,
    net::{SocketAddr, ToSocketAddrs},
    sync::{mpsc::Sender, Arc, RwLock},
    thread,
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::mpsc::{self, Receiver};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Response, Status};

use crate::{
    broker::{
        broker_server::{Broker, BrokerServer},
        ConsumerInput, ConsumerOutput, CreateInput, LeaderAndIsr as RpcLeaderAndIsr, ProducerInput,
        TopicPartition as RpcTopicPartition, TopicPartitionLeaderInput, Void,
    },
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
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

fn parse_address(addr: String) -> Result<(String, String), Box<(dyn Error + Send + Sync)>> {
    match addr.rsplit_once(':') {
        Some((host, port)) => Ok((host.to_owned(), port.to_owned())),
        None => Ok((addr, "".to_owned())),
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
    broker_info: BrokerInfo,
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

        let broker_id = this as u32;

        // Start controller
        let (host, port) = match parse_address(addrs[this].clone()) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };
        let broker_info = BrokerInfo::init(host.as_str(), port.as_str(), broker_id);
        let broker_epoch = 0;
        let controller =
            ControllerWorker::startup(zk_client.clone(), broker_info.clone(), broker_epoch);
        controller.activate();

        let log_manager = Arc::new(LogManager::init());
        let replica_manager = Arc::new(ReplicaManager::init(
            broker_id,
            None,
            log_manager,
            zk_client.clone(),
        ));

        let svc = BrokerServer::new(BrokerStream {
            zk_client,
            broker_info,
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

    async fn set_topic_partition_leader(
        &self,
        request: tonic::Request<TopicPartitionLeaderInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let TopicPartitionLeaderInput {
            topic_partition,
            leader_and_isr,
        } = request.into_inner();

        let RpcTopicPartition { topic, partition } = match topic_partition {
            Some(tp) => tp,
            None => return Err(tonic::Status::invalid_argument("Missing TopicPartition")),
        };
        let RpcLeaderAndIsr {
            leader,
            isr,
            leader_epoch,
            controller_epoch,
        } = match leader_and_isr {
            Some(lisr) => lisr,
            None => return Err(tonic::Status::invalid_argument("Missing LeaderAndIsr")),
        };

        match self.replica_manager.become_leader_or_follower(
            TopicPartition { topic, partition },
            LeaderAndIsr {
                leader,
                isr,
                leader_epoch: leader_epoch as u128,
                controller_epoch: controller_epoch as u128,
            },
        ) {
            Ok(()) => Ok(Response::new(Void {})),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn create(
        &self,
        request: tonic::Request<CreateInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let CreateInput { topic_partitions } = request.into_inner();

        let topic_partitions: Vec<(String, u32)> = topic_partitions
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partitions))
            .collect();

        match self
            .zk_client
            .create_new_topic("greeting".to_string(), 1, 2)
        {
            Ok(()) => {
                thread::sleep(time::Duration::from_secs(2));
                Ok(Response::new(Void {}))
            }
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
            max,
        } = request.into_inner();

        let tp = TopicPartition::init(topic.as_str(), partition);

        let fetch_max_bytes = max as usize;

        let (tx, rx) = mpsc::channel(fetch_max_bytes);

        let consume_broker_info = self.broker_info.clone();
        let consume_replica_manager = self.replica_manager.clone();

        tokio::spawn(async move {
            let messages = consume_replica_manager
                .fetch_messages(
                    Some(consume_broker_info.id),
                    u64::max_value(),
                    vec![(tp, offset)],
                )
                .await;

            let messages = match messages
                .into_iter()
                .find(|(tp, _)| tp.topic == topic.as_str() && tp.partition == partition)
            {
                Some(m) => m,
                None => return Err(tonic::Status::not_found("TopicPartition not found")),
            };

            let (_tp, (messages, _high_watermark)) = messages;

            let len = messages.len();
            let mut start = 0;
            while start < len {
                let end = std::cmp::min(fetch_max_bytes, len - start);
                let send = messages[start..(start + end)].to_vec();
                match tx
                    .send(Result::<_, Status>::Ok(ConsumerOutput { messages: send }))
                    .await
                {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                        start += end;
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                    }
                };
            }
            Ok(())
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream as Self::consumeStream))
    }
}
