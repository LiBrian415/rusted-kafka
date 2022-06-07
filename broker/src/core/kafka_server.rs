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
    println!("{}", ready);
    match tx.send(ready) {
        Ok(_) => {}
        Err(e) => {
            println!("ready channel error: {}", e);
        }
    }
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
    controller: Arc<ControllerWorker>,
    replica_manager: Arc<ReplicaManager>,
    event_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<()>>,
    event_tx: tokio::sync::mpsc::Sender<()>,
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

        let broker_id = this as u32;

        // Start controller
        let (host, port) = match parse_address(addrs[this].clone()) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };
        let broker_info = BrokerInfo::init(host.as_str(), port.as_str(), broker_id);
        let broker_epoch = 0;
        let controller = Arc::new(ControllerWorker::startup(
            zk_client.clone(),
            broker_info.clone(),
            broker_epoch,
        ));
        controller.activate();
        println!("CONTROLLER ACTIVATED");
        let controller_clone = controller.clone();

        let log_manager = Arc::new(LogManager::init());
        let replica_manager = ReplicaManager::init(broker_id, None, log_manager, zk_client.clone());

        let (event_tx, unlock_event_rx) = tokio::sync::mpsc::channel(1);
        let event_rx = tokio::sync::Mutex::new(unlock_event_rx);
        let svc = BrokerServer::new(BrokerStream {
            zk_client,
            broker_info,
            controller,
            replica_manager,
            event_tx,
            event_rx,
        });
        let server = Server::builder().add_service(svc);

        let ready_clone = ready.clone();
        let shutdown_rx = async {
            send_ready(ready_clone, true);
            wait_shutdown(shutdown).await;
            controller_clone.shutdown();
        };
        println!("EVERYTHING READY!");
        let server_res = server.serve_with_shutdown(addr, shutdown_rx).await;
        // Cleanup any background thread tasks here
        // End cleanup
        match server_res {
            Ok(()) => Ok(()),
            Err(e) => {
                send_ready(ready, false);
                eprintln!("server startup error ({:?}) {:?}", addr, e);
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
            Ok(()) => {
                let _ = self.event_tx.send(()).await;
                Ok(Response::new(Void {}))
            }
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn create(
        &self,
        request: tonic::Request<CreateInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let CreateInput { topic_partitions } = request.into_inner();

        if let Some(topic_partitions) = topic_partitions {
            match self.zk_client.create_new_topic(
                topic_partitions.topic,
                topic_partitions.partitions as usize,
                topic_partitions.replicas as usize,
            ) {
                Ok(()) => {
                    let _ = self.event_rx.lock().await.recv().await;
                    Ok(Response::new(Void {}))
                }
                Err(e) => Err(tonic::Status::unknown(e.to_string())),
            }
        } else {
            Err(tonic::Status::invalid_argument("Missing TopicPartitions"))
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
