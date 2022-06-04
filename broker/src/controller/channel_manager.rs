use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Arc, RwLock},
    thread,
};

use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    controller::controller_context::ControllerContext,
    core::kafka_client::KafkaClient,
};

use super::event_manager::ControllerEventManager;
use tokio::runtime::Runtime;

pub struct BrokerClient {
    broker_info: BrokerInfo,
    broker_client: Arc<KafkaClient>,
}

// A structure responsible for managing the active broker connections
pub struct ControllerChannelManager {
    context: Rc<RefCell<ControllerContext>>,
    brokers: RwLock<HashMap<u32, BrokerClient>>,
}

impl ControllerChannelManager {
    pub fn init(context: Rc<RefCell<ControllerContext>>) -> ControllerChannelManager {
        Self {
            context,
            brokers: RwLock::new(HashMap::new()),
        }
    }

    pub fn startup(&self) {
        let context = self.context.borrow();
        let brokers = context.live_or_shutting_down_brokers();
        for broker in brokers {
            self.add_broker(broker);
        }
        println!("channel manager started");
    }

    pub fn shutdown(&self) {
        todo!();
    }

    pub fn remove_broker(&self, broker_id: u32) {
        let mut w = self.brokers.write().unwrap();
        w.remove(&broker_id);
    }

    pub fn add_broker(&self, broker_info: BrokerInfo) {
        let BrokerInfo {
            hostname,
            port,
            id: _,
        } = broker_info.clone();
        let broker_client = Arc::new(KafkaClient::new(hostname, port));

        println!(
            "broker {}:{} is added",
            broker_info.hostname, broker_info.port
        );
        let mut w = self.brokers.write().unwrap();
        (*w).insert(
            broker_info.id,
            BrokerClient {
                broker_info,
                broker_client,
            },
        );
    }

    pub fn send_request(&self) {
        let dummy_id = 0;
        let r = self.brokers.read().unwrap();
        let client = r.get(&dummy_id);
        todo!();
    }
}

// A structure responsible for sending controller requests
pub struct ControllerBrokerRequestBatch {
    broker_id: u32,
    event_manager: ControllerEventManager,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    leader_and_isr_requests: HashMap<u32, (TopicPartition, LeaderAndIsr)>,
}

impl ControllerBrokerRequestBatch {
    pub fn init(
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        broker_id: u32,
        event_manager: ControllerEventManager,
    ) -> ControllerBrokerRequestBatch {
        let leader_and_isr_requests = HashMap::new();
        Self {
            broker_id,
            event_manager,
            channel_manager,
            leader_and_isr_requests,
        }
    }

    pub fn add_leader_and_isr_request_for_brokers(
        &mut self,
        broker_ids: Vec<u32>,
        partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) {
        for id in broker_ids {
            self.leader_and_isr_requests
                .insert(id, (partition.clone(), leader_and_isr.clone()));
        }
    }

    pub fn add_update_metadata_request_fro_brokers(&self) {
        todo!();
    }

    pub fn send_request_to_brokers(&self, epoch: u128) {
        // send request, and if success, send an event over eventManager
        let rt = Runtime::new().unwrap();
        let handle = rt.handle();
        println!(
            "we have {} messages to send",
            self.leader_and_isr_requests.len()
        );

        for (broker, message) in self.leader_and_isr_requests.iter() {
            let manager = self.channel_manager.borrow();
            let client = match manager.brokers.read().unwrap().get(&broker) {
                Some(c) => c.broker_client.clone(),
                None => {
                    println!("no client found");
                    return;
                }
            };

            println!("start sending");
            handle.block_on(async {
                println!("send out request");
                let _ = client
                    .set_topic_partition_leader(message.0.clone(), message.1.clone())
                    .await;
            });
        }
    }

    pub fn new_batch(&mut self) {
        self.leader_and_isr_requests.clear();
    }
}
