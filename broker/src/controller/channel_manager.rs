use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::RwLock};

use tokio::sync::Mutex;

use crate::{
    common::broker::BrokerInfo, controller::controller_context::ControllerContext,
    core::kafka_client::KafkaClient,
};

use super::event_manager::ControllerEventManager;

pub struct BrokerClient {
    broker_info: BrokerInfo,
    broker_client: KafkaClient,
}

// A structure responsible for managing the active broker connections
pub struct ControllerChannelManager {
    context: Rc<RefCell<ControllerContext>>,
    brokers: RwLock<HashMap<u32, BrokerClient>>,
}

// A structure responsible for sending controller requests
pub struct ControllerBrokerRequestBatch {
    broker_id: u32,
    event_manager: ControllerEventManager,
    channel_manager: ControllerChannelManager,
}

impl ControllerChannelManager {
    pub fn init(context: Rc<RefCell<ControllerContext>>) -> ControllerChannelManager {
        Self {
            context,
            brokers: RwLock::new(HashMap::new()),
        }
    }

    pub fn startup(&self) {
        todo!();
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
        let broker_client = KafkaClient::new(hostname, port);

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

impl ControllerBrokerRequestBatch {
    pub fn init() -> ControllerBrokerRequestBatch {
        todo!();
    }

    pub fn add_leader_and_isr_request_for_brokers(&self) {
        todo!();
    }

    pub fn add_update_metadata_request_fro_brokers(&self) {
        todo!();
    }

    pub fn send_request_to_brokers(&self) {
        // send request, and if success, send an event over eventManager
        todo!();
    }
}
