use std::{cell::RefCell, rc::Rc};

use crate::{common::broker::BrokerInfo, controller::controller_context::ControllerContext};

use super::event_manager::ControllerEventManager;

// A structure responsible for managing the active broker connections
pub struct ControllerChannelManager {
    context: Rc<RefCell<ControllerContext>>,
}

// A structure responsible for sending controller requests
pub struct ControllerBrokerRequestBatch {
    broker_id: u32,
    event_manager: ControllerEventManager,
    channel_manager: ControllerChannelManager,
}

impl ControllerChannelManager {
    pub fn init(context: Rc<RefCell<ControllerContext>>) -> ControllerChannelManager {
        todo!();
    }

    pub fn startup(&self) {
        todo!();
    }

    pub fn shutdown(&self) {
        todo!();
    }

    fn add_new_broker(&self, broker: BrokerInfo) {
        // add a new broker to the managed broker set
        todo!();
    }

    pub fn remove_broker(&self, broker_id: u32) {
        todo!();
    }

    pub fn add_broker(&self, broker: BrokerInfo) {
        todo!();
    }

    pub fn send_request() {
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
