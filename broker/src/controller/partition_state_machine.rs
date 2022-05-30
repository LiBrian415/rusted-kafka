use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::zk::zk_client::KafkaZkClient;

use super::{
    channel_manager::ControllerChannelManager, controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

pub struct PartitionStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zkClient: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
}

impl PartitionStateMachine {
    pub fn init(
        id: u32,
        context: Rc<RefCell<ControllerContext>>,
        client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> PartitionStateMachine {
        todo!();
    }
    pub fn startup(&self) {
        todo!();
    }

    pub fn shutdown(&self) {
        todo!();
    }

    pub fn handle_state_change() {
        todo!();
    }

    pub fn trigger_online_partition_state_change() {
        todo!();
    }
}
