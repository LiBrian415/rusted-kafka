use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::zk::zk_client::KafkaZkClient;

use super::{
    channel_manager::ControllerChannelManager, controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

pub struct ReplicaStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zk_client: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
}

impl ReplicaStateMachine {
    pub fn init(
        broker_id: u32,
        context: Rc<RefCell<ControllerContext>>,
        zk_client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> ReplicaStateMachine {
        Self {
            broker_id,
            context,
            zk_client,
            channel_manager,
            event_manager,
        }
    }

    pub fn startup() {
        todo!();
    }

    pub fn shutdown() {
        todo!();
    }

    pub fn handle_state_change() {
        todo!();
    }
}
