use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};

use crate::{common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient};

use super::{
    channel_manager::ControllerChannelManager, controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

const NEW_REPLICA: u32 = 0;
const ONLINE_REPLICA: u32 = 1;
const OFFLINE_REPLICA: u32 = 2;

pub struct ReplicaStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zkClient: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
}

impl ReplicaStateMachine {
    pub fn init(
        id: u32,
        context: Rc<RefCell<ControllerContext>>,
        client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> ReplicaStateMachine {
        todo!();
    }

    pub fn startup(&self) {
        todo!();
    }

    pub fn shutdown(&self) {
        todo!();
    }

    pub fn handle_state_change(
        &self,
        replicas: HashSet<(TopicPartition, i32)>,
        target_state: Box<dyn ReplicaState>,
    ) {
        todo!();
    }
}

pub trait ReplicaState {
    fn state(&self) -> u32;
}

pub struct NewReplica {}
impl ReplicaState for NewReplica {
    fn state(&self) -> u32 {
        NEW_REPLICA
    }
}

pub struct OnlineReplica {}
impl ReplicaState for OnlineReplica {
    fn state(&self) -> u32 {
        ONLINE_REPLICA
    }
}

pub struct OfflineReplica {}
impl ReplicaState for OfflineReplica {
    fn state(&self) -> u32 {
        OFFLINE_REPLICA
    }
}
