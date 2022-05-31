use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};

use crate::{common::topic_partition::TopicPartition, zk::zk_client::KafkaZkClient};

use super::{
    channel_manager::ControllerChannelManager, controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

const NEW_PARTITION: u32 = 0;
const ONLINE_PARTITION: u32 = 1;
const OFFLINE_PARTITION: u32 = 2;

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

    pub fn handle_state_change(
        &self,
        partitions: HashSet<TopicPartition>,
        target_state: Box<dyn PartitionState>,
    ) {
        todo!();
    }

    pub fn trigger_online_partition_state_change(&self) {
        todo!();
    }
}

pub trait PartitionState {
    fn state(&self) -> u32;
}

pub struct NewPartition {}
impl PartitionState for NewPartition {
    fn state(&self) -> u32 {
        NEW_PARTITION
    }
}

pub struct OnlinePartition {}
impl PartitionState for OnlinePartition {
    fn state(&self) -> u32 {
        ONLINE_PARTITION
    }
}

pub struct OfflinePartition {}
impl PartitionState for OfflinePartition {
    fn state(&self) -> u32 {
        OFFLINE_PARTITION
    }
}
