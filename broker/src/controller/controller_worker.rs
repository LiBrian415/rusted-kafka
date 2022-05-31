use crate::common::broker::BrokerInfo;
use crate::zk::zk_client::KafkaZkClient;

use super::{
    controller::start_controller,
    controller_events::{ControllerEvent, RegisterBrokerAndReElect, Startup},
};
use std::{
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
    thread::{self, JoinHandle},
};

pub struct ControllerWorker {
    event_tx: SyncSender<Box<dyn ControllerEvent>>, // tx channel for activating the controller
    _handle: JoinHandle<()>, // controller thread handle, just put here, maybe will never be used
}

impl ControllerWorker {
    pub fn startup(
        zk_client: Arc<KafkaZkClient>,
        broker_info: BrokerInfo,
        broker_epoch: u128,
    ) -> ControllerWorker {
        let (tx, rx) = sync_channel(5);
        let event_tx = tx.clone();
        let handle = thread::spawn(move || {
            start_controller(zk_client, broker_info, broker_epoch, event_tx, rx)
        });

        ControllerWorker {
            event_tx: tx,
            _handle: handle,
        }
    }

    pub fn activate(&self) {
        // TODO: maybe an expire event here
        let _ = self.event_tx.send(Box::new(RegisterBrokerAndReElect {}));
        let _ = self.event_tx.send(Box::new(Startup {}));
    }
}
