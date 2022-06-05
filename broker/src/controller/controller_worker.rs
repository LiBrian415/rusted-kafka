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
        let (tx, rx) = sync_channel(1);

        // TODO: maybe an expire event here
        let _ = self.event_tx.send(Box::new(RegisterBrokerAndReElect {}));
        let _ = self.event_tx.send(Box::new(Startup { tx }));
        let _ = rx.recv();
    }
}

#[cfg(test)]
mod controller_test {
    use std::{sync::Arc, time::Duration};

    use crate::{
        common::broker::BrokerInfo, controller::controller_events::BrokerChange,
        zk::zk_client::KafkaZkClient,
    };

    use super::ControllerWorker;

    const CONN_STR: &str = "localhost:2181";
    const SESS_TIMEOUT: Duration = Duration::from_secs(3);

    fn get_client() -> Arc<KafkaZkClient> {
        let client = KafkaZkClient::init(CONN_STR, SESS_TIMEOUT);
        assert!(!client.is_err());
        Arc::new(client.unwrap())
    }

    #[test]
    fn test_broker_change() {
        let client = get_client();
        client.cleanup();
        client.create_top_level_paths();

        // Start controller
        let broker_info = BrokerInfo::init("localhost", "7777", 1);
        let broker_epoch = 0;
        let controller = ControllerWorker::startup(client.clone(), broker_info, broker_epoch);
        controller.activate();
        let _ = controller.event_tx.send(Box::new(BrokerChange {}));
    }
}
