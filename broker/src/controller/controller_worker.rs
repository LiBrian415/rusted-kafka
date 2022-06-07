use crate::common::broker::BrokerInfo;
use crate::zk::zk_client::KafkaZkClient;

use super::{
    controller::start_controller,
    controller_events::{ControllerEvent, RegisterBrokerAndReElect, Shutdown, Startup},
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

    pub fn shutdown(&self) {
        let (tx, rx) = sync_channel(1);
        let _ = self.event_tx.send(Box::new(Shutdown { tx }));
        let _ = rx.recv();
    }
}

#[cfg(test)]
mod controller_test {
    use core::time;
    use std::{sync::Arc, thread, time::Duration};

    use crate::{common::broker::BrokerInfo, zk::zk_client::KafkaZkClient};

    use super::ControllerWorker;

    const CONN_STR: &str = "localhost:2181";
    const SESS_TIMEOUT: Duration = Duration::from_secs(3);

    fn get_client() -> Arc<KafkaZkClient> {
        let client = KafkaZkClient::init(CONN_STR, SESS_TIMEOUT);
        assert!(!client.is_err());
        Arc::new(client.unwrap())
    }

    #[test]
    fn test_controller_fail() {
        let client1 = get_client();
        client1.cleanup();
        client1.create_top_level_paths();
        let client2 = get_client();

        // Start controller
        let broker_info1 = BrokerInfo::init("localhost", "7777", 1);
        let broker_info2 = BrokerInfo::init("localhost", "6666", 2);
        let broker_epoch = 0;
        let controller1 = ControllerWorker::startup(client1.clone(), broker_info1, broker_epoch);
        let controller2 = ControllerWorker::startup(client2.clone(), broker_info2, broker_epoch);
        controller1.activate();
        controller2.activate();
        controller1.shutdown();

        thread::sleep(time::Duration::from_secs(3));
        assert_eq!(client2.get_controller_id().unwrap().unwrap(), 2);
    }

    #[test]
    fn test_broker_change() {
        let client1 = get_client();
        client1.cleanup();
        client1.create_top_level_paths();
        let client2 = get_client();

        // Start controller
        let broker_info1 = BrokerInfo::init("localhost", "7777", 1);
        let broker_info2 = BrokerInfo::init("localhost", "6666", 2);
        let broker_epoch = 0;
        let controller1 = ControllerWorker::startup(client1.clone(), broker_info1, broker_epoch);
        let controller2 = ControllerWorker::startup(client2.clone(), broker_info2, broker_epoch);
        controller1.activate();
        controller2.activate();
        controller2.shutdown();

        thread::sleep(time::Duration::from_secs(3));
    }

    #[test]
    fn test_topic_change_with_alive_leader() {
        let client1 = get_client();
        client1.cleanup();
        client1.create_top_level_paths();
        let client2 = get_client();

        // Start controller
        let broker_info1 = BrokerInfo::init("localhost", "7777", 1);
        let broker_info2 = BrokerInfo::init("localhost", "6666", 2);
        let broker_epoch = 0;
        let controller1 = ControllerWorker::startup(client1.clone(), broker_info1, broker_epoch);
        let controller2 = ControllerWorker::startup(client2.clone(), broker_info2, broker_epoch);
        controller1.activate();
        controller2.activate();

        let _ = client2.create_new_topic("greeting".to_string(), 1, 2);
        thread::sleep(time::Duration::from_secs(3));
        controller2.shutdown();

        thread::sleep(time::Duration::from_secs(3));
    }

    #[test]
    fn test_topic_change_with_dead_leader() {
        let client1 = get_client();
        client1.cleanup();
        client1.create_top_level_paths();
        let client2 = get_client();

        // Start controller
        let broker_info1 = BrokerInfo::init("localhost", "7777", 1);
        let broker_info2 = BrokerInfo::init("localhost", "6666", 2);
        let broker_epoch = 0;
        let controller1 = ControllerWorker::startup(client1.clone(), broker_info1, broker_epoch);
        let controller2 = ControllerWorker::startup(client2.clone(), broker_info2, broker_epoch);

        // controller2 becomes the controller so that we can kill broker1 (default leader of a partition)
        controller2.activate();
        controller1.activate();

        let _ = client2.create_new_topic("greeting".to_string(), 1, 2);
        thread::sleep(time::Duration::from_secs(3));
        controller1.shutdown();

        thread::sleep(time::Duration::from_secs(3));
    }
}
