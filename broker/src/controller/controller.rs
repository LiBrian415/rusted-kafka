use zookeeper::ZooKeeper;
use std::{sync::{Arc, RwLock}, collections::HashSet};

use crate::{zk::{zk_client::KafkaZkClient, zk_watcher::KafkaZkHandlers}, common::{broker::BrokerInfo, topic_partition::TopicPartition}, controller::change_handlers::ControllerChangeHandler};
use super::{event_manager::{ControllerEventManager}, change_handlers::{get_change_handlers, get_child_change_handlers}, 
            constants::{EVENT_STARTUP, EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE, EVENT_RE_ELECT, EVENT_TOPIC_CHNAGE}, 
            controller_events::Startup};
use crate::controller::controller_events::ControllerEvent;
use crate::controller::controller_context::ControllerContext;

#[derive(Clone)]
pub struct Controller {
    broker_id: u32,
    active_controller_id: Arc<RwLock<u32>>,
    zk_client: Arc<KafkaZkClient>,
    broker_info: Arc<BrokerInfo>,
    broker_epoch: Arc<RwLock<u128>>,
    em: Arc<ControllerEventManager>,
    context: Arc<RwLock<ControllerContext>>,
    // channel_manager: ControllerChannelManager,
}

impl Controller {
    pub fn init(mut self, zk_client: ZooKeeper, init_broker_info: BrokerInfo, init_broker_epoch: u128) {
        let event_manager = Arc::new(ControllerEventManager::init(Arc::new(self.clone())));

        let client = KafkaZkClient {
            client: zk_client,
            handlers: KafkaZkHandlers {
                change_handlers: Arc::new(RwLock::new(get_change_handlers(event_manager.clone(), init_broker_info.id))),
                child_change_handlers: Arc::new(RwLock::new(get_child_change_handlers(event_manager.clone()))),
            },
        };
        
        self.broker_id = init_broker_info.id;
        self.zk_client = Arc::new(client);
        self.broker_info = Arc::new(init_broker_info);
        self.broker_epoch = Arc::new(RwLock::new(init_broker_epoch));
        self.em = event_manager;
        self.active_controller_id = Arc::new(RwLock::new(0));
        // context: ControllerContext.init(),
        // channel_manager: ControllerChannelManager.init(),
    }

    pub async fn startup(&self) {
        self.em.put(Box::new(Startup {})).await;
        self.em.start();
    }

    // process functions for different events
    pub fn process(&self, event: Arc<Box<dyn ControllerEvent>>) {
        match event.state() {
            EVENT_STARTUP => self.process_startup(),
            EVENT_CONTROLLER_CHANGE=> self.process_controller_change(),
            EVENT_RE_ELECT => self.process_re_elect(),
            EVENT_BROKER_CHANGE => self.process_broker_change(),
            EVENT_BROKER_MODIFICATION => self.process_broker_modification(),
            EVENT_TOPIC_CHNAGE => self.process_topic_change(),
            _ => {},
        }
    }

    fn process_startup(&self) {
        self.zk_client.register_znode_change_handler(Arc::new(Box::new(ControllerChangeHandler {event_manager: self.em.clone()})));
        self.elect();
    }

    fn process_broker_change(&self) {
        todo!();
    }

    fn process_broker_modification(&self) {
        todo!();
    }

    fn process_topic_change(&self) {
        todo!();
    }

    fn process_controller_change(&self) {
        todo!();
    }

    fn process_re_elect(&self) {
        todo!();
    }

    fn elect(&self) {
        if let Ok(Some(id)) = self.zk_client.get_controller_id() {
            let mut active_id = self.active_controller_id.write().unwrap();
            *active_id = id;
            if id != 0 { // init value, maybe TODO
                return;
            }
        }

        match self.zk_client.register_controller_and_increment_controller_epoch(
            self.broker_id) {
                Ok(resp) => {
                    let mut context = self.context.write().unwrap();
                    (*context).epoch = resp.0;
                    (*context).epoch_zk_version = resp.1;
                    let mut active_id = self.active_controller_id.write().unwrap();
                    *active_id = self.broker_id;
                
                    self.controller_failover();
                },
                Err(_) => {
                    // TODO: handle ControllerMovedException and others
                }
        };
    }

    /// invoked when this broker is elected as the new controller
    fn controller_failover(&self) {
        // register wathcers to get broker/topic callbacks
        let _: Vec<()> = self.zk_client.handlers.child_change_handlers.read().unwrap().iter().map(|(_, handler)| 
            self.zk_client.register_znode_child_change_handler(handler.clone())).collect();

        let _: Vec<()> = self.zk_client.handlers.change_handlers.read().unwrap().iter().map(|(_, handler)| 
            self.zk_client.register_znode_change_handler(handler.clone())).collect();
        
        // initialize the controller's context that holds cache objects for current topics, live brokers
        // leaders for all existing partitions
        self.zk_client.delete_isr_change_notifications(self.context.read().unwrap().epoch_zk_version);
        self.initialize_control_context();
        // TODO: delete topics here l260

        // Send UpdateMetadata after the context is initialized and before the state machines startup.
        // reason: brokers need to receive the list of live brokers from UpdateMetadata before they can
        // process LeaderAndIsrRequests that are generated by replicaStateMachine.startup()
        self.send_update_metadata_request(self.context.read().unwrap().live_brokers.clone().into_iter().collect(), HashSet::new());

        // starts the replica state machine
        // self.replica_state_machine.startup();
        
        // TODO: starts the partition state machine

        // if any unexpected errors/ resigns as the current controller
    }

    fn initialize_control_context(&self) {
        todo!();
    }

    fn send_update_metadata_request(&self, brokers: Vec<u32>, partitions: HashSet<TopicPartition>) {
        todo!();
    }

}