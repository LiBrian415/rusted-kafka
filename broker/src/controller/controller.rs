use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};
use zookeeper::ZooKeeper;

use super::{
    change_handlers::{get_change_handlers, get_child_change_handlers},
    constants::{
        EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE, EVENT_RE_ELECT,
        EVENT_STARTUP, EVENT_TOPIC_CHNAGE,
    },
    controller_events::Startup,
    event_manager::ControllerEventManager,
};
use crate::controller::controller_events::ControllerEvent;
use crate::{
    common::topic_partition::TopicIdReplicaAssignment,
    controller::controller_context::ControllerContext,
};
use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{TopicPartition},
    },
    controller::change_handlers::ControllerChangeHandler,
    zk::{zk_client::KafkaZkClient, zk_watcher::KafkaZkHandlers},
};

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
    pub fn init(
        &mut self,
        zk_client: ZooKeeper,
        init_broker_info: BrokerInfo,
        init_broker_epoch: u128,
    ) {
        let event_manager = Arc::new(ControllerEventManager::init(Arc::new(self.clone())));

        let client = KafkaZkClient {
            client: zk_client,
            handlers: KafkaZkHandlers {
                change_handlers: Arc::new(RwLock::new(get_change_handlers(
                    event_manager.clone(),
                    init_broker_info.id,
                ))),
                child_change_handlers: Arc::new(RwLock::new(get_child_change_handlers(
                    event_manager.clone(),
                ))),
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

    pub fn startup(&self) {
        self.em.put(Box::new(Startup {}));
        self.em.start();
    }

    // process functions for different events
    pub fn process(&self, event: Arc<Box<dyn ControllerEvent>>) {
        match event.state() {
            EVENT_STARTUP => self.process_startup(),
            EVENT_CONTROLLER_CHANGE => self.process_controller_change(),
            EVENT_RE_ELECT => self.process_re_elect(),
            EVENT_BROKER_CHANGE => self.process_broker_change(),
            EVENT_BROKER_MODIFICATION => self.process_broker_modification(),
            EVENT_TOPIC_CHNAGE => self.process_topic_change(),
            _ => {}
        }
    }

    fn process_startup(&self) {
        self.zk_client
            .register_znode_change_handler(Arc::new(Box::new(ControllerChangeHandler {
                event_manager: self.em.clone(),
            })));
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
            if id != 0 {
                // init value, maybe TODO
                return;
            }
        }

        match self
            .zk_client
            .register_controller_and_increment_controller_epoch(self.broker_id)
        {
            Ok(resp) => {
                let mut context = self.context.write().unwrap();
                (*context).epoch = resp.0;
                (*context).epoch_zk_version = resp.1;
                let mut active_id = self.active_controller_id.write().unwrap();
                *active_id = self.broker_id;

                self.controller_failover();
            }
            Err(_) => {
                // TODO: handle ControllerMovedException and others
            }
        };
    }

    /// invoked when this broker is elected as the new controller
    fn controller_failover(&self) {
        // register wathcers to get broker/topic callbacks
        let _: Vec<()> = self
            .zk_client
            .handlers
            .child_change_handlers
            .read()
            .unwrap()
            .iter()
            .map(|(_, handler)| {
                self.zk_client
                    .register_znode_child_change_handler(handler.clone())
            })
            .collect();

        let _: Vec<()> = self
            .zk_client
            .handlers
            .change_handlers
            .read()
            .unwrap()
            .iter()
            .map(|(_, handler)| {
                self.zk_client
                    .register_znode_change_handler(handler.clone())
            })
            .collect();

        // initialize the controller's context that holds cache objects for current topics, live brokers
        // leaders for all existing partitions
        self.zk_client
            .delete_isr_change_notifications(self.context.read().unwrap().epoch_zk_version);
        self.initialize_control_context();
        // TODO: delete topics here l260

        // Send UpdateMetadata after the context is initialized and before the state machines startup.
        // reason: brokers need to receive the list of live brokers from UpdateMetadata before they can
        // process LeaderAndIsrRequests that are generated by replicaStateMachine.startup()
        self.send_update_metadata_request(
            self.context
                .read()
                .unwrap()
                .live_brokers
                .clone()
                .into_iter()
                .collect(),
            HashSet::new(),
        );

        // starts the replica state machine
        // self.replica_state_machine.startup();

        // TODO: starts the partition state machine

        // if any unexpected errors/ resigns as the current controller
    }

    fn initialize_control_context(&self) {
        let curr_broker_and_epochs = self.zk_client.get_all_broker_and_epoch();
        // TODO: Here the source code check the "compatibility" of broker and epoch.
        // I dont really understand why they do this check, so I'll assume all brokers
        // and epoches are compatibile
        let mut context = self.context.write().unwrap();
        context.set_live_brokers(curr_broker_and_epochs);

        match self.zk_client.get_all_topics(true) {
            Ok(topics) => context.set_all_topics(topics),
            Err(e) => {
                // TODO
            }
        }

        // TODO: Here the source code register the partitionModification handlers with zk

        match self
            .zk_client
            .get_replica_assignment_and_topic_ids_for_topics(context.all_topics.clone())
        {
            Ok(replica_assignment_and_topic_ids) => {
                self.process_topic_ids(replica_assignment_and_topic_ids.clone());
                for topic_id_assignment in replica_assignment_and_topic_ids {
                    let _: Vec<()> = topic_id_assignment
                        .assignment
                        .iter()
                        .map(|(partition, assignment)| {
                            context.update_partition_full_replica_assignment(
                                partition.clone(),
                                assignment.clone(),
                            );
                            if assignment.is_being_reassigned() {
                                context
                                    .partitions_being_reassigned
                                    .insert(partition.clone());
                            }
                        })
                        .collect();
                }
            }
            Err(e) => {}
        }

        context.clear_partition_leadership_info();
        context.shuttingdown_broker_ids.clear();

        // register_broker_modification_handler(self.context.live_or_shutting_down_broker_ids);
        std::mem::drop(context);
        self.update_leader_and_isr_cache();
        // start the channel manager
    }

    fn update_leader_and_isr_cache(&self) {
        let mut context = self.context.write().unwrap();
        let partitions = context.all_partitions();
        match self.zk_client.get_topic_partition_states(partitions) {
            Ok(leader_isr_and_epochs) => {
                for (partition, (leader_isr, epoch)) in leader_isr_and_epochs {
                    context.put_partition_leadership_info(partition, leader_isr, epoch);
                }
            }
            Err(e) => {}
        }
    }
    fn send_update_metadata_request(&self, brokers: Vec<u32>, partitions: HashSet<TopicPartition>) {
        todo!();
    }

    fn process_topic_ids(&self, topic_id_assignment: HashSet<TopicIdReplicaAssignment>) {
        todo!();
    }
}
