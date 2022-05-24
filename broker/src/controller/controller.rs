use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use zookeeper::ZooKeeper;

use super::{
    change_handlers::{
        get_change_handlers, get_child_change_handlers, BrokerChangeHandler,
        BrokerModificationHandler, TopicChangeHandler,
    },
    constants::{
        EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE, EVENT_RE_ELECT,
        EVENT_STARTUP, EVENT_TOPIC_CHNAGE,
    },
    controller_events::Startup,
    event_manager::ControllerEventManager,
};
use crate::controller::controller_events::{BrokerModification, ControllerEvent};
use crate::{
    common::topic_partition::TopicIdReplicaAssignment,
    controller::controller_context::ControllerContext,
};
use crate::{
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    controller::change_handlers::ControllerChangeHandler,
    zk::{zk_client::KafkaZkClient, zk_watcher::KafkaZkHandlers},
};

use crate::zk::zk_watcher::{ZkChangeHandler, ZkChildChangeHandler};

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
    broker_modification_handlers: Arc<RwLock<HashMap<u32, BrokerModificationHandler>>>,
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
            EVENT_BROKER_MODIFICATION => self.process_broker_modification(
                event
                    .as_any()
                    .downcast_ref::<BrokerModification>()
                    .unwrap()
                    .broker_id,
            ),
            EVENT_TOPIC_CHNAGE => self.process_topic_change(),
            _ => {}
        }
    }

    /***** Process Functions Entry Points for Events Start *****/
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

    fn process_broker_modification(&self, broker_id: u32) {
        if !self.is_active() {
            return;
        }

        let mut context = self.context.write().unwrap();
        let new_meta = match self.zk_client.get_broker(broker_id) {
            Ok(broker) => broker,
            Err(_) => {
                return;
            }
        };
        let old_meta = context.live_or_shutting_down_broker(broker_id);
        if !new_meta.is_none() && !old_meta.is_none() {
            let old = old_meta.unwrap();
            let new = new_meta.unwrap();
            if new.hostname != old.hostname && new.port != old.port {
                context.update_broker_metadata(old, new);
                self.on_broker_update(broker_id);
            }
        }
    }

    fn process_topic_change(&self) {
        todo!();
    }

    fn process_controller_change(&self) {
        self.maybe_resign();
    }

    fn process_re_elect(&self) {
        self.maybe_resign();
        self.elect();
    }

    /***** Process Functions Entry Points for Events End *****/
    fn is_active(&self) -> bool {
        *self.active_controller_id.read().unwrap() == self.broker_id
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
                .live_or_shutting_down_broker_ids()
                .into_iter()
                .collect::<Vec<u32>>(),
            HashSet::new(),
        );

        // TODO: starts the replica state machine
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
            Err(_) => {
                // TODO: should we return anything?
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
            Err(_) => {
                // TODO: maybe do something
            }
        }

        context.clear_partition_leadership_info();
        context.shuttingdown_broker_ids.clear();

        self.register_broker_modification_handler(context.live_or_shutting_down_broker_ids());
        std::mem::drop(context);
        self.update_leader_and_isr_cache();
        // TODO: start the channel manager
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
            Err(_) => {
                // TODO
            }
        }
    }

    fn send_update_metadata_request(&self, brokers: Vec<u32>, partitions: HashSet<TopicPartition>) {
        // do this later, may need a discussion on grpc interface
        todo!();
    }

    fn process_topic_ids(&self, topic_id_assignment: HashSet<TopicIdReplicaAssignment>) {
        // Add topic IDs to controller context, Do we really need topic ids tho?
        todo!();
    }

    fn register_broker_modification_handler(&self, broker_ids: HashSet<u32>) {
        let _ = broker_ids
            .iter()
            .map(|&id| {
                let handler = BrokerModificationHandler {
                    event_manager: self.em.clone(),
                    broker_id: id,
                };
                self.zk_client
                    .register_znode_change_handler(Arc::new(Box::new(handler)));
                // source code has brokerModificationsHandlers, but i dont think we need it
            })
            .collect::<Vec<()>>();
    }

    fn unregister_broker_modification_handler(&self, brokers_id: Vec<u32>) {
        let mut broker_modification_handlers = self.broker_modification_handlers.write().unwrap();

        for id in brokers_id {
            broker_modification_handlers.remove(&id);
            let handler = broker_modification_handlers.get(&id).unwrap();
            self.zk_client
                .unregister_znode_change_handler(handler.path().as_str());
        }
    }

    fn maybe_resign(&self) {
        let was_active_before_change = self.is_active();

        self.zk_client
            .register_znode_change_handler_and_check_existence(Arc::new(Box::new(
                ControllerChangeHandler {
                    event_manager: self.em.clone(),
                },
            )));

        *self.active_controller_id.write().unwrap() = match self.zk_client.get_controller_id() {
            Ok(id_opt) => match id_opt {
                Some(id) => id,
                None => 0,
            },
            Err(_) => 0, // TODO: init value
        };

        if was_active_before_change && !self.is_active() {
            self.on_controller_resignation();
        }
    }

    fn on_broker_update(&self, _update_broker_id: u32) {
        let context = self.context.read().unwrap();
        self.send_update_metadata_request(
            context
                .live_or_shutting_down_broker_ids()
                .into_iter()
                .collect::<Vec<u32>>(),
            HashSet::new(),
        )
    }

    /* From Kafka src code:
     * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
     * required to clean up internal controller data structures
     */
    fn on_controller_resignation(&self) {
        // TODO: unregister watchers maybe need to add more
        let guard = self.broker_modification_handlers.read().unwrap();
        let broker_ids: Vec<u32> = guard.keys().cloned().collect();
        std::mem::drop(guard);
        self.unregister_broker_modification_handler(broker_ids);

        // shutdown leader rebalance scheduler?

        // stop other things?

        self.zk_client.unregister_znode_child_change_handler(
            TopicChangeHandler {
                event_manager: self.em.clone(),
            }
            .path()
            .as_str(),
        );

        self.zk_client.unregister_znode_child_change_handler(
            BrokerChangeHandler {
                event_manager: self.em.clone(),
            }
            .path()
            .as_str(),
        );

        // shutdown channel manager

        self.context.write().unwrap().reset_context();
    }
}
