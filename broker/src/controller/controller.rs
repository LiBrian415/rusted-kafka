use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::{
        mpsc::{Receiver, SyncSender},
        Arc, RwLock,
    },
};

use super::{
    change_handlers::{
        BrokerChangeHandler, BrokerModificationHandler, ControllerChangeHandler, TopicChangeHandler,
    },
    constants::{
        EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE,
        EVENT_CONTROLLER_SHUTDOWN, EVENT_ISR_CHANGE_NOTIFICATION,
        EVENT_LEADER_AND_ISR_RESPONSE_RECEIVED, EVENT_REGISTER_BROKER_AND_REELECT,
        EVENT_REPLICA_LEADER_ELECTION, EVENT_RE_ELECT, EVENT_STARTUP, EVENT_TOPIC_CHNAGE,
        EVENT_UPDATE_METADATA_RESPONSE_RECEIVED,
    },
    event_manager::ControllerEventManager,
};
use crate::controller::controller_events::{BrokerModification, ControllerEvent};
use crate::{
    common::topic_partition::TopicIdReplicaAssignment,
    controller::controller_context::ControllerContext,
};
use crate::{
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    zk::zk_client::KafkaZkClient,
};

use crate::controller::channel_manager::ControllerChannelManager;
use crate::controller::partition_state_machine::PartitionStateMachine;
use crate::controller::replica_state_machine::ReplicaStateMachine;
use crate::zk::zk_watcher::{ZkChangeHandler, ZkChildChangeHandler};

pub struct Controller {
    broker_id: u32,
    active_controller_id: RwLock<u32>,
    zk_client: Arc<KafkaZkClient>,
    broker_info: BrokerInfo,
    broker_epoch: u128,
    em: ControllerEventManager,
    event_rx: Receiver<Box<dyn ControllerEvent>>,
    context: Rc<RefCell<ControllerContext>>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    partition_state_machine: PartitionStateMachine,
    replica_state_machine: ReplicaStateMachine,
    broker_modification_handlers: RwLock<HashMap<u32, BrokerModificationHandler>>,
}

impl Controller {
    pub fn startup(&self) {
        loop {
            match self.event_rx.recv() {
                Ok(event) => {
                    self.process(event);
                }
                Err(_) => {}
            }
        }
    }

    // process functions for different events
    pub fn process(&self, event: Box<dyn ControllerEvent>) {
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
            EVENT_REPLICA_LEADER_ELECTION => todo!(),
            EVENT_CONTROLLER_SHUTDOWN => todo!(),
            EVENT_LEADER_AND_ISR_RESPONSE_RECEIVED => todo!(),
            EVENT_UPDATE_METADATA_RESPONSE_RECEIVED => todo!(),
            EVENT_REGISTER_BROKER_AND_REELECT => todo!(),
            EVENT_ISR_CHANGE_NOTIFICATION => todo!(),
            _ => {}
        }
    }

    /***** Process Functions Entry Points for Events Start *****/
    fn process_startup(&self) {
        self.zk_client
            .register_znode_change_handler(Arc::new(ControllerChangeHandler {
                event_manager: self.em.clone(),
            }));
        self.elect();
    }

    fn process_broker_change(&self) {
        todo!();
    }

    fn process_broker_modification(&self, broker_id: u32) {
        if !self.is_active() {
            return;
        }

        let mut context = self.context.borrow_mut();
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
                std::mem::drop(context);
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
                let mut context = self.context.borrow_mut();
                (*context).epoch = resp.0;
                (*context).epoch_zk_version = resp.1;
                let mut active_id = self.active_controller_id.write().unwrap();
                *active_id = self.broker_id;

                std::mem::drop(context);
                std::mem::drop(active_id);

                self.controller_failover();
            }
            Err(_) => {
                // TODO: handle ControllerMovedException and others
            }
        };
    }

    // invoked when this broker is elected as the new controller
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
        let context = self.context.borrow();
        self.zk_client
            .delete_isr_change_notifications(context.epoch_zk_version);
        self.initialize_control_context();
        // TODO: delete topics here l260

        // Send UpdateMetadata after the context is initialized and before the state machines startup.
        // reason: brokers need to receive the list of live brokers from UpdateMetadata before they can
        // process LeaderAndIsrRequests that are generated by replicaStateMachine.startup()
        self.send_update_metadata_request(
            context
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
        let curr_broker_and_epochs = match self.zk_client.get_all_broker_and_epoch() {
            Ok(data) => data,
            Err(_) => {
                return;
            }
        };
        // TODO: Here the source code check the "compatibility" of broker and epoch.
        // I dont really understand why they do this check, so I'll assume all brokers
        // and epoches are compatibile
        let mut context = self.context.borrow_mut();
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
        let mut context = self.context.borrow_mut();
        let partitions = context.all_partitions();
        match self.zk_client.get_topic_partition_states(partitions) {
            Ok(leader_isr_and_epochs) => {
                for (partition, leader_isr) in leader_isr_and_epochs {
                    context.put_partition_leadership_info(
                        partition,
                        leader_isr.clone(),
                        leader_isr.controller_epoch,
                    );
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

    fn process_topic_ids(&self, topic_id_assignment: Vec<TopicIdReplicaAssignment>) {
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
                    .register_znode_change_handler(Arc::new(handler));
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
            .register_znode_change_handler_and_check_existence(Arc::new(ControllerChangeHandler {
                event_manager: self.em.clone(),
            }));

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
        let context = self.context.borrow();
        let broker_ids = context
            .live_or_shutting_down_broker_ids()
            .into_iter()
            .collect::<Vec<u32>>();
        std::mem::drop(context);
        self.send_update_metadata_request(broker_ids, HashSet::new());
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

        self.context.borrow_mut().reset_context();
    }
}

pub fn start_controller(
    zk_client: Arc<KafkaZkClient>,
    init_broker_info: BrokerInfo,
    init_broker_epoch: u128,
    event_tx: SyncSender<Box<dyn ControllerEvent>>,
    event_rx: Receiver<Box<dyn ControllerEvent>>,
) {
    let id = init_broker_info.id;
    let event_manager = ControllerEventManager::init(event_tx.clone());
    let control_context = Rc::new(RefCell::new(ControllerContext::init()));
    let channel_manager = Rc::new(RefCell::new(ControllerChannelManager::init(
        control_context.clone(),
    )));

    let controller = Controller {
        broker_id: id,
        active_controller_id: RwLock::new(0),
        zk_client: zk_client.clone(),
        broker_info: init_broker_info,
        broker_epoch: init_broker_epoch,
        em: event_manager.clone(),
        event_rx: event_rx,
        context: control_context.clone(),
        broker_modification_handlers: RwLock::new(HashMap::new()),
        replica_state_machine: ReplicaStateMachine::init(
            id,
            control_context.clone(),
            zk_client.clone(),
            channel_manager.clone(),
            event_manager.clone(),
        ),
        partition_state_machine: PartitionStateMachine::init(
            id,
            control_context.clone(),
            zk_client.clone(),
            channel_manager.clone(),
            event_manager.clone(),
        ),
        channel_manager: channel_manager,
    };

    controller.startup();
}
