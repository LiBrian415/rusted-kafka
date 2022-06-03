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
        get_child_change_handlers, BrokerChangeHandler, BrokerModificationHandler,
        ControllerChangeHandler, TopicChangeHandler,
    },
    constants::{
        EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE,
        EVENT_CONTROLLER_SHUTDOWN, EVENT_ISR_CHANGE_NOTIFICATION,
        EVENT_LEADER_AND_ISR_RESPONSE_RECEIVED, EVENT_REGISTER_BROKER_AND_REELECT,
        EVENT_REPLICA_LEADER_ELECTION, EVENT_RE_ELECT, EVENT_STARTUP, EVENT_TOPIC_CHANGE,
        EVENT_UPDATE_METADATA_RESPONSE_RECEIVED,
    },
    controller_events::ReplicaLeaderElection,
    event_manager::ControllerEventManager,
    partition_state_machine::{NewPartition, OfflinePartition, OnlinePartition},
    replica_state_machine::{NewReplica, OfflineReplica, OnlineReplica},
};
use crate::{
    common::topic_partition::TopicIdReplicaAssignment,
    controller::controller_context::ControllerContext,
};
use crate::{
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    zk::zk_client::KafkaZkClient,
};
use crate::{
    controller::controller_events::{BrokerModification, ControllerEvent},
    zk::zk_data::IsrChangeNotificationZNode,
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
            EVENT_STARTUP => {
                self.process_startup();
                event.complete();
            }
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
            EVENT_TOPIC_CHANGE => self.process_topic_change(),
            EVENT_REPLICA_LEADER_ELECTION => {
                let partitions = event
                    .as_any()
                    .downcast_ref::<ReplicaLeaderElection>()
                    .unwrap()
                    .partitions
                    .clone();
                self.process_replica_leader_election(partitions);
            }
            EVENT_REGISTER_BROKER_AND_REELECT => self.process_register_broker_and_reelect(),
            EVENT_ISR_CHANGE_NOTIFICATION => self.process_isr_change_notification(),
            EVENT_CONTROLLER_SHUTDOWN => todo!(),
            EVENT_LEADER_AND_ISR_RESPONSE_RECEIVED => todo!(),
            EVENT_UPDATE_METADATA_RESPONSE_RECEIVED => todo!(),
            _ => {}
        }
    }

    /***** Process Functions Entry Points for Events Start *****/
    fn process_startup(&self) {
        println!("process startup");
        let _ = self
            .zk_client
            .register_znode_change_handler_and_check_existence(Arc::new(ControllerChangeHandler {
                event_manager: self.em.clone(),
            }));
        self.elect();
        println!("processed startup");
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

    fn process_controller_change(&self) {
        self.maybe_resign();
    }

    fn process_re_elect(&self) {
        self.maybe_resign();
        println!("maybe resign done");
        self.elect();
        println!("elect done");
    }

    fn process_broker_change(&self) {
        if !self.is_active() {
            return;
        }

        let mut curr_broker_and_epochs = match self.zk_client.get_all_broker_and_epoch() {
            Ok(resp) => resp,
            Err(_) => {
                return;
            }
        };

        let mut curr_broker_id_and_epoch = HashMap::new();
        for (broker, epoch) in curr_broker_and_epochs.iter() {
            curr_broker_id_and_epoch.insert(broker.id, epoch);
        }

        let curr_broker_ids: HashSet<u32> = curr_broker_id_and_epoch.keys().cloned().collect();
        let live_or_shutting_down_broker_ids =
            self.context.borrow().live_or_shutting_down_broker_ids();

        let new_broker_ids: Vec<u32> = curr_broker_ids
            .difference(&live_or_shutting_down_broker_ids)
            .cloned()
            .collect();
        let dead_broker_ids: Vec<u32> = live_or_shutting_down_broker_ids
            .difference(&curr_broker_ids)
            .cloned()
            .collect();

        curr_broker_and_epochs.retain(|broker, _| new_broker_ids.contains(&broker.id));
        let new_broker_and_epoch = curr_broker_and_epochs;

        for (broker, _) in new_broker_and_epoch.iter() {
            self.channel_manager.borrow_mut().add_broker(broker.clone());
        }

        for id in dead_broker_ids.iter() {
            self.channel_manager.borrow_mut().remove_broker(id.clone());
        }

        if !new_broker_ids.is_empty() {
            self.context
                .borrow_mut()
                .add_live_brokers(new_broker_and_epoch);
            self.on_broker_startup(new_broker_ids);
        }

        if !dead_broker_ids.is_empty() {
            self.context
                .borrow_mut()
                .remove_live_brokers(dead_broker_ids.clone());
            self.on_broker_failure(dead_broker_ids);
        }
    }

    fn process_topic_change(&self) {
        println!("start process topic change");
        if !self.is_active() {
            return;
        }

        let mut context = self.context.borrow_mut();

        let topics = match self.zk_client.get_all_topics(true) {
            Ok(topics) => topics,
            Err(_) => {
                return;
            }
        };
        let new_topics = topics.difference(&context.all_topics).cloned().collect();
        // let deleted_topics: HashSet<String> =
        //     context.all_topics.difference(&topics).cloned().collect();

        context.set_all_topics(topics);

        let added_partition_replica_assignment = match self
            .zk_client
            .get_replica_assignment_and_topic_ids_for_topics(new_topics)
        {
            Ok(resp) => resp,
            Err(_) => {
                return;
            }
        };

        // for topic in deleted_topics {
        //     context.remove_topic(topic);
        // }

        let _: Vec<()> = added_partition_replica_assignment
            .iter()
            .map(|id_assignment| {
                let assignment = id_assignment.assignment.clone();
                for (partition, new_assignment) in assignment {
                    context.update_partition_full_replica_assignment(partition, new_assignment);
                }
            })
            .collect();

        std::mem::drop(context);
        let mut partitions = HashSet::new();
        if !added_partition_replica_assignment.is_empty() {
            let _: Vec<()> = added_partition_replica_assignment
                .iter()
                .map(|id_assignment| {
                    let assignment = id_assignment.assignment.clone();
                    partitions.extend(assignment.keys().cloned());
                })
                .collect();
            self.on_new_partition_creation(partitions);
        }
    }

    fn process_register_broker_and_reelect(&self) {
        match self.zk_client.register_broker(self.broker_info.clone()) {
            Ok(_) => {
                self.process_re_elect();
                println!("broker has been registerd and reelct is done");
            }
            Err(_) => {
                return;
            }
        }
    }

    fn process_isr_change_notification(&self) {
        if !self.is_active() {
            return;
        }

        let seq_num = match self.zk_client.get_all_isr_change_notifications() {
            Ok(resp) => resp,
            Err(_) => {
                return;
            }
        };

        match self
            .zk_client
            .get_partitions_from_isr_change_notifications(seq_num.clone())
        {
            Ok(partitions) => {
                if !partitions.is_empty() {
                    self.update_leader_and_isr_cache(partitions.clone());
                    self.process_update_notifications(partitions);
                }
            }
            Err(_) => {
                return;
            }
        }
        let _ = self
            .zk_client
            .delete_isr_change_notifications_with_sequence_num(
                seq_num,
                self.context.borrow().epoch_zk_version,
            );
    }

    fn process_replica_leader_election(&self, partitions: HashSet<TopicPartition>) {
        if !self.is_active() {
            return;
        }

        let all_partitions = self.context.borrow().all_partitions();
        let mut known_partitions = partitions.clone();
        known_partitions.retain(|p| all_partitions.contains(p));

        // TODO: need to check
        self.on_replica_election(known_partitions);
    }
    /***** Process Functions Entry Points for Events End *****/
    fn is_active(&self) -> bool {
        *self.active_controller_id.read().unwrap() == self.broker_id
    }

    fn elect(&self) {
        if let Ok(Some(id)) = self.zk_client.get_controller_id() {
            let mut active_id = self.active_controller_id.write().unwrap();
            *active_id = id;
            std::mem::drop(active_id);
            if id != 0 {
                // The controller has been elected
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

                println!("controller failover start");
                self.controller_failover();
                println!("controller failover done");
            }
            Err(_) => {
                // TODO: handle ControllerMovedException and others
                self.maybe_resign()
            }
        };
    }

    // invoked when this broker is elected as the new controller
    fn controller_failover(&self) {
        // register watchers to get broker/topic callbacks
        let _: Vec<()> = get_child_change_handlers(self.em.clone())
            .iter()
            .map(|handler| {
                self.zk_client
                    .register_znode_child_change_handler(handler.clone())
            })
            .collect();

        println!("child change handler registered");
        // initialize the controller's context that holds cache objects for current topics, live brokers
        // leaders for all existing partitions
        let context = self.context.borrow();
        let _ = self
            .zk_client
            .delete_isr_change_notifications(context.epoch_zk_version);
        std::mem::drop(context);
        println!("deleted isr change notification");
        self.initialize_control_context();
        println!("initialized controller context");

        // Send UpdateMetadata after the context is initialized and before the state machines startup.
        // reason: brokers need to receive the list of live brokers from UpdateMetadata before they can
        // process LeaderAndIsrRequests that are generated by replicaStateMachine.startup()
        let context = self.context.borrow();
        self.send_update_metadata_request(
            context
                .live_or_shutting_down_broker_ids()
                .into_iter()
                .collect::<Vec<u32>>(),
            HashSet::new(),
        );

        std::mem::drop(context);
        self.replica_state_machine.startup();
        self.partition_state_machine.startup();
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

        // Here the source code register the partitionModification handlers with zk

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
        self.update_leader_and_isr_cache(Vec::new());
        self.channel_manager.borrow_mut().startup();
    }

    fn update_leader_and_isr_cache(&self, partitions_given: Vec<TopicPartition>) {
        let mut partitions: Vec<TopicPartition> = Vec::new();
        let mut context = self.context.borrow_mut();
        if partitions_given.is_empty() {
            partitions = context.all_partitions();
        }
        match self.zk_client.get_topic_partition_states(partitions) {
            Ok(leader_isr_and_epochs) => {
                for (partition, leader_isr) in leader_isr_and_epochs {
                    context.put_partition_leadership_info(partition, leader_isr.clone());
                }
            }
            Err(_) => {}
        }
    }

    fn send_update_metadata_request(
        &self,
        _brokers: Vec<u32>,
        _partitions: HashSet<TopicPartition>,
    ) {
        // do this later, may need a discussion on grpc interface
        // todo!();
    }

    fn process_update_notifications(&self, _partitions: Vec<TopicPartition>) {
        // just send update metadata request
        // todo!();
    }

    fn process_topic_ids(&self, _topic_id_assignment: Vec<TopicIdReplicaAssignment>) {
        // Add topic IDs to controller context, Do we really need topic ids tho?
        // do nothing since we assume no topic id
    }

    fn register_broker_modification_handler(&self, broker_ids: HashSet<u32>) {
        let _ = broker_ids
            .iter()
            .map(|&id| {
                let handler = BrokerModificationHandler {
                    event_manager: self.em.clone(),
                    broker_id: id,
                };
                let _ = self
                    .zk_client
                    .register_znode_change_handler_and_check_existence(Arc::new(handler));
                self.broker_modification_handlers.write().unwrap().insert(
                    id,
                    BrokerModificationHandler {
                        event_manager: self.em.clone(),
                        broker_id: id,
                    },
                );
            })
            .collect::<Vec<()>>();
    }

    fn unregister_broker_modification_handler(&self, brokers_id: Vec<u32>) {
        let mut broker_modification_handlers = self.broker_modification_handlers.write().unwrap();

        for id in brokers_id {
            let handler = broker_modification_handlers.get(&id).unwrap();
            self.zk_client
                .unregister_znode_change_handler(handler.path().as_str());
            broker_modification_handlers.remove(&id);
        }
    }

    fn maybe_resign(&self) {
        let was_active_before_change = self.is_active();

        let _ = self
            .zk_client
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
        self.zk_client.unregister_znode_child_change_handler(
            IsrChangeNotificationZNode::path("".to_string()).as_str(),
        );
        let guard = self.broker_modification_handlers.read().unwrap();
        let broker_ids: Vec<u32> = guard.keys().cloned().collect();
        std::mem::drop(guard);
        self.unregister_broker_modification_handler(broker_ids);

        self.partition_state_machine.shutdown();

        self.zk_client.unregister_znode_child_change_handler(
            TopicChangeHandler {
                event_manager: self.em.clone(),
            }
            .path()
            .as_str(),
        );

        self.replica_state_machine.shutdown();
        self.zk_client.unregister_znode_child_change_handler(
            BrokerChangeHandler {
                event_manager: self.em.clone(),
            }
            .path()
            .as_str(),
        );

        self.channel_manager.borrow_mut().shutdown();

        self.context.borrow_mut().reset_context();
    }

    fn on_new_partition_creation(&self, new_partitions: HashSet<TopicPartition>) {
        let partitions: Vec<TopicPartition> = new_partitions.iter().cloned().collect();

        let context = self.context.borrow();
        let replicas = context.replicas_for_partition(partitions);
        std::mem::drop(context);

        self.partition_state_machine
            .handle_state_change(new_partitions.clone(), Arc::new(NewPartition {}));
        self.replica_state_machine
            .handle_state_change(replicas.clone(), Arc::new(NewReplica {}));
        self.partition_state_machine
            .handle_state_change(new_partitions, Arc::new(OnlinePartition {}));

        self.replica_state_machine
            .handle_state_change(replicas, Arc::new(OnlineReplica {}));
    }

    fn on_broker_startup(&self, broker: Vec<u32>) {
        let context = self.context.borrow();

        let broker_set: HashSet<u32> = broker.into_iter().collect();
        let existing_brokers: HashSet<u32> = context
            .live_or_shutting_down_broker_ids()
            .difference(&broker_set)
            .cloned()
            .collect();

        self.send_update_metadata_request(existing_brokers.into_iter().collect(), HashSet::new());
        self.send_update_metadata_request(
            broker_set.iter().cloned().collect(),
            context.partitions_with_leaders(),
        );

        let all_replias_on_new_brokers = context.replicas_on_brokers(broker_set.clone());

        self.replica_state_machine
            .handle_state_change(all_replias_on_new_brokers, Arc::new(OnlineReplica {}));

        self.partition_state_machine
            .trigger_online_partition_state_change();
        self.register_broker_modification_handler(broker_set);
    }

    fn on_broker_failure(&self, brokers: Vec<u32>) {
        let replicas = self
            .context
            .borrow()
            .replicas_on_brokers(brokers.iter().cloned().collect());
        self.on_replicas_become_offline(replicas);

        self.unregister_broker_modification_handler(brokers.into_iter().collect());
    }

    fn on_replicas_become_offline(&self, replicas: HashSet<(TopicPartition, u32)>) {
        let partitions_with_offline_leader =
            self.context.borrow_mut().partitions_with_offline_leader();
        self.partition_state_machine.handle_state_change(
            partitions_with_offline_leader,
            Arc::new(OfflinePartition {}),
        );

        self.partition_state_machine
            .trigger_online_partition_state_change();
        self.replica_state_machine
            .handle_state_change(replicas, Arc::new(OfflineReplica {}));
    }

    fn on_replica_election(&self, partitions: HashSet<TopicPartition>) {
        self.partition_state_machine
            .handle_state_change(partitions, Arc::new(OnlinePartition {}));
    }
}

// activation function for controller
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
