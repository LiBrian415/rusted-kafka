use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use zookeeper::recipes::leader;

use crate::{
    common::topic_partition::{LeaderAndIsr, PartitionReplica, ReplicaAssignment, TopicPartition},
    zk::zk_client::KafkaZkClient,
};

use super::{
    channel_manager::{ControllerBrokerRequestBatch, ControllerChannelManager},
    controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

const NEW_REPLICA: u32 = 0;
const ONLINE_REPLICA: u32 = 1;
const OFFLINE_REPLICA: u32 = 2;
const NON_EXISTENT_REPLICA: u32 = 3;

pub struct ReplicaStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zk_client: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
    request_batch: RefCell<ControllerBrokerRequestBatch>,
}

impl ReplicaStateMachine {
    pub fn init(
        broker_id: u32,
        context: Rc<RefCell<ControllerContext>>,
        zk_client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> ReplicaStateMachine {
        let request_batch = RefCell::new(ControllerBrokerRequestBatch::init(
            channel_manager.clone(),
            broker_id,
            event_manager.clone(),
        ));
        Self {
            broker_id,
            context,
            zk_client,
            channel_manager,
            event_manager,
            request_batch,
        }
    }

    pub fn startup(&self) {
        self.initialize_replica_state();
        let (online, offline) = self.context.borrow().online_and_offline_replicas();
        self.handle_state_change(online, Arc::new(OnlineReplica {}));
        self.handle_state_change(offline, Arc::new(OfflineReplica {}));
    }

    pub fn shutdown(&self) {
        println!("shutdown replica state machine");
    }

    fn initialize_replica_state(&self) {
        let mut context = self.context.borrow_mut();

        for partition in context.all_partitions().iter() {
            let replicas = context.partition_replica_assignment(partition.clone());
            for rid in replicas {
                if context.is_replica_online(rid, partition.clone()) {
                    context.put_replica_state(
                        PartitionReplica::init(partition.clone(), rid),
                        Arc::new(OnlineReplica {}),
                    );
                } else {
                    // TODO: maybe a different state
                }
            }
        }
    }

    pub fn handle_state_change(
        &self,
        replicas: HashSet<PartitionReplica>,
        target_state: Arc<dyn ReplicaState>,
    ) {
        if replicas.is_empty() {
            return;
        }

        let mut batch = self.request_batch.borrow_mut();
        batch.new_batch();
        std::mem::drop(batch);

        let mut rid_to_replicas: HashMap<u32, HashSet<PartitionReplica>> = HashMap::new();
        for replica in replicas.iter() {
            match rid_to_replicas.get_mut(&replica.replica) {
                Some(r) => {
                    r.insert(replica.clone());
                }
                None => {
                    rid_to_replicas
                        .insert(replica.replica.clone(), HashSet::from([replica.clone()]));
                }
            }
        }

        for (rid, grouped_replicas) in rid_to_replicas {
            // println!("{}, {:?}", rid, grouped_replicas);
            self.do_handle_state_change(rid, grouped_replicas, target_state.clone());
        }

        let batch = self.request_batch.borrow();
        let context = self.context.borrow();
        batch.send_request_to_brokers(context.epoch);
    }

    fn do_handle_state_change(
        &self,
        rid: u32,
        replicas: HashSet<PartitionReplica>,
        target_state: Arc<dyn ReplicaState>,
    ) {
        let mut context = self.context.borrow_mut();
        let _: Vec<()> = replicas
            .iter()
            .map(|replica| {
                context.put_replica_state_if_not_exists(
                    replica.clone(),
                    Arc::new(NonExistentReplica {}),
                );
            })
            .collect();
        let valid_replicas =
            context.check_valid_replica_state_change(replicas, target_state.clone());
        std::mem::drop(context);

        match target_state.state() {
            NEW_REPLICA => {
                let mut context = self.context.borrow_mut();
                let mut batch = self.request_batch.borrow_mut();
                for replica in valid_replicas.iter() {
                    match context.partition_leadership_info.get(&replica.partition) {
                        Some(leader_and_isr) => {
                            if leader_and_isr.leader != rid {
                                batch.add_leader_and_isr_request_for_brokers(
                                    vec![rid],
                                    replica.partition.clone(),
                                    leader_and_isr.clone(),
                                );
                                context.put_replica_state(replica.clone(), Arc::new(NewReplica {}));
                            }
                        }
                        None => {
                            context.put_replica_state(replica.clone(), Arc::new(NewReplica {}));
                        }
                    }
                }
            }
            ONLINE_REPLICA => {
                let mut context = self.context.borrow_mut();
                for replica in valid_replicas.iter() {
                    let curr_state = context.replica_states.get(replica).unwrap();

                    match curr_state.state() {
                        NEW_REPLICA => {
                            let assignment =
                                context.partition_replica_assignment(replica.partition.clone());

                            let mut replica_assignment = context
                                .partition_assignments
                                .get(&replica.partition.topic)
                                .unwrap()
                                .clone();
                            replica_assignment
                                .partitions
                                .get_mut(&replica.partition)
                                .unwrap()
                                .push(rid);
                            if !assignment.contains(&rid) {
                                context.update_partition_full_replica_assignment(
                                    replica.partition.clone(),
                                    replica_assignment,
                                );
                            }
                        }
                        _ => {
                            let mut batch = self.request_batch.borrow_mut();
                            match context.partition_leadership_info.get(&replica.partition) {
                                Some(leader_and_isr) => {
                                    batch.add_leader_and_isr_request_for_brokers(
                                        vec![rid],
                                        replica.partition.clone(),
                                        leader_and_isr.clone(),
                                    );
                                }
                                None => {}
                            }
                        }
                    }
                    context.put_replica_state(replica.clone(), Arc::new(OnlineReplica {}));
                }
            }
            OFFLINE_REPLICA => {
                // Send StopReplicaRequest
                let mut context = self.context.borrow_mut();
                let mut batch = self.request_batch.borrow_mut();
                let partitions = valid_replicas
                    .iter()
                    .map(|replica| replica.partition.clone())
                    .collect();
                let updated_leader_and_isr =
                    self.remove_replica_from_isr_of_partitions(rid, partitions);
                for (partition, leader_and_isr) in updated_leader_and_isr {
                    let mut recipients = context.partition_replica_assignment(partition.clone());
                    recipients.retain(|id| *id != rid);
                    batch.add_leader_and_isr_request_for_brokers(
                        recipients,
                        partition.clone(),
                        leader_and_isr.clone(),
                    );
                    context.put_replica_state(
                        PartitionReplica::init(partition, rid),
                        Arc::new(OfflineReplica {}),
                    );
                }
            }
            _ => {}
        }
    }

    fn remove_replica_from_isr_of_partitions(
        &self,
        replica_id: u32,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        let mut results = HashMap::new();

        let finished = self.do_remove_replica_from_isr(replica_id, partitions);
        results.extend(finished);
        results
    }

    fn do_remove_replica_from_isr(
        &self,
        rid: u32,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        let leader_and_isrs = self.get_topic_partition_states(partitions);
        let mut leader_and_isrs_with_replica = leader_and_isrs.clone();
        let mut leader_and_isrs_without_replica = leader_and_isrs.clone();
        leader_and_isrs_with_replica.retain(|_, leader_and_isr| leader_and_isr.isr.contains(&rid));
        for (partition, _) in leader_and_isrs_with_replica.iter() {
            leader_and_isrs_without_replica.remove(partition);
        }

        for leader_and_isr in leader_and_isrs_with_replica.values_mut() {
            if rid == leader_and_isr.leader {
                leader_and_isr.leader = 0; // NO LEADER
            }
            if leader_and_isr.isr.len() != 1 {
                leader_and_isr.isr.retain(|id| *id != rid);
            }
        }

        let mut context = self.context.borrow_mut();
        let _ = self.zk_client.set_leader_and_isr(
            leader_and_isrs_with_replica.clone(),
            context.epoch_zk_version,
        );

        leader_and_isrs_with_replica.extend(leader_and_isrs_without_replica);
        for (partition, leader_and_isr) in leader_and_isrs_with_replica.iter() {
            context.put_partition_leadership_info(partition.clone(), leader_and_isr.clone());
        }

        leader_and_isrs_with_replica
    }

    fn get_topic_partition_states(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        match self.zk_client.get_topic_partition_states(partitions) {
            Ok(states) => states,
            Err(_) => HashMap::new(),
        }
    }
}

pub trait ReplicaState {
    fn state(&self) -> u32;
    fn valid_previous_state(&self) -> Vec<u32>;
}

pub struct NewReplica {}
impl ReplicaState for NewReplica {
    fn state(&self) -> u32 {
        NEW_REPLICA
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NON_EXISTENT_REPLICA]
    }
}

pub struct OnlineReplica {}
impl ReplicaState for OnlineReplica {
    fn state(&self) -> u32 {
        ONLINE_REPLICA
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NEW_REPLICA, ONLINE_REPLICA, OFFLINE_REPLICA]
    }
}

pub struct OfflineReplica {}
impl ReplicaState for OfflineReplica {
    fn state(&self) -> u32 {
        OFFLINE_REPLICA
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NEW_REPLICA, ONLINE_REPLICA, OFFLINE_REPLICA]
    }
}

pub struct NonExistentReplica {}
impl ReplicaState for NonExistentReplica {
    fn state(&self) -> u32 {
        NON_EXISTENT_REPLICA
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        Vec::new()
    }
}
