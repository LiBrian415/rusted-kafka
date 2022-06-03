use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use crate::{
    common::topic_partition::{LeaderAndIsr, PartitionReplica, ReplicaAssignment, TopicPartition},
    zk::zk_client::KafkaZkClient,
};

use super::{
    channel_manager::ControllerChannelManager, controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

const NEW_REPLICA: u32 = 0;
const ONLINE_REPLICA: u32 = 1;
const OFFLINE_REPLICA: u32 = 2;

pub struct ReplicaStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zk_client: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
}

impl ReplicaStateMachine {
    pub fn init(
        broker_id: u32,
        context: Rc<RefCell<ControllerContext>>,
        zk_client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> ReplicaStateMachine {
        Self {
            broker_id,
            context,
            zk_client,
            channel_manager,
            event_manager,
        }
    }

    pub fn startup(&self) {
        self.initialize_replica_state();
        // let (online, offline) = self.context.borrow().online_and_offline_replicas();
        // self.handle_state_change(online, Arc::new(OnlineReplica {}));
        // self.handle_state_change(offline, Arc::new(OfflineReplica {}));
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
                }
            }
        }
    }

    pub fn handle_state_change(
        &self,
        replicas: HashSet<(TopicPartition, u32)>,
        target_state: Arc<dyn ReplicaState>,
    ) {
        if !replicas.is_empty() {
            return;
        }

        self.do_handle_state_change(replicas, target_state);
        // TODO: send request
    }

    fn do_handle_state_change(
        &self,
        replicas: HashSet<(TopicPartition, u32)>,
        target_state: Arc<dyn ReplicaState>,
    ) {
        // let mut context = self.context.borrow_mut();
        // let valid_replicas = context.check_valid_replica_state_change(replicas, target_state);

        // match target_state.state() {
        //     NEW_REPLICA => {
        //         for (partition, replica) in valid_replicas.iter() {
        //             let curr_state = context.replica_states.get(&partition);

        //             match context.partition_leadership_info.get(&partition) {
        //                 Some(leader_and_isr) => {
        //                     // TODO:Add leaderAndIsr requests
        //                     context.put_replica_state(replica, NewReplica);
        //                 }
        //                 None => {
        //                     context.put_replica_state(replica, NewReplica);
        //                 }
        //             }
        //         }
        //     }
        //     ONLINE_REPLICA => {
        //         for (partition, replica) in valid_replicas.iter() {
        //             let curr_state = context.replica_states.get(&partition);

        //             match curr_state.state() {
        //                 NEW_REPLICA => {
        //                     let assignment = match context.partition_assignments.get(&partition) {
        //                         Some(result) => result.clone(),
        //                         None => ReplicaAssignment::init(
        //                             HashMap::new(),
        //                             HashMap::new(),
        //                             HashMap::new(),
        //                         ),
        //                     };

        //                     if !assignment
        //                         .partitions
        //                         .get(partition)
        //                         .unwrap()
        //                         .contains(replica_id)
        //                     {
        //                         context.update_partition_full_replica_assignment(
        //                             partition.clone(),
        //                             assignment.clone(),
        //                         );
        //                     }
        //                 }
        //                 _ => {
        //                     match context.partition_leadership_info.get(partition) {
        //                         Some(leader_and_isr) => {
        //                             // TODO: add request
        //                         }
        //                         None => {}
        //                     }
        //                 }
        //             }
        //             context.put_replica_state(replica, OnlineReplica);
        //         }
        //     }
        //     OFFLINE_REPLICA => {
        //         // Send StopReplicaRequest
        //         let updated_leader_and_isr = self.remove_replica_from_isr(replica_id);
        //         for (partition, leader_and_isr) in updated_leader_and_isr {
        //             context.put_replica_state(replica, OfflineReplica);
        //         }
        //     }
        //     _ => {}
        // }
        todo!();
    }

    fn remove_replica_from_isr(
        &self,
        replica_id: u32,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        // let results = HashMap::new();

        // let finished = self.do_remove_replicas_from_isr(replica_id, partitions);
        // results.extend(finished);
        // results
        todo!();
    }

    fn do_remove_replica_from_isr(
        &self,
        replica_id: u32,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        todo!();
    }
}

pub trait ReplicaState {
    fn state(&self) -> u32;
}

pub struct NewReplica {}
impl ReplicaState for NewReplica {
    fn state(&self) -> u32 {
        NEW_REPLICA
    }
}

pub struct OnlineReplica {}
impl ReplicaState for OnlineReplica {
    fn state(&self) -> u32 {
        ONLINE_REPLICA
    }
}

pub struct OfflineReplica {}
impl ReplicaState for OfflineReplica {
    fn state(&self) -> u32 {
        OFFLINE_REPLICA
    }
}
