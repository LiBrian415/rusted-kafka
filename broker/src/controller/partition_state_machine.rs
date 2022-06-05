use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use crate::{
    common::topic_partition::{LeaderAndIsr, TopicPartition},
    zk::zk_client::KafkaZkClient,
};

use super::{
    channel_manager::{ControllerBrokerRequestBatch, ControllerChannelManager},
    constants::INITIAL_PARTITION_EPOCH,
    controller_context::ControllerContext,
    event_manager::ControllerEventManager,
};

const NEW_PARTITION: u32 = 0;
const ONLINE_PARTITION: u32 = 1;
const OFFLINE_PARTITION: u32 = 2;
const NON_EXISTENT_PARTITION: u32 = 4;

pub struct PartitionStateMachine {
    broker_id: u32,
    context: Rc<RefCell<ControllerContext>>,
    zk_client: Arc<KafkaZkClient>,
    channel_manager: Rc<RefCell<ControllerChannelManager>>,
    event_manager: ControllerEventManager,
    request_batch: RefCell<ControllerBrokerRequestBatch>,
}

impl PartitionStateMachine {
    pub fn init(
        broker_id: u32,
        context: Rc<RefCell<ControllerContext>>,
        zk_client: Arc<KafkaZkClient>,
        channel_manager: Rc<RefCell<ControllerChannelManager>>,
        event_manager: ControllerEventManager,
    ) -> PartitionStateMachine {
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
        self.initialize_partition_state();
        self.trigger_online_partition_state_change();
    }

    fn initialize_partition_state(&self) {
        let mut context = self.context.borrow_mut();
        for partition in context.all_partitions().iter() {
            match context.partition_leadership_info.get(partition) {
                Some(leader_and_isr) => {
                    if context.is_replica_online(leader_and_isr.leader, partition.clone()) {
                        context
                            .put_partition_state(partition.clone(), Arc::new(OnlinePartition {}));
                    } else {
                        context
                            .put_partition_state(partition.clone(), Arc::new(OfflinePartition {}));
                    }
                }
                None => {
                    context.put_partition_state(partition.clone(), Arc::new(NewPartition {}));
                }
            }
        }
    }

    pub fn shutdown(&self) {
        println!("shutdown partition state machine!");
    }

    pub fn trigger_online_partition_state_change(&self) {
        let context = self.context.borrow();
        let partitions = context.get_partitions(vec![OFFLINE_PARTITION, NEW_PARTITION]);
        std::mem::drop(context);

        self.handle_state_change(partitions, Arc::new(OnlinePartition {}));
    }

    pub fn handle_state_change(
        &self,
        partitions: HashSet<TopicPartition>,
        target_state: Arc<dyn PartitionState>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        if partitions.is_empty() {
            return HashMap::new();
        }
        let mut batch = self.request_batch.borrow_mut();
        batch.new_batch();
        std::mem::drop(batch);
        let result = self.do_handle_state_change(partitions, target_state);

        let batch = self.request_batch.borrow();
        let context = self.context.borrow();
        batch.send_request_to_brokers(context.epoch);

        result
    }

    fn do_handle_state_change(
        &self,
        partitions: HashSet<TopicPartition>,
        target_state: Arc<dyn PartitionState>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        let mut context = self.context.borrow_mut();
        let _: Vec<()> = partitions
            .iter()
            .map(|partition| {
                context.put_partition_state_if_not_exists(
                    partition.clone(),
                    Arc::new(NonExistentPartition {}),
                );
            })
            .collect();
        let valid_partitions =
            context.check_valid_partition_state_change(partitions.clone(), target_state.clone());

        println!(
            "{:?}, valid: {:?}: target = {}",
            partitions,
            valid_partitions,
            target_state.state()
        );

        std::mem::drop(context);

        match target_state.state() {
            NEW_PARTITION | OFFLINE_PARTITION | NON_EXISTENT_PARTITION => {
                let mut context = self.context.borrow_mut();
                let _: Vec<()> = valid_partitions
                    .iter()
                    .map(|partition| {
                        context.put_partition_state(partition.clone(), target_state.clone());
                    })
                    .collect();
                println!(
                    "set {:?} to state {}",
                    valid_partitions,
                    target_state.state()
                );
                HashMap::new()
            }
            ONLINE_PARTITION => {
                let context = self.context.borrow_mut();
                let mut uninitial_partitions = valid_partitions.clone();
                uninitial_partitions.retain(|partition| {
                    context.partition_states.get(&partition).unwrap().state() == NEW_PARTITION
                });
                let mut to_elect_partitions = valid_partitions.clone();
                to_elect_partitions.retain(|partition| {
                    context.partition_states.get(&partition).unwrap().state() == OFFLINE_PARTITION
                        || context.partition_states.get(&partition).unwrap().state()
                            == ONLINE_PARTITION
                });

                println!(
                    "to-initial: {:?}, to-elect: {:?}",
                    uninitial_partitions, to_elect_partitions
                );
                std::mem::drop(context);
                if !uninitial_partitions.is_empty() {
                    let initialized_partitions = self.initialize_partitions(uninitial_partitions);
                    let mut context = self.context.borrow_mut();
                    for partition in initialized_partitions {
                        context.put_partition_state(partition, target_state.clone())
                    }
                }

                if !to_elect_partitions.is_empty() {
                    let partitions_with_leader =
                        self.elect_leader_for_partitions(to_elect_partitions);
                    let mut context = self.context.borrow_mut();
                    for (partition, _) in partitions_with_leader.iter() {
                        context.put_partition_state(partition.clone(), target_state.clone());
                    }

                    partitions_with_leader
                } else {
                    HashMap::new()
                }
            }
            _ => HashMap::new(),
        }
    }

    /// initialize leaderAndIsr for partitions
    fn initialize_partitions(&self, partitions: Vec<TopicPartition>) -> Vec<TopicPartition> {
        let mut context = self.context.borrow_mut();
        let mut succ_init_partitions = Vec::new();
        let mut replicas_per_partition: HashMap<TopicPartition, Vec<u32>> = HashMap::new();
        for partition in partitions.iter() {
            replicas_per_partition.insert(
                partition.clone(),
                context.partition_replica_assignment(partition.clone()),
            );
        }

        for (partition, replicas) in replicas_per_partition.iter() {
            println!("partition: {:?}, replcias: {:?}", partition, replicas);
        }

        let mut partitions_with_live_replicas: HashMap<TopicPartition, Vec<u32>> = HashMap::new();
        for (partition, replicas) in replicas_per_partition.iter() {
            let mut live_replicas = Vec::new();
            for replica in replicas {
                if context.is_replica_online(replica.clone(), partition.clone()) {
                    live_replicas.push(replica.clone());
                }
            }
            if live_replicas.is_empty() {
                continue;
            }
            partitions_with_live_replicas.insert(partition.clone(), live_replicas);
        }

        for (partition, replicas) in partitions_with_live_replicas {
            let leader_and_isr = LeaderAndIsr::init(
                replicas[0].clone(),
                replicas.clone(),
                context.epoch,
                INITIAL_PARTITION_EPOCH,
            );
            println!("live_replicas: {:?}, {:?}", partition, replicas);
            match self.zk_client.create_topic_partition_state(HashMap::from([(
                partition.clone(),
                leader_and_isr.clone(),
            )]))[0]
            {
                Ok(_) => {
                    context
                        .put_partition_leadership_info(partition.clone(), leader_and_isr.clone());
                    let mut batch = self.request_batch.borrow_mut();
                    batch.add_leader_and_isr_request_for_brokers(
                        leader_and_isr.isr.clone(),
                        partition.clone(),
                        leader_and_isr,
                    );
                    succ_init_partitions.push(partition.clone());
                }
                Err(_) => {}
            }
        }

        succ_init_partitions
    }

    fn elect_leader_for_partitions(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        let mut finished_elections = HashMap::new();

        let finished = self.do_leader_election_for_partitions(partitions);
        finished_elections.extend(finished);

        finished_elections
    }

    fn do_leader_election_for_partitions(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, LeaderAndIsr> {
        let mut valid_leader_isrs = HashMap::new();
        let context = self.context.borrow();
        let partition_states = match self.zk_client.get_topic_partition_states(partitions) {
            Ok(states) => states,
            Err(_) => return HashMap::new(),
        };

        for (partition, leader_and_isr) in partition_states {
            if !(leader_and_isr.controller_epoch > context.epoch) {
                valid_leader_isrs.insert(partition, leader_and_isr);
            }
        }

        if valid_leader_isrs.is_empty() {
            return HashMap::new();
        }
        std::mem::drop(context);

        let partitions_after_election = self.elect_leader_for_offline(valid_leader_isrs.clone());

        let mut recipients_per_partition = HashMap::new();
        let mut new_leader_and_isrs = HashMap::new();
        for (partition, (lsr, replicas)) in partitions_after_election.iter() {
            recipients_per_partition.insert(partition.clone(), replicas.clone());
            new_leader_and_isrs.insert(partition.clone(), lsr.clone());
        }

        let mut context = self.context.borrow_mut();
        match self
            .zk_client
            .set_leader_and_isr(new_leader_and_isrs.clone(), context.epoch_zk_version)
        {
            Ok(_) => {
                for (partition, leader_and_isr) in new_leader_and_isrs.iter() {
                    context
                        .put_partition_leadership_info(partition.clone(), leader_and_isr.clone());
                    let mut batch = self.request_batch.borrow_mut();
                    batch.add_leader_and_isr_request_for_brokers(
                        recipients_per_partition.get(&partition).unwrap().to_vec(),
                        partition.clone(),
                        leader_and_isr.clone(),
                    );
                }
            }
            Err(_) => {
                return HashMap::new();
            }
        }

        new_leader_and_isrs
    }

    fn elect_leader_for_offline(
        &self,
        partitions_with_lsr: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> HashMap<TopicPartition, (LeaderAndIsr, Vec<u32>)> {
        let mut results: HashMap<TopicPartition, (LeaderAndIsr, Vec<u32>)> = HashMap::new();
        let context = self.context.borrow();
        for (partition, leader_and_isr) in partitions_with_lsr.iter() {
            let assignment = context.partition_replica_assignment(partition.clone());
            let mut live_replicas = assignment.clone();
            live_replicas.retain(|id| context.is_replica_online(*id, partition.clone()));

            let new_leader = self.do_offline_leader_election(
                assignment,
                leader_and_isr.isr.clone(),
                live_replicas.clone(),
            );
            let mut new_isr = leader_and_isr.isr.clone();
            if leader_and_isr.isr.contains(&new_leader) {
                new_isr.retain(|id| context.is_replica_online(*id, partition.clone()));
            } else {
                new_isr.clear();
                new_isr.push(new_leader.clone());
            }
            println!("new leader is {}, new isr is {:?}", new_leader, new_isr);
            results.insert(
                partition.clone(),
                (
                    LeaderAndIsr::init(
                        new_leader,
                        new_isr,
                        leader_and_isr.controller_epoch, // TODO: should we update the epoch and how
                        leader_and_isr.leader_epoch,
                    ),
                    live_replicas,
                ),
            );
        }

        results
    }

    fn do_offline_leader_election(
        &self,
        assignment: Vec<u32>,
        isr: Vec<u32>,
        live_replicas: Vec<u32>,
    ) -> u32 {
        let mut new_leader = 0; // default none value
        for id in assignment {
            if live_replicas.contains(&id) && isr.contains(&id) {
                new_leader = id
            }
        }

        new_leader
    }
}

pub trait PartitionState {
    fn state(&self) -> u32;
    fn valid_previous_state(&self) -> Vec<u32>;
}

pub struct NewPartition {}
impl PartitionState for NewPartition {
    fn state(&self) -> u32 {
        NEW_PARTITION
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NON_EXISTENT_PARTITION]
    }
}

pub struct OnlinePartition {}
impl PartitionState for OnlinePartition {
    fn state(&self) -> u32 {
        ONLINE_PARTITION
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NEW_PARTITION, ONLINE_PARTITION, OFFLINE_PARTITION]
    }
}

pub struct OfflinePartition {}
impl PartitionState for OfflinePartition {
    fn state(&self) -> u32 {
        OFFLINE_PARTITION
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![NEW_PARTITION, ONLINE_PARTITION, OFFLINE_PARTITION]
    }
}

pub struct NonExistentPartition {}
impl PartitionState for NonExistentPartition {
    fn state(&self) -> u32 {
        NON_EXISTENT_PARTITION
    }

    fn valid_previous_state(&self) -> Vec<u32> {
        vec![OFFLINE_PARTITION]
    }
}
