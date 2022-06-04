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
        batch.add_leader_and_isr_request_for_brokers(
            vec![0],
            partitions
                .get(&TopicPartition::init("greeting", 0))
                .unwrap()
                .clone(),
            LeaderAndIsr::init(0, vec![0], 0, 0),
        );
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

                println!("{:?}, {:?}", uninitial_partitions, to_elect_partitions);
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
        let mut context = self.context.borrow_mut();
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
        match self
            .zk_client
            .set_leader_and_isr(valid_leader_isrs.clone(), context.epoch_zk_version)
        {
            Ok(_) => {
                for (partition, leader_and_isr) in valid_leader_isrs.iter() {
                    context.put_partition_leadership_info(partition.clone(), leader_and_isr.clone())
                    // TODO: add sendLeaderAndIsr requests
                }
            }
            Err(_) => {
                return HashMap::new();
            }
        }

        valid_leader_isrs
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
