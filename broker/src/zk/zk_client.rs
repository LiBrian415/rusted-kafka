use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};
use zookeeper::{Acl, CreateMode, Stat, ZkError, ZkResult, ZkState, ZooKeeper};

use super::zk_data::{
    IsrChangeNotificationSequenceZNode, IsrChangeNotificationZNode, PersistentZkPaths,
    TopicPartitionZNode,
};
use super::{
    zk_data::{
        BrokerIdZNode, BrokerIdsZNode, ControllerEpochZNode, ControllerZNode,
        TopicPartitionOffsetZNode, TopicPartitionStateZNode, TopicPartitionsZNode, TopicZNode,
        TopicsZNode,
    },
    zk_helper::get_parent,
    zk_watcher::{KafkaZkHandlers, KafkaZkWatcher, ZkChangeHandler, ZkChildChangeHandler},
};
use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicIdReplicaAssignment, TopicPartition},
};

use crate::controller::constants::{INITIAL_CONTROLLER_EPOCH, INITIAL_CONTROLLER_EPOCH_ZK_VERSION};
pub struct KafkaZkClient {
    pub client: ZooKeeper,
    pub handlers: KafkaZkHandlers,
}

const GET_REQUEST: u32 = 0;
const GET_CHILDREN_REQUEST: u32 = 1;
const EXIST_REQUEST: u32 = 2;

impl KafkaZkClient {
    pub fn init(conn_str: &str, sess_timeout: Duration) -> ZkResult<KafkaZkClient> {
        let handlers = KafkaZkHandlers {
            change_handlers: Arc::new(RwLock::new(HashMap::new())),
            child_change_handlers: Arc::new(RwLock::new(HashMap::new())),
        };

        match ZooKeeper::connect(
            conn_str,
            sess_timeout,
            KafkaZkWatcher::init(handlers.clone()),
        ) {
            Ok(client) => Ok(KafkaZkClient {
                client: client,
                handlers: handlers.clone(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Ensures that the follow paths exist:
    ///  - BrokersIdZNode
    ///  - TopicsZNode
    pub fn create_top_level_paths(&self) {
        let persistent_path = PersistentZkPaths::init();
        let _: Vec<ZkResult<()>> = persistent_path
            .paths
            .iter()
            .map(|path| self.check_persistent_path(path))
            .collect();
    }

    fn check_persistent_path(&self, path: &str) -> ZkResult<()> {
        self.create_recursive(path, Vec::new())
    }

    fn create_recursive(&self, path: &str, data: Vec<u8>) -> ZkResult<()> {
        let res = self.client.create(
            path,
            data.clone(),
            Acl::open_unsafe().clone(),
            zookeeper::CreateMode::Persistent,
        );

        match res {
            Ok(_) => Ok(()),
            Err(e1) => match e1 {
                ZkError::NoNode => match self.recursive_create(get_parent(path).as_str()) {
                    Ok(_) => match self.client.create(
                        path,
                        data,
                        Acl::open_unsafe().clone(),
                        zookeeper::CreateMode::Persistent,
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    Err(e2) => match e2 {
                        ZkError::NodeExists => Ok(()),
                        _ => Err(e2),
                    },
                },
                ZkError::NodeExists => Ok(()),
                _ => Err(e1),
            },
        }
    }

    fn recursive_create(&self, path: &str) -> ZkResult<()> {
        if path == "" {
            return Ok(());
        }

        match self.client.create(
            path,
            Vec::new(),
            Acl::open_unsafe().clone(),
            zookeeper::CreateMode::Persistent,
        ) {
            Ok(_) => Ok(()),
            Err(e1) => match e1 {
                ZkError::NoNode => match self.recursive_create(get_parent(path).as_str()) {
                    Ok(_) => match self.client.create(
                        path,
                        Vec::new(),
                        Acl::open_unsafe().clone(),
                        zookeeper::CreateMode::Persistent,
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    Err(e2) => match e2 {
                        ZkError::NodeExists => Ok(()),
                        _ => Err(e2),
                    },
                },
                _ => Err(e1),
            },
        }
    }

    // Controller

    pub fn register_controller_and_increment_controller_epoch(
        &self,
        controller_id: u32,
    ) -> ZkResult<(u128, i32)> {
        // read /controller_epoch to get the current controller epoch and zkVersion
        // create /controller_epoch if not exists
        let result = match self.get_controller_epoch() {
            Ok(resp) => resp,
            Err(e) => return Err(e),
        };

        let (curr_epoch, curr_epoch_zk_version) = match result {
            Some(info) => (info.0, info.1.version),
            None => match self.create_controller_epoch_znode() {
                Ok(r) => r,
                Err(e) => return Err(e),
            },
        };

        // try create /controller and update /controller_epoch atomatically
        let new_controller_epoch = curr_epoch + 1;
        let expected_controller_epoch_zk_version = curr_epoch_zk_version;
        let mut correct_controller = false;
        let mut correct_epoch = false;

        match self.client.create(
            ControllerZNode::path().as_str(),
            ControllerZNode::encode(controller_id),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(_) => match self.client.set_data(
                ControllerEpochZNode::path().as_str(),
                ControllerEpochZNode::encode(new_controller_epoch),
                Some(expected_controller_epoch_zk_version),
            ) {
                Ok(resp) => return Ok((new_controller_epoch, resp.version)),
                Err(e) => match e {
                    ZkError::BadVersion => match self.check_epoch(new_controller_epoch) {
                        Ok(result) => {
                            correct_epoch = result;
                        }
                        Err(e) => return Err(e),
                    },
                    _ => return Err(e),
                },
            },
            Err(e) => match e {
                ZkError::NodeExists => match self.check_controller(controller_id) {
                    Ok(result) => {
                        correct_controller = result;
                    }
                    Err(e) => return Err(e),
                },
                _ => return Err(e),
            },
        };

        if correct_controller && correct_epoch {
            match self.get_controller_epoch() {
                Ok(resp) => {
                    let result = resp.unwrap();
                    Ok((result.0, result.1.version))
                }
                Err(e) => Err(e),
            }
        } else {
            return Err(ZkError::SystemError);
        }
    }

    fn create_controller_epoch_znode(&self) -> ZkResult<(u128, i32)> {
        match self.client.create(
            ControllerEpochZNode::path().as_str(),
            ControllerEpochZNode::encode(INITIAL_CONTROLLER_EPOCH),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ) {
            Ok(_) => Ok((
                INITIAL_CONTROLLER_EPOCH,
                INITIAL_CONTROLLER_EPOCH_ZK_VERSION,
            )),
            Err(e) => match e {
                ZkError::NodeExists => match self.get_controller_epoch() {
                    Ok(resp) => match resp {
                        Some(info) => Ok((info.0, info.1.version)),
                        None => Err(e),
                    },
                    Err(e) => Err(e),
                },
                _ => Err(e),
            },
        }
    }

    fn get_controller_epoch(&self) -> ZkResult<Option<(u128, Stat)>> {
        let path = ControllerEpochZNode::path();
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(res) => res,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_data(path.as_str(), false),
        };

        match result {
            Ok(resp) => Ok(Some((ControllerEpochZNode::decode(&resp.0), resp.1))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => return Err(e),
            },
        }
    }

    pub fn get_controller_id(&self) -> ZkResult<Option<u32>> {
        let path = ControllerZNode::path();
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(res) => res,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_data(path.as_str(), false),
        };
        match result {
            Ok(resp) => Ok(Some(ControllerZNode::decode(&resp.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    fn check_controller(&self, controller_id: u32) -> ZkResult<bool> {
        let curr_controller_id = match self.get_controller_id() {
            Ok(resp) => match resp {
                Some(id) => id,
                None => return Err(ZkError::NoNode),
            },
            Err(e) => return Err(e),
        };

        if controller_id == curr_controller_id {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    fn check_epoch(&self, new_controller_epoch: u128) -> ZkResult<bool> {
        let controller_epoch = match self.get_controller_epoch() {
            Ok(resp) => match resp {
                Some(info) => info.0,
                None => return Err(ZkError::NoNode),
            },
            Err(e) => return Err(e),
        };

        if new_controller_epoch == controller_epoch {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    // Broker

    pub fn register_broker(&self, broker: BrokerInfo) -> ZkResult<i64> {
        match self.client.create(
            BrokerIdZNode::path(broker.id).as_str(),
            BrokerIdZNode::encode(&broker),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(_) => match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str())
            {
                Ok(stat) => Ok(stat.czxid),
                Err(e) => Err(e),
            },
            Err(e) => match e {
                ZkError::NodeExists => {
                    match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str()) {
                        Ok(stat) => Ok(stat.czxid),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(e),
            },
        }
    }

    fn get_stat_after_node_exists(&self, path: &str) -> ZkResult<Stat> {
        let watch = match self.should_watch(path.to_string(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_data_w(path, KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_data(path, false),
        };

        match result {
            Ok(resp) => Ok(resp.1),
            Err(e) => Err(e),
        }
    }

    pub fn get_broker(&self, broker_id: u32) -> ZkResult<Option<BrokerInfo>> {
        let path = BrokerIdZNode::path(broker_id);
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_data(path.as_str(), false),
        };

        match result {
            Ok(data) => Ok(Some(BrokerIdZNode::decode(&data.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    // TODO: add watcher setup
    pub fn get_all_brokers(&self) -> ZkResult<Vec<Option<BrokerInfo>>> {
        let mut broker_ids: Vec<u32> = match self.get_children(BrokerIdsZNode::path().as_str()) {
            Ok(list) => list.iter().map(|id| id.parse::<u32>().unwrap()).collect(),
            Err(e) => return Err(e),
        };
        broker_ids.sort();

        let mut brokers: Vec<Option<BrokerInfo>> = Vec::new();

        for id in broker_ids {
            match self.get_broker(id) {
                Ok(info) => brokers.push(info),
                Err(e) => return Err(e),
            }
        }

        Ok(brokers)
    }

    pub fn delete_isr_change_notifications(&self, epoch_zk_version: i32) -> ZkResult<()> {
        let path = IsrChangeNotificationZNode::path("".to_string());
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_children_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_children(path.as_str(), false),
        };

        match result {
            Ok(data) => {
                return self.delete_isr_change_notifications_with_sequence_num(
                    data.iter()
                        .map(|child| IsrChangeNotificationZNode::seq_num(child.to_string()))
                        .collect(),
                    epoch_zk_version,
                );
            }
            Err(e) => match e {
                ZkError::NoNode => {}
                _ => return Err(e),
            },
        };

        Ok(())
    }

    pub fn delete_isr_change_notifications_with_sequence_num(
        &self,
        seq_num: Vec<String>,
        version: i32,
    ) -> ZkResult<()> {
        for num in seq_num {
            let _ = self.client.delete(
                IsrChangeNotificationSequenceZNode::path(num).as_str(),
                Some(version),
            );
        }

        Ok(())
    }

    pub fn get_all_isr_change_notifications(&self) -> ZkResult<Vec<String>> {
        todo!();
    }

    pub fn get_partitions_from_isr_change_notifications(
        &self,
        seq_num: Vec<String>,
    ) -> ZkResult<Vec<TopicPartition>> {
        todo!();
    }

    pub fn get_all_broker_and_epoch(&self) -> ZkResult<HashMap<BrokerInfo, i64>> {
        let mut broker_ids: Vec<u32> = match self.get_children(BrokerIdsZNode::path().as_str()) {
            Ok(list) => list.iter().map(|id| id.parse::<u32>().unwrap()).collect(),
            Err(e) => return Err(e),
        };
        broker_ids.sort();

        let mut brokers: HashMap<BrokerInfo, i64> = HashMap::new();

        for id in broker_ids {
            match self
                .client
                .get_data(BrokerIdZNode::path(id).as_str(), false)
            {
                Ok(data) => {
                    brokers.insert(BrokerIdZNode::decode(&data.0), data.1.czxid);
                }
                Err(e) => match e {
                    ZkError::NoNode => {}
                    _ => return Err(e),
                },
            };
        }

        Ok(brokers)
    }
    // Topic + Partition

    pub fn get_all_topics(&self, register_watch: bool) -> ZkResult<HashSet<String>> {
        let path = TopicsZNode::path();
        let watch = match self.should_watch(path.to_string(), GET_CHILDREN_REQUEST, register_watch)
        {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_children_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_children(path.as_str(), false),
        };

        match result {
            Ok(resp) => Ok(HashSet::from_iter(resp.iter().cloned())),
            Err(e) => match e {
                ZkError::NoNode => Ok(HashSet::new()),
                _ => Err(e),
            },
        }
    }

    fn create_topic_partitions(&self, topics: Vec<String>) -> Vec<ZkResult<String>> {
        let resps: Vec<ZkResult<String>> = topics
            .iter()
            .map(|topic| {
                self.client.create(
                    TopicPartitionsZNode::path(topic).as_str(),
                    Vec::new(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect();
        resps
    }

    fn create_topic_partition(&self, partitions: Vec<TopicPartition>) -> Vec<ZkResult<String>> {
        let resps: Vec<ZkResult<String>> = partitions
            .iter()
            .map(|partition| {
                self.client.create(
                    TopicPartitionZNode::path(partition.topic.as_str(), partition.partition)
                        .as_str(),
                    Vec::new(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect();

        resps
    }

    pub fn create_topic_partition_state(
        &self,
        leader_isr_and_epoch: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> Vec<ZkResult<String>> {
        self.create_topic_partitions(
            leader_isr_and_epoch
                .iter()
                .map(|(partition, _)| partition.topic.clone())
                .collect(),
        );

        self.create_topic_partition(leader_isr_and_epoch.keys().cloned().collect());

        leader_isr_and_epoch
            .iter()
            .map(|(partition, leader_and_isr)| {
                self.client.create(
                    TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition)
                        .as_str(),
                    TopicPartitionStateZNode::encode(leader_and_isr.clone()),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect()
    }

    /// gets the partition numbers for the given topics
    pub fn get_partitions_for_topics(
        &self,
        topics: Vec<String>,
    ) -> ZkResult<HashMap<String, Vec<u32>>> {
        let mut partitions: HashMap<String, Vec<u32>> = HashMap::new();
        match self.get_partition_assignment_for_topics(topics) {
            Ok(partition_assignments) => {
                for (topic, assignment) in partition_assignments.iter() {
                    match assignment {
                        Some(am) => {
                            let mut partition_numbers = Vec::new();
                            for partition in am.partitions.iter() {
                                if partition.0.topic == topic.to_string() {
                                    partition_numbers.push(partition.0.partition);
                                }
                            }
                            partitions.insert(topic.to_string(), partition_numbers);
                        }
                        None => {
                            partitions.insert(topic.to_string(), Vec::new());
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }

        Ok(partitions)
    }

    fn get_partition_assignment_for_topics(
        &self,
        topics: Vec<String>,
    ) -> ZkResult<HashMap<String, Option<ReplicaAssignment>>> {
        let mut partition_assignments = HashMap::new();
        let resps: Vec<ZkResult<(Vec<u8>, Stat)>> = topics
            .iter()
            .map(|topic| {
                let path = TopicZNode::path(topic);
                let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                    Ok(resp) => resp,
                    Err(_) => false,
                };
                match watch {
                    true => self
                        .client
                        .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
                    false => self.client.get_data(path.as_str(), false),
                }
            })
            .collect();
        let mut i = 0;

        for resp in resps {
            match resp {
                Ok(data) => {
                    partition_assignments
                        .insert(topics[i].clone(), Some(TopicZNode::decode(&data.0)));
                }
                Err(e) => match e {
                    ZkError::NoNode => {
                        partition_assignments.insert(topics[i].clone(), None);
                    }
                    _ => return Err(e),
                },
            }
            i = i + 1;
        }

        Ok(partition_assignments)
    }

    pub fn get_full_replica_assignment_for_topics(
        &self,
        topics: Vec<String>,
    ) -> ZkResult<HashMap<TopicPartition, ReplicaAssignment>> {
        let mut replica_assignment: HashMap<TopicPartition, ReplicaAssignment> = HashMap::new();
        let resps: Vec<Result<(Vec<u8>, Stat), ZkError>> = topics
            .iter()
            .map(|topic| {
                let path = TopicZNode::path(topic);
                let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                    Ok(resp) => resp,
                    Err(_) => false,
                };
                match watch {
                    true => self
                        .client
                        .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
                    false => self.client.get_data(path.as_str(), false),
                }
            })
            .collect();
        let mut i = 0;

        for resp in resps {
            match resp {
                Ok(data) => {
                    replica_assignment.extend(
                        TopicZNode::decode_with_topic(topics[i].clone(), &data.0).assignment,
                    );
                }
                Err(e) => match e {
                    ZkError::NoNode => {}
                    _ => return Err(e),
                },
            }
            i = i + 1;
        }

        Ok(replica_assignment)
    }

    pub fn set_replica_assignment_for_topics(
        &self,
        topics: Vec<String>,
        replica_assignments: Vec<ReplicaAssignment>,
        version: Option<i32>,
    ) -> ZkResult<bool> {
        if topics.len() != replica_assignments.len() {
            return Err(ZkError::SystemError); // TODO: later need to change systemerror to specific error messages
        }

        for i in 0..topics.len() {
            match self.client.set_data(
                TopicZNode::path(topics[i].as_str()).as_str(),
                TopicZNode::encode(replica_assignments[i].clone()),
                version,
            ) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(true)
    }

    pub fn get_leader_and_isr(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> ZkResult<HashMap<TopicPartition, LeaderAndIsr>> {
        let mut leader_and_isr: HashMap<TopicPartition, LeaderAndIsr> = HashMap::new();

        for partition in partitions {
            let path =
                TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition);
            let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                Ok(watch) => watch,
                Err(_) => false,
            };

            let result = match watch {
                true => self
                    .client
                    .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
                false => self.client.get_data(path.as_str(), false),
            };

            match result {
                Ok(resp) => {
                    leader_and_isr.insert(partition, TopicPartitionStateZNode::decode(&resp.0));
                }
                Err(e) => return Err(e),
            }
        }

        Ok(leader_and_isr)
    }

    pub fn set_leader_and_isr(
        &self,
        leader_and_isrs: HashMap<TopicPartition, LeaderAndIsr>,
        expected_controller_epoch_zk_version: i32,
    ) -> ZkResult<bool> {
        for (partition, leader_and_isr) in leader_and_isrs.iter() {
            match self.client.set_data(
                TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition)
                    .as_str(),
                TopicPartitionStateZNode::encode(leader_and_isr.clone()),
                Some(expected_controller_epoch_zk_version),
            ) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(true)
    }

    pub fn get_topic_partition_offset(
        &self,
        topic: &str,
        partition: u32,
    ) -> ZkResult<Option<Vec<u8>>> {
        let path = TopicPartitionOffsetZNode::path(topic, partition);
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(resp) => resp,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_data(path.as_str(), false),
        };
        match result {
            Ok(resp) => Ok(Some(resp.0)),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub fn set_topic_partition_offset(
        &self,
        topic: &str,
        partition: u32,
        version: Option<i32>,
    ) -> ZkResult<bool> {
        match self.client.set_data(
            TopicPartitionOffsetZNode::path(topic, partition).as_str(),
            TopicPartitionOffsetZNode::encode(),
            version,
        ) {
            Ok(_) => Ok(true),
            Err(e) => match e {
                ZkError::BadVersion | ZkError::NoNode => Ok(false),
                _ => Err(e),
            },
        }
    }

    pub fn get_topic_partition_states(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> ZkResult<HashMap<TopicPartition, LeaderAndIsr>> {
        let mut partition_states: HashMap<TopicPartition, LeaderAndIsr> = HashMap::new();

        for partition in partitions {
            let path =
                TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition);
            let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                Ok(watch) => watch,
                Err(_) => false,
            };
            let result = match watch {
                true => self
                    .client
                    .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
                false => self.client.get_data(path.as_str(), false),
            };

            match result {
                Ok(resp) => {
                    partition_states
                        .insert(partition.clone(), TopicPartitionStateZNode::decode(&resp.0));
                }
                Err(e) => match e {
                    ZkError::NoNode => {}
                    _ => return Err(e),
                },
            };
        }

        Ok(partition_states)
    }

    pub fn get_replica_assignment_and_topic_ids_for_topics(
        &self,
        topics: HashSet<String>,
    ) -> ZkResult<Vec<TopicIdReplicaAssignment>> {
        let mut topic_replica_assignment: Vec<TopicIdReplicaAssignment> = Vec::new();

        for topic in topics {
            let path = TopicZNode::path(topic.as_str());
            let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                Ok(watch) => watch,
                Err(_) => false,
            };
            let result = match watch {
                true => self
                    .client
                    .get_data_w(path.as_str(), KafkaZkWatcher::init(self.handlers.clone())),
                false => self.client.get_data(path.as_str(), false),
            };

            match result {
                Ok(resp) => {
                    topic_replica_assignment.push(TopicZNode::decode_with_topic(topic, &resp.0));
                }
                Err(e) => match e {
                    ZkError::NoNode => {
                        topic_replica_assignment.push(TopicIdReplicaAssignment {
                            topic: topic,
                            assignment: HashMap::new(),
                        });
                    }
                    _ => return Err(e),
                },
            };
        }

        Ok(topic_replica_assignment)
    }

    // ZooKeeper

    pub fn get_children(&self, path: &str) -> ZkResult<Vec<String>> {
        let watch = match self.should_watch(path.to_string(), GET_CHILDREN_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = match watch {
            true => self
                .client
                .get_children_w(path, KafkaZkWatcher::init(self.handlers.clone())),
            false => self.client.get_children(path, false),
        };

        match result {
            Ok(resp) => Ok(resp),
            Err(e) => match e {
                ZkError::NoNode => Ok(Vec::new()),
                _ => Err(e),
            },
        }
    }

    pub fn register_znode_change_handler_and_check_existence(
        &self,
        handler: Arc<dyn ZkChangeHandler>,
    ) -> ZkResult<bool> {
        self.register_znode_change_handler(handler.clone());
        match self.client.exists_w(
            handler.path().as_str(),
            KafkaZkWatcher::init(self.handlers.clone()),
        ) {
            Ok(_) => Ok(true),
            Err(e) => match e {
                ZkError::NoNode => Ok(false),
                _ => Err(e),
            },
        }
    }

    pub fn register_znode_change_handler(&self, handler: Arc<dyn ZkChangeHandler>) {
        self.handlers.register_znode_change_handler(handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_change_handler(path);
    }

    pub fn register_znode_child_change_handler(&self, handler: Arc<dyn ZkChildChangeHandler>) {
        self.handlers.register_znode_child_change_handler(handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_child_change_handler(path);
    }

    fn should_watch(&self, path: String, req_type: u32, register_watch: bool) -> ZkResult<bool> {
        match req_type {
            GET_CHILDREN_REQUEST => {
                let handlers = self.handlers.child_change_handlers.read().unwrap();
                match handlers.get(&path) {
                    Some(_) => Ok(true && register_watch),
                    None => Ok(false),
                }
            }
            GET_REQUEST | EXIST_REQUEST => {
                let handlers = self.handlers.change_handlers.read().unwrap();
                match handlers.get(&path) {
                    Some(_) => Ok(true),
                    None => Ok(false),
                }
            }
            _ => Err(ZkError::NoAuth),
        }
    }
}

#[cfg(test)]
mod client_tests {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::Duration,
    };

    use zookeeper::{recipes::leader, Acl, CreateMode, ZkError, ZkResult};

    use crate::{
        common::{
            broker::BrokerInfo,
            topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
        },
        controller::constants::INITIAL_CONTROLLER_EPOCH,
        zk::{
            zk_data::{
                BrokerIdZNode, ControllerEpochZNode, ControllerZNode, PersistentZkPaths,
                TopicPartitionStateZNode, TopicPartitionZNode, TopicPartitionsZNode, TopicZNode,
                TopicsZNode,
            },
            zk_watcher::KafkaZkHandlers,
        },
    };

    use super::KafkaZkClient;

    const CONN_STR: &str = "localhost:2181";
    const SESS_TIMEOUT: Duration = Duration::from_secs(3);

    fn get_client() -> KafkaZkClient {
        let client = KafkaZkClient::init(CONN_STR, SESS_TIMEOUT);
        assert!(!client.is_err());
        client.unwrap()
    }

    #[test]
    fn test_create_top_level_paths() {
        let client = get_client();

        client.create_top_level_paths();
        let persistent_path = PersistentZkPaths::init();
        for path in persistent_path.paths {
            let result = client.client.get_data(&path, false);

            match result {
                Ok(_) => {}
                Err(e) => match e {
                    ZkError::NodeExists => (),
                    _ => panic!("failed with some other reasons"),
                },
            }
        }
    }

    #[test]
    fn test_register_controller_and_increment_controller_epoch() {
        let id = 0;
        let client = get_client();
        let _ = client.register_controller_and_increment_controller_epoch(id);

        match client
            .client
            .get_data(ControllerZNode::path().as_str(), false)
        {
            Ok(data) => {
                assert_eq!(ControllerZNode::decode(&data.0), id);
            }
            Err(e) => panic!("{}", e),
        }

        match client
            .client
            .get_data(ControllerEpochZNode::path().as_str(), false)
        {
            Ok(data) => {
                assert_eq!(ControllerEpochZNode::decode(&data.0), 1);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_create_controller_epoch_znode() {
        let client = get_client();
        let _ = client.create_controller_epoch_znode();

        match client
            .client
            .get_data(ControllerEpochZNode::path().as_str(), false)
        {
            Ok(data) => {
                assert_eq!(
                    ControllerEpochZNode::decode(&data.0),
                    INITIAL_CONTROLLER_EPOCH
                );
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_controller_epoch() {
        let client = get_client();
        let result = client.get_controller_epoch();

        match result {
            Ok(resp) => match resp {
                Some(epoch) => assert_eq!(epoch.0, 0),
                None => panic!("controller_epoch is not there"),
            },
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_controller_id() {
        let client = get_client();
        let _ = client.register_controller_and_increment_controller_epoch(0);
        let result = client.get_controller_id();
        match result {
            Ok(resp) => match resp {
                Some(id) => assert_eq!(id, 0),
                None => panic!("controller has not registerd"),
            },
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_check_controller() {
        let client = get_client();
        let _ = client.register_controller_and_increment_controller_epoch(0);
        match client.check_controller(0) {
            Ok(resp) => assert_eq!(true, resp),
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_check_epoch() {
        let client = get_client();
        match client.check_epoch(1) {
            Ok(resp) => assert_eq!(false, resp),
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_register_broker() {
        let client = get_client();
        let broker_info: BrokerInfo = BrokerInfo::init("localhost", "223", 0);
        let _ = client.register_broker(broker_info);
        match client
            .client
            .get_data(BrokerIdZNode::path(0).as_str(), false)
        {
            Ok(_) => {}
            Err(e) => panic!("register failed {}", e),
        }
    }

    #[test]
    fn test_get_stat_after_node_exists() {
        let client = get_client();
        let broker_info: BrokerInfo = BrokerInfo::init("localhost", "223", 0);
        let _ = client.register_broker(broker_info);
        match client.get_stat_after_node_exists(BrokerIdZNode::path(0).as_str()) {
            Ok(_) => {}
            Err(e) => panic!("register failed {}", e),
        }
    }

    #[test]
    fn test_get_broker() {
        let client = get_client();
        let broker_info: BrokerInfo = BrokerInfo::init("localhost", "223", 0);
        let _ = client.register_broker(broker_info.clone());
        match client.get_broker(0) {
            Ok(fetched) => match fetched {
                Some(info) => {
                    assert_eq!(info.hostname, broker_info.hostname);
                    assert_eq!(info.port, broker_info.port);
                    assert_eq!(info.id, broker_info.id);
                }
                None => panic!("broker is not registered"),
            },
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_all_brokers() {
        let client = get_client();
        let broker_info0: BrokerInfo = BrokerInfo::init("localhost", "223", 0);
        let broker_info1: BrokerInfo = BrokerInfo::init("localhost", "224", 1);
        let broker_infos = vec![broker_info0.clone(), broker_info1.clone()];
        let _ = client.register_broker(broker_info0.clone());
        let _ = client.register_broker(broker_info1.clone());
        let mut i = 0;
        match client.get_all_brokers() {
            Ok(fetched) => {
                for broker in fetched {
                    match broker {
                        Some(info) => {
                            assert_eq!(info.hostname, broker_infos[i].hostname);
                            assert_eq!(info.port, broker_infos[i].port);
                            assert_eq!(info.id, broker_infos[i].id);
                        }
                        None => panic!("brokers should all be up"),
                    };
                    i = i + 1;
                }
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_all_broker_and_epoch() {
        let client = get_client();
        let broker_info0: BrokerInfo = BrokerInfo::init("localhost", "223", 0);
        let broker_info1: BrokerInfo = BrokerInfo::init("localhost", "224", 1);
        let broker_infos = vec![broker_info0.clone(), broker_info1.clone()];
        let _ = client.register_broker(broker_info0.clone());
        let _ = client.register_broker(broker_info1.clone());

        match client.get_all_broker_and_epoch() {
            Ok(fetched) => {
                for info in broker_infos {
                    assert!(fetched.contains_key(&info));
                    assert!(!(*fetched.get(&info).unwrap() == (2 as i64)));
                }
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_delete_isr_change_notifications() {
        todo!();
    }

    #[test]
    fn test_delete_isr_change_notifications_with_sequence_num() {
        todo!();
    }

    #[test]
    fn test_get_all_topics() {
        let client = get_client();
        let _ = client.client.create(
            TopicsZNode::path().as_str(),
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        let _ = client.client.create(
            format!("{}/test", TopicsZNode::path()).as_str(),
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );

        match client.get_all_topics(false) {
            Ok(resp) => assert!(resp.contains("test")),
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_create_topic_partitions() {
        let client = get_client();
        let _ = client.create_topic_partitions(vec!["test".to_string()]);

        match client
            .client
            .exists(TopicPartitionsZNode::path("test").as_str(), false)
        {
            Ok(resp) => match resp {
                Some(_) => {}
                None => panic!("topic partition is not created"),
            },
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_create_topic_partition() {
        let client = get_client();
        let _ = client.create_topic_partition(vec![TopicPartition {
            topic: "test".to_string(),
            partition: 0,
        }]);

        match client
            .client
            .exists(TopicPartitionZNode::path("test", 0).as_str(), false)
        {
            Ok(resp) => match resp {
                Some(_) => {}
                None => panic!("topic partition is not created"),
            },
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_create_topic_partition_state() {
        let client = get_client();
        let partition = TopicPartition {
            topic: "test".to_string(),
            partition: 0,
        };

        let leader_and_isr = LeaderAndIsr::init(0, vec![0, 1, 2], 1, 1);

        let mut leader_isr_and_epoch = HashMap::new();
        leader_isr_and_epoch.insert(partition, leader_and_isr);
        let _ = client.create_topic_partition_state(leader_isr_and_epoch);

        match client
            .client
            .get_data(TopicPartitionStateZNode::path("test", 0).as_str(), false)
        {
            Ok(resp) => {
                let state = TopicPartitionStateZNode::decode(&resp.0);
                assert_eq!(state.leader, 0);
                assert_eq!(state.isr, vec![0, 1, 2]);
                assert_eq!(state.controller_epoch, 1);
                assert_eq!(state.leader_epoch, 1);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_partitions_for_topics() {
        // we need a set function to examine this
        todo!();
    }

    #[test]
    fn test_get_partition_assignment_for_topics() {
        // we need a set function to examine this
        todo!();
    }

    #[test]
    fn test_get_full_replica_assignment_for_topics() {
        // we need a set function to examine this
        todo!();
    }

    #[test]
    fn test_set_replica_assignment_for_topics() {
        let client = get_client();
        let topics = vec!["test".to_string()];
        let assignment = ReplicaAssignment::init(HashMap::new(), HashMap::new(), HashMap::new());
        let replica_assignments = vec![assignment];
        let version = None;

        let _ = client.set_replica_assignment_for_topics(topics, replica_assignments, version);

        match client
            .client
            .get_data(TopicZNode::path("test").as_str(), false)
        {
            Ok(resp) => {
                let data = TopicZNode::decode(&resp.0);
                assert!(data.partitions.is_empty());
                assert!(data.adding_replicas.is_empty());
                assert!(data.removing_replicas.is_empty());
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_get_leader_and_isr() {
        todo!();
    }

    #[test]
    fn test_set_leader_and_isr() {
        todo!();
    }

    #[test]
    fn test_get_children() {
        todo!();
    }
}
