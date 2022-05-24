use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};
use zookeeper::{Acl, CreateMode, Stat, ZkError, ZkResult, ZkState, ZooKeeper};

use super::zk_data::PersistentZkPaths;
use super::{
    zk_data::{
        BrokerIdZNode, BrokerIdsZNode, ControllerEpochZNode, ControllerZNode,
        TopicPartitionOffsetZNode, TopicPartitionStateZNode, TopicPartitionsZNode, TopicZNode,
        TopicsZNode,
    },
    zk_watcher::{KafkaZkHandlers, KafkaZkWatcher, ZkChangeHandler, ZkChildChangeHandler},
};
use crate::common::{
    broker::BrokerInfo,
    topic_partition::{
        LeaderAndIsr, PartitionOffset, ReplicaAssignment, TopicIdReplicaAssignment, TopicPartition,
    },
};

use crate::controller::constants::{INITIAL_CONTROLLER_EPOCH, INITIAL_CONTROLLER_EPOCH_ZK_VERSION};
pub struct KafkaZkClient {
    pub client: ZooKeeper,
    pub handlers: KafkaZkHandlers,
}

impl KafkaZkClient {
    pub fn init(conn_str: &str, sess_timeout: Duration) -> ZkResult<KafkaZkClient> {
        let handlers = KafkaZkHandlers {
            change_handlers: Arc::new(RwLock::new(HashMap::new())),
            child_change_handlers: Arc::new(RwLock::new(HashMap::new())),
        };

        let client = ZooKeeper::connect(
            conn_str,
            sess_timeout,
            KafkaZkWatcher {
                handlers: handlers.clone(),
            },
        );

        match wait_until_connected(client.as_ref().unwrap(), Duration::from_millis(10)) {
            Ok(_) => Ok(KafkaZkClient {
                client: client.unwrap(),
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
        let path = self.client.create(
            path,
            data,
            Acl::open_unsafe().clone(),
            zookeeper::CreateMode::Persistent,
        );

        // TODO: maybe we should retry
        match path {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
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
            None => match self.create_controller_epoch_znode(INITIAL_CONTROLLER_EPOCH) {
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

    fn create_controller_epoch_znode(&self, epoch: u128) -> ZkResult<(u128, i32)> {
        match self.client.create(
            ControllerEpochZNode::path().as_str(),
            ControllerEpochZNode::encode(epoch),
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
        match self
            .client
            .get_data(ControllerEpochZNode::path().as_str(), true)
        {
            Ok(resp) => Ok(Some((ControllerEpochZNode::decode(&resp.0), resp.1))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => return Err(e),
            },
        }
    }

    pub fn get_controller_id(&self) -> ZkResult<Option<u32>> {
        match self.client.get_data(ControllerZNode::path().as_str(), true) {
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
        match self.client.exists(path, false) {
            Ok(resp) => Ok(resp.unwrap()),
            Err(e) => Err(e),
        }
    }

    pub fn get_broker(&self, broker_id: u32) -> ZkResult<Option<BrokerInfo>> {
        match self
            .client
            .get_data(BrokerIdZNode::path(broker_id).as_str(), false)
        {
            Ok(data) => Ok(Some(BrokerIdZNode::decode(&data.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub fn get_all_brokers(&self) -> ZkResult<Vec<Option<BrokerInfo>>> {
        let broker_ids: Vec<u32> = match self.get_children(BrokerIdsZNode::path().as_str()) {
            Ok(list) => list.iter().map(|id| id.parse::<u32>().unwrap()).collect(),
            Err(e) => return Err(e),
        };

        let mut brokers: Vec<Option<BrokerInfo>> = Vec::new();

        for id in broker_ids.iter() {
            match self.get_broker(*id) {
                Ok(info) => brokers.push(info),
                Err(e) => return Err(e),
            }
        }

        Ok(brokers)
    }

    pub fn delete_isr_change_notifications(&self, epoch_zk_version: i32) {
        todo!();
    }

    pub fn get_all_broker_and_epoch(&self) -> HashMap<BrokerInfo, u128> {
        todo!();
    }
    // Topic + Partition

    pub fn get_all_topics(&self, watch: bool) -> ZkResult<HashSet<String>> {
        match self
            .client
            .get_children(TopicsZNode::path().as_str(), watch)
        {
            Ok(resp) => Ok(HashSet::from_iter(resp.iter().cloned())),
            Err(e) => match e {
                ZkError::NoNode => Ok(HashSet::new()),
                _ => Err(e),
            },
        }
    }

    // TODO: not sure if this is correct, need to check
    pub fn create_topic_partitions(
        &self,
        topics: Vec<String>,
        expected_controller_epoch_zk_version: i32,
    ) -> Vec<ZkResult<String>> {
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
                            for t_partition in am.partitions.iter() {
                                if t_partition.0.topic == topic.to_string() {
                                    partition_numbers.push(t_partition.0.partition);
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
                self.client
                    .get_data(TopicZNode::path(topic).as_str(), false)
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

    pub fn get_replica_assignment_for_topics(
        &self,
        topics: Vec<String>,
    ) -> ZkResult<HashMap<String, ReplicaAssignment>> {
        let mut replica_assignment: HashMap<String, ReplicaAssignment> = HashMap::new();
        let resps: Vec<Result<(Vec<u8>, Stat), ZkError>> = topics
            .iter()
            .map(|topic| {
                self.client
                    .get_data(TopicZNode::path(topic).as_str(), false)
            })
            .collect();
        let mut i = 0;

        for resp in resps.iter() {
            match resp {
                Ok(data) => {
                    replica_assignment.insert(topics[i].clone(), TopicZNode::decode(&data.0));
                }
                Err(e) => return Err(*e),
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
        todo!();
    }

    pub fn set_leader_and_isr(
        &self,
        leader_and_isrs: HashMap<TopicPartition, LeaderAndIsr>,
        controller_epoch: u128,
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
    ) -> ZkResult<Option<PartitionOffset>> {
        match self.client.get_data(
            TopicPartitionOffsetZNode::path(topic, partition).as_str(),
            false,
        ) {
            Ok(resp) => Ok(Some(TopicPartitionOffsetZNode::decode(&resp.0))),
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
        partition_offset: PartitionOffset,
        version: Option<i32>,
    ) -> ZkResult<bool> {
        match self.client.set_data(
            TopicPartitionOffsetZNode::path(topic, partition).as_str(),
            TopicPartitionOffsetZNode::encode(partition_offset),
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
    ) -> ZkResult<HashMap<TopicPartition, (LeaderAndIsr, u128)>> {
        todo!();
    }

    pub fn get_replica_assignment_and_topic_ids_for_topics(
        &self,
        topics: HashSet<String>,
    ) -> ZkResult<HashSet<TopicIdReplicaAssignment>> {
        todo!();
    }

    // ZooKeeper

    pub fn get_children(&self, path: &str) -> ZkResult<Vec<String>> {
        match self.client.get_children(path, false) {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    pub fn register_znode_change_handler(&self, handler: Arc<Box<dyn ZkChangeHandler>>) {
        self.handlers.register_znode_change_handler(handler);
    }

    pub fn register_znode_change_handler_and_check_existence(
        &self,
        handler: Arc<Box<dyn ZkChangeHandler>>,
    ) {
        todo!();
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_change_handler(path);
    }

    pub fn register_znode_child_change_handler(&self, handler: Arc<Box<dyn ZkChildChangeHandler>>) {
        self.handlers.register_znode_child_change_handler(handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_child_change_handler(path);
    }
}

fn wait_until_connected(client: &ZooKeeper, timeout: Duration) -> ZkResult<()> {
    // TODO: need to check correctness
    let mut state: ZkState = ZkState::Closed;
    client.add_listener(move |state| {
        let start = SystemTime::now();
        while state != ZkState::Connected && state != ZkState::Connecting {
            if start.elapsed().unwrap() >= timeout {
                eprintln!("Connection timeout");
                break;
            }
        }

        if state == ZkState::AuthFailed {
            eprintln!("Conntion AuthFailed");
        } else if state == ZkState::Closed {
            eprintln!("Connection closed");
        }
    });

    if state != ZkState::Connected || state != ZkState::ConnectedReadOnly {
        return Err(ZkError::SystemError);
    } else {
        return Ok(());
    }
}
