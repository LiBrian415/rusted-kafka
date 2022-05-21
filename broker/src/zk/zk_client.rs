use zookeeper::{ZooKeeper, ZkResult, ZkState, ZkError, Acl, Stat, CreateMode};
use std::{
    time::{Duration, SystemTime},
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::{zk_watcher::{KafkaZkHandlers, ZkChangeHandler, ZkChildChangeHandler, KafkaZkWatcher}, zk_data::{ControllerEpochZNode, ControllerZNode, BrokerIdZNode, BrokerIdsZNode}};
use super::zk_data::PersistentZkPaths;
use crate::common::broker::BrokerInfo;

use crate::controller::constants::{InitialControllerEpoch, InitialControllerEpochZkVersion};
pub struct KafkaZkClient {
    client: ZooKeeper,
    handlers: KafkaZkHandlers
}

impl KafkaZkClient {

    pub fn init(conn_str: &str, sess_timeout: Duration) -> ZkResult<KafkaZkClient> {
        let handlers = KafkaZkHandlers {
            change_handlers: Arc::new(RwLock::new(HashMap::new())),
            child_change_handlers: Arc::new(RwLock::new(HashMap::new())),
        };

        let client = 
            ZooKeeper::connect(conn_str, sess_timeout, 
                KafkaZkWatcher{
                    handlers: handlers.clone(),
            });
        
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
        let _ :Vec<ZkResult<()>> = persistent_path.paths.iter().map(|path| self.check_persistent_path(path)).collect();
    }

    fn check_persistent_path(&self, path: &str) -> ZkResult<()> {
        self.create_recursive(path, Vec::new())
    }

    fn create_recursive(&self, path: &str, data: Vec<u8>) -> ZkResult<()> {
        let path = self.client.create(path, data, Acl::open_unsafe().clone(), zookeeper::CreateMode::Persistent);

        // TODO: maybe we should retry
        match path {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    // Controller

    pub fn register_controller_and_increment_controller_epoch(&self, controller_id: u32) -> ZkResult<(u128, i32)>{
        // read /controller_epoch to get the current controller epoch and zkVersion
        // create /controller_epoch if not exists
        let result = match self.get_controller_epoch() {
            Ok(resp) => resp,
            Err(e) => return Err(e),
        };
        
        let (curr_epoch, curr_epoch_zk_version) = match result {
            Some(info) => (info.0, info.1.version),
            None => match self.create_controller_epoch_znode(InitialControllerEpoch) {
                Ok(r) => r,
                Err(e) => return Err(e),
            }
        };

        // try create /controller and update /controller_epoch atomatically
        let new_controller_epoch = curr_epoch + 1;
        let expected_controller_epoch_zk_version = curr_epoch_zk_version;
        let mut correct_controller = false;
        let mut correct_epoch = false;
        
        match self.client.create(ControllerZNode::path().as_str(), ControllerZNode::encode(controller_id),
         Acl::open_unsafe().clone(), CreateMode::Ephemeral) {
             Ok(_) => match self.client.set_data(ControllerEpochZNode::path().as_str(), ControllerEpochZNode::encode(new_controller_epoch), Some(expected_controller_epoch_zk_version)){
                Ok(resp) => return Ok((new_controller_epoch, resp.version)),
                Err(e) => match e {
                    ZkError::BadVersion => {
                        match self.check_epoch(new_controller_epoch) {
                            Ok(result) => {
                                correct_epoch = result;
                            },
                            Err(e) => return Err(e),
                        }
                    },
                    _ => return Err(e),
                }
             },
             Err(e) => match e{
                ZkError::NodeExists => match self.check_controller(controller_id) {
                    Ok(result) => {
                        correct_controller = result;
                    },
                    Err(e) => return Err(e),
                },
                _ => return Err(e),
             }
         };

         if correct_controller && correct_epoch {
             match self.get_controller_epoch() {
                 Ok(resp) => {
                     let result = resp.unwrap();
                     Ok((result.0, result.1.version))
                 },
                 Err(e) => Err(e),
             }   
         } else {
             return Err(ZkError::SystemError);
         }
    }

    fn create_controller_epoch_znode(&self, epoch: u128) -> ZkResult<(u128, i32)>{
        match self.client.create(ControllerEpochZNode::path().as_str(), ControllerEpochZNode::encode(epoch),
        Acl::open_unsafe().clone(), CreateMode::Persistent) {
            Ok(_) => Ok((InitialControllerEpoch, InitialControllerEpochZkVersion)),
            Err(e) => match e {
                ZkError::NodeExists => match self.get_controller_epoch() {
                    Ok(resp) => match resp {
                        Some(info) => Ok((info.0, info.1.version)),
                        None => Err(e)
                    },
                    Err(e) => Err(e),
                },
                _ => Err(e)
            }
        }
    }

    fn get_controller_epoch(&self) -> ZkResult<Option<(u128, Stat)>> {
        match self.client.get_data(ControllerEpochZNode::path().as_str(), true) {
            Ok(resp) => Ok(Some((ControllerEpochZNode::decode(&resp.0), resp.1))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => return Err(e),
            }
        }
    }

    pub fn get_controller_id(&self) -> ZkResult<Option<u32>> {
        match self.client.get_data(ControllerZNode::path().as_str(), true) {
            Ok(resp) => Ok(Some(ControllerZNode::decode(&resp.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e)
            }
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
        match self.client.create(BrokerIdZNode::path(broker.id).as_str(), BrokerIdZNode::encode(&broker),
        Acl::open_unsafe().clone(), CreateMode::Ephemeral) {
            Ok(_) => match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str()) {
                Ok(stat) => Ok(stat.czxid),
                Err(e) => Err(e),
            },
            Err(e) => match e {
                ZkError::NodeExists =>match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str()) {
                    Ok(stat) => Ok(stat.czxid),
                    Err(e) => Err(e),
                },
                _ => return Err(e),
            }
        }
    }

    fn get_stat_after_node_exists(&self, path: &str) -> ZkResult<Stat> {
        match self.client.exists(path, false) {
            Ok(resp) => Ok(resp.unwrap()),
            Err(e) => Err(e),
        }
    }

    pub fn get_broker(&self, broker_id: u32) -> ZkResult<Option<BrokerInfo>> {
        match self.client.get_data(BrokerIdZNode::path(broker_id).as_str(), false) {
            Ok(data) => Ok(Some(BrokerIdZNode::decode(&data.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            }
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
    
    // Topic + Partition

    pub fn get_all_topics() {
        todo!();
    }

    pub fn create_topic_partitions() {
        todo!();
    }

    pub fn get_partitions_for_topics() {
        todo!();
    }

    pub fn get_replica_assignment_for_topics() {
        todo!();
    }

    pub fn set_replica_assignment_for_topics() {
        todo!();
    }

    pub fn set_leader_and_isr() {
        todo!();
    }

    pub fn get_topic_partition_offset() {
        todo!();
    }

    pub fn set_topic_partition_offset() {
        todo!();
    }

    // ZooKeeper

    pub fn get_children(&self, path: &str) -> ZkResult<Vec<String>> {
        match self.client.get_children(path, false) {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    pub fn register_znode_change_handler(&self, handler: Box<dyn ZkChangeHandler>) {
        self.handlers.register_znode_change_handler(handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_change_handler(path);
    }

    pub fn register_znode_child_change_handler(&self, handler: Box<dyn ZkChildChangeHandler>) {
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
        return Ok(())
    }
}