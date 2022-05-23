use std::{collections::HashMap, sync::Arc};

use crate::zk::{zk_watcher::{ZkChangeHandler, ZkChildChangeHandler}, zk_data::{BrokerIdsZNode, ControllerZNode, BrokerIdZNode, TopicsZNode}};

use super::{event_manager::{ControllerEventManager}, controller_events::{TopicChange, BrokerModification, ControllerChange, ReElect, BrokerChange}};

pub fn get_change_handlers(em: Arc<ControllerEventManager>, bid: u32) -> HashMap<String, Arc<Box<dyn ZkChangeHandler>>> {
    let mut change_handlers: HashMap<String, Arc<Box<dyn ZkChangeHandler>>> = HashMap::new();

    change_handlers.insert("ControllerChange".to_string(), Arc::new(Box::new(ControllerChangeHandler {
        event_manager: em.clone(),
    })));

    change_handlers.insert("BrokerModification".to_string(), Arc::new(Box::new(BrokerModificationHandler {
        broker_id: bid,
        event_manager: em.clone(),
    })));

    change_handlers
}

pub fn get_child_change_handlers(em: Arc<ControllerEventManager>) -> HashMap<String, Arc<Box<dyn ZkChildChangeHandler>>> {
    let mut child_change_handlers: HashMap<String, Arc<Box<dyn ZkChildChangeHandler>>> = HashMap::new();

    child_change_handlers.insert("BrokerChange".to_string(), Arc::new(Box::new(BrokerChangeHandler {
        event_manager: em.clone(),
    })));

    child_change_handlers.insert("TopicChange".to_string(), Arc::new(Box::new(TopicChangeHandler {
        event_manager: em.clone(),
    })));

    child_change_handlers
}

pub struct BrokerChangeHandler {
    event_manager: Arc<ControllerEventManager>,
}

impl ZkChildChangeHandler for BrokerChangeHandler {
    fn path(&self) -> String {
        BrokerIdsZNode::path()
    }

    fn handle_child_change(&self) {
        self.event_manager.put(Box::new(BrokerChange {}));
    }
}

pub struct ControllerChangeHandler {
    pub event_manager: Arc<ControllerEventManager>,
}

impl ZkChangeHandler for ControllerChangeHandler {
    fn path(&self) -> String {
        ControllerZNode::path()
    }

    fn handle_create(&self) {
        self.event_manager.put(Box::new(ControllerChange {}));
    }

    fn handle_delete(&self) {
        self.event_manager.put(Box::new(ReElect {}));
    }

    fn handle_data_change(&self) {
        self.event_manager.put(Box::new(ControllerChange {}));
    }
}

pub struct BrokerModificationHandler {
    broker_id: u32,
    event_manager: Arc<ControllerEventManager>,
}

impl ZkChangeHandler for BrokerModificationHandler {
    fn path(&self) -> String {
        BrokerIdZNode::path(self.broker_id)
    }

    fn handle_create(&self) {
        todo!();
    }

    fn handle_delete(&self) {
        todo!();
    }

    fn handle_data_change(&self) {
        self.event_manager.put(Box::new(BrokerModification {}));
    }
}

pub struct TopicChangeHandler {
    event_manager: Arc<ControllerEventManager>,
}

impl ZkChildChangeHandler for TopicChangeHandler {
    fn path(&self) -> String {
        TopicsZNode::path()
    }

    fn handle_child_change(&self) {
        self.event_manager.put(Box::new(TopicChange {}));
    }
}