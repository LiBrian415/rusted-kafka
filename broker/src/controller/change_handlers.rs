use std::{collections::HashMap, sync::Arc};

use crate::zk::{
    zk_data::{BrokerIdZNode, BrokerIdsZNode, ControllerZNode, TopicsZNode},
    zk_watcher::{ZkChangeHandler, ZkChildChangeHandler},
};

use super::{
    controller_events::{BrokerChange, BrokerModification, ControllerChange, ReElect, TopicChange},
    event_manager::ControllerEventManager,
};

pub fn get_change_handlers(
    em: Arc<ControllerEventManager>,
    bid: u32,
) -> HashMap<String, Arc<dyn ZkChangeHandler>> {
    let mut change_handlers: HashMap<String, Arc<dyn ZkChangeHandler>> = HashMap::new();

    change_handlers.insert(
        "ControllerChange".to_string(),
        Arc::new(ControllerChangeHandler {
            event_manager: em.clone(),
        }),
    );

    change_handlers.insert(
        "BrokerModification".to_string(),
        Arc::new(BrokerModificationHandler {
            broker_id: bid,
            event_manager: em.clone(),
        }),
    );

    change_handlers
}

pub fn get_child_change_handlers(
    em: Arc<ControllerEventManager>,
) -> HashMap<String, Arc<dyn ZkChildChangeHandler>> {
    let mut child_change_handlers: HashMap<String, Arc<dyn ZkChildChangeHandler>> = HashMap::new();

    child_change_handlers.insert(
        "BrokerChange".to_string(),
        Arc::new(BrokerChangeHandler {
            event_manager: em.clone(),
        }),
    );

    child_change_handlers.insert(
        "TopicChange".to_string(),
        Arc::new(TopicChangeHandler {
            event_manager: em.clone(),
        }),
    );

    child_change_handlers
}

pub struct BrokerChangeHandler {
    pub event_manager: Arc<ControllerEventManager>,
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
    pub broker_id: u32,
    pub event_manager: Arc<ControllerEventManager>,
}

impl ZkChangeHandler for BrokerModificationHandler {
    fn path(&self) -> String {
        BrokerIdZNode::path(self.broker_id)
    }

    fn handle_create(&self) {}

    fn handle_delete(&self) {}

    fn handle_data_change(&self) {
        self.event_manager.put(Box::new(BrokerModification {
            broker_id: self.broker_id,
        }));
    }
}

pub struct TopicChangeHandler {
    pub event_manager: Arc<ControllerEventManager>,
}

impl ZkChildChangeHandler for TopicChangeHandler {
    fn path(&self) -> String {
        TopicsZNode::path()
    }

    fn handle_child_change(&self) {
        self.event_manager.put(Box::new(TopicChange {}));
    }
}
