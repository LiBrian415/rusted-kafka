use std::{iter::Map, sync::{Arc, Mutex}};

use zookeeper::Watcher;

pub struct KafkaZkWatcher {
    handlers: KafkaZkHandlers
}

pub trait ZkChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handleCreate(&self);
    fn handleDelete(&self);
    fn handleDataChange(&self);
}

pub struct KafkaZkHandlers {
    changeHandler: Arc<Mutex<Map<String, Box<dyn ZkChangeHandler>>>>, 
    childChangeHandler: Arc<Mutex<Map<String, Box<dyn ZkChildChangeHandler>>>>, 
}

pub trait ZkChildChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handleChildChange(&self);
}

impl Watcher for KafkaZkWatcher {
    fn handle(&self, event: zookeeper::WatchedEvent) {
        todo!()
    }
}

impl KafkaZkHandlers {

    pub fn getZNodeChangeHandler() {
        todo!();
    }

    pub fn registerZNodeChangeHandler() {
        todo!();
    }

    pub fn unregisterZNodeChangeHandler() {
        todo!();
    }

    pub fn getZNodeChildChangeHandler() {
        todo!();
    }
    
    pub fn registerZNodeChildChangeHandler() {
        todo!();
    }

    pub fn unregisterZNodeChildChangeHandler() {
        todo!();
    }

}