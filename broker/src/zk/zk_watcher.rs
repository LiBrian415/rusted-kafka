use std::{sync::{Arc, RwLock}, collections::HashMap};

use zookeeper::{Watcher, WatchedEventType};

pub struct KafkaZkWatcher {
    handlers: KafkaZkHandlers
}

/// Note: Adding a handler only registers it with the Handler object. It doesn't
/// create a Watcher. Instead, a watcher will be created for an operation if a 
/// handler exists for the path.
/// 
/// I.e. It's better to think of handlers and watchers as notifications/interrupts 
/// instead of callbacks. Namely, they should trigger a state change which will 
/// causes a cache invalidation and force the Broker to retry at some later time.
pub struct KafkaZkHandlers {
    change_handlers: Arc<RwLock<HashMap<String, Box<dyn ZkChangeHandler>>>>, 
    child_change_handlers: Arc<RwLock<HashMap<String, Box<dyn ZkChildChangeHandler>>>>, 
}

pub trait ZkChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handle_create(&self);
    fn handle_delete(&self);
    fn handle_data_change(&self);
}

pub trait ZkChildChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handle_child_change(&self);
}

impl Watcher for KafkaZkWatcher {
    fn handle(&self, event: zookeeper::WatchedEvent) {
        if let Some(path) = event.path {
            match event.event_type {
                WatchedEventType::NodeCreated => {
                    let g = self.handlers.change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_create();
                    }
                },
                WatchedEventType::NodeDeleted => {
                    let g = self.handlers.change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_delete();
                    }
                },
                WatchedEventType::NodeDataChanged => {
                    let g = self.handlers.change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_data_change();
                    }
                },
                WatchedEventType::NodeChildrenChanged => {
                    let g = self.handlers.child_change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_child_change();
                    }
                },
                _ => {}
            }
        }
    }
}

impl KafkaZkHandlers {

    pub fn register_znode_change_handler(&self, handler: Box<dyn ZkChangeHandler>) {
        let mut g = self.change_handlers.write().unwrap();
        (*g).insert(handler.path(), handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        let mut g = self.change_handlers.write().unwrap();
        (*g).remove(path);
    }

    pub fn contains_znode_change_handler(&self, path: &str) -> bool {
        let g = self.change_handlers.read().unwrap();
        (*g).contains_key(path)
    }

    pub fn register_znode_child_change_handler(&self, handler: Box<dyn ZkChildChangeHandler>) {
        let mut g = self.child_change_handlers.write().unwrap();
        (*g).insert(handler.path(), handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        let mut g = self.child_change_handlers.write().unwrap();
        (*g).remove(path);
    }

    pub fn contains_znode_child_change_handler(&self, path: &str) -> bool {
        let g = self.child_change_handlers.read().unwrap();
        (*g).contains_key(path)
    }

}