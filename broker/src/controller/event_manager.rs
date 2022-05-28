use std::sync::mpsc::SyncSender;

use super::controller_events::ControllerEvent;

// this structure just have a tx channel, and define the true callback functions for watchers
#[derive(Clone)]
pub struct ControllerEventManager {
    processor_queue: SyncSender<Box<dyn ControllerEvent>>,
}

impl ControllerEventManager {
    pub fn init(sender: SyncSender<Box<dyn ControllerEvent>>) -> ControllerEventManager {
        ControllerEventManager {
            processor_queue: sender,
        }
    }

    pub fn put(&self, event: Box<dyn ControllerEvent>) {
        let _ = self.processor_queue.send(event);
    }
}
