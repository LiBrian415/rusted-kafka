use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use super::{controller::Controller, controller_events::ControllerEvent};

pub struct ControllerEventManager {
    queue: Arc<RwLock<VecDeque<QueuedEvent>>>,
    processor: Arc<Controller>,
}

// TODO: is there a way to not use Mutex?
impl ControllerEventManager {
    pub fn init(processor: Arc<Controller>) -> ControllerEventManager {
        ControllerEventManager {
            queue: Arc::new(RwLock::new(VecDeque::new())),
            processor: processor,
        }
    }

    pub fn put(&self, event: Box<dyn ControllerEvent>) {
        let q_event = QueuedEvent {
            event: Arc::new(event),
        };

        let mut q = self.queue.write().unwrap();
        (*q).push_front(q_event);
        std::mem::drop(q);
    }

    pub fn start(&self) {
        tokio::spawn(event_worker(self.queue.clone(), self.processor.clone()));
    }
}

async fn event_worker(queue: Arc<RwLock<VecDeque<QueuedEvent>>>, processor: Arc<Controller>) -> () {
    loop {
        let mut q = queue.write().unwrap();
        match (*q).pop_back() {
            Some(event) => {
                event.process(processor.clone());
            }
            None => {}
        }
        std::mem::drop(q);
    }
}

struct QueuedEvent {
    event: Arc<Box<dyn ControllerEvent>>,
}

impl QueuedEvent {
    fn process(&self, processor: Arc<Controller>) {
        processor.process(self.event.clone());
    }
}
