use std::{sync::Arc, collections::VecDeque};

use super::{controller::Controller, controller_events::ControllerEvent};

use tokio::sync::Mutex;

pub struct ControllerEventManager {
    queue: Arc<Mutex<VecDeque<QueuedEvent>>>,
    processor: Arc<Controller>,
}

// TODO: is there a way to not use Mutex?
impl ControllerEventManager {
    pub fn init(processor: Arc<Controller>) -> ControllerEventManager {
        ControllerEventManager { queue: Arc::new(Mutex::new(VecDeque::new())), processor: processor }
    }

    pub async fn put(&self, event: Box<dyn ControllerEvent> ) {
        let q_event = QueuedEvent { event: Arc::new(event)};

        let mut q = self.queue.lock().await;
        (*q).push_front(q_event);

    }

    pub fn start(&self) {
        tokio::spawn(event_worker(self.queue.clone(), self.processor.clone()));
    }
}

async fn event_worker(queue: Arc<Mutex<VecDeque<QueuedEvent>>>, processor: Arc<Controller>) -> () {
    loop {
        let mut q = queue.lock().await;
        match (*q).pop_back() {
            Some(event) => {
                event.process(processor.clone());
            },
            None => {},
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