use std::any::Any;
// https://stackoverflow.com/questions/33687447/how-to-get-a-reference-to-a-concrete-type-from-a-trait-object
use super::constants::{
    EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE, EVENT_RE_ELECT,
    EVENT_STARTUP, EVENT_TOPIC_CHNAGE,
};

pub trait ControllerEvent: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn state(&self) -> u32;
}

pub struct TopicChange {}
impl ControllerEvent for TopicChange {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_TOPIC_CHNAGE
    }
}

pub struct Startup {}
impl ControllerEvent for Startup {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_STARTUP
    }
}

pub struct BrokerModification {
    pub broker_id: u32,
}
impl ControllerEvent for BrokerModification {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_BROKER_MODIFICATION
    }
}

pub struct BrokerChange {}
impl ControllerEvent for BrokerChange {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_BROKER_CHANGE
    }
}

pub struct ControllerChange {}
impl ControllerEvent for ControllerChange {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_CONTROLLER_CHANGE
    }
}

pub struct ReElect {}
impl ControllerEvent for ReElect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_RE_ELECT
    }
}
