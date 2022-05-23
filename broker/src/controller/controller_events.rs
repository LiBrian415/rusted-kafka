use super::constants::{EVENT_TOPIC_CHNAGE, EVENT_RE_ELECT, EVENT_CONTROLLER_CHANGE, EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_STARTUP};

pub trait ControllerEvent: Send + Sync {
    fn state(&self) -> u32;
}

pub struct TopicChange {}
impl ControllerEvent for TopicChange {
    fn state(&self) -> u32 {
        EVENT_TOPIC_CHNAGE
    }
}

pub struct Startup {}
impl ControllerEvent for Startup {
    fn state(&self) -> u32 {
        EVENT_STARTUP
    }
}

pub struct BrokerModification {}
impl ControllerEvent for BrokerModification {
    fn state(&self) -> u32 {
        EVENT_BROKER_MODIFICATION
    }
}

pub struct BrokerChange {}
impl ControllerEvent for BrokerChange {
    fn state(&self) -> u32 {
        EVENT_BROKER_CHANGE
    }
}

pub struct ControllerChange {}
impl ControllerEvent for ControllerChange {
    fn state(&self) -> u32 {
        EVENT_CONTROLLER_CHANGE
    }
}

pub struct ReElect {}
impl ControllerEvent for ReElect {
    fn state(&self) -> u32 {
        EVENT_RE_ELECT
    }
}