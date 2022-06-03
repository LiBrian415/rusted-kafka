use crate::common::topic_partition::TopicPartition;
use std::{any::Any, collections::HashSet, sync::mpsc::SyncSender};

// https://stackoverflow.com/questions/33687447/how-to-get-a-reference-to-a-concrete-type-from-a-trait-object
use super::constants::{
    EVENT_BROKER_CHANGE, EVENT_BROKER_MODIFICATION, EVENT_CONTROLLER_CHANGE,
    EVENT_ISR_CHANGE_NOTIFICATION, EVENT_REGISTER_BROKER_AND_REELECT,
    EVENT_REPLICA_LEADER_ELECTION, EVENT_RE_ELECT, EVENT_STARTUP, EVENT_TOPIC_CHANGE,
};

pub trait ControllerEvent: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn state(&self) -> u32;
    fn complete(&self) -> ();
}

pub struct TopicChange {}
impl ControllerEvent for TopicChange {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_TOPIC_CHANGE
    }

    fn complete(&self) -> () {
        todo!();
    }
}

pub struct Startup {
    pub tx: SyncSender<()>,
}
impl ControllerEvent for Startup {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_STARTUP
    }

    fn complete(&self) -> () {
        self.tx.send(());
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

    fn complete(&self) -> () {
        todo!();
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

    fn complete(&self) -> () {
        todo!();
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

    fn complete(&self) -> () {
        todo!();
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

    fn complete(&self) -> () {
        todo!();
    }
}

pub struct ReplicaLeaderElection {
    pub partitions: HashSet<TopicPartition>,
}
impl ControllerEvent for ReplicaLeaderElection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_REPLICA_LEADER_ELECTION
    }

    fn complete(&self) -> () {
        todo!();
    }
}

pub struct RegisterBrokerAndReElect {}
impl ControllerEvent for RegisterBrokerAndReElect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_REGISTER_BROKER_AND_REELECT
    }

    fn complete(&self) -> () {
        todo!();
    }
}

pub struct IsrChangeNotification {}
impl ControllerEvent for IsrChangeNotification {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state(&self) -> u32 {
        EVENT_ISR_CHANGE_NOTIFICATION
    }

    fn complete(&self) -> () {
        todo!();
    }
}
