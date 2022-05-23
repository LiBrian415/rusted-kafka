use std::sync::Arc;

use crate::zk::zk_client::KafkaZkClient;

use super::log_manager::LogManager;

pub struct ReplicaFetcherManager {
    zkClient: Arc<KafkaZkClient>,
    logManager: Arc<LogManager>,
}

impl ReplicaFetcherManager {
    pub fn init(zkClient: Arc<KafkaZkClient>, logManager: Arc<LogManager>) -> Self {
        return Self {
            zkClient,
            logManager,
        };
    }

    pub fn fetch(path: String) {
        // TODO call createFetcherThread
    }

    fn createFetcherThread(&self) {
        // TODO create fetcher thread (consumer client) to leader

        // 0) TODO: break path into topic, partition
        // 1) TODO: get leader
        // 2) TODO: fetch from leader
        // 3) call log manager to append log
        // 4) repeat 2 if still data
        // 5) re-set zk watch
    }
}
