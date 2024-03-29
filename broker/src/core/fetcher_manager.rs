use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use tokio::{sync::Notify, task::JoinHandle, time};

use crate::{
    broker::ConsumerOutput,
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    zk::{
        zk_client::KafkaZkClient, zk_data::TopicPartitionOffsetZNode, zk_watcher::ZkChangeHandler,
    },
};

use super::{
    config::POLL_INTERVAL, err::ReplicaResult, kafka_client::KafkaClient,
    replica_manager::ReplicaManager,
};

pub struct FetcherManager {
    fetcher_threads: Mutex<HashMap<TopicPartition, (JoinHandle<ReplicaResult<()>>, Arc<Watcher>)>>,
    replica_manager: RwLock<Option<Arc<ReplicaManager>>>,
    zk_client: Arc<KafkaZkClient>,
    kafka_clients: Mutex<HashMap<BrokerInfo, Arc<KafkaClient>>>,
}

impl FetcherManager {
    pub fn init(zk_client: Arc<KafkaZkClient>) -> FetcherManager {
        FetcherManager {
            fetcher_threads: Mutex::new(HashMap::new()),
            replica_manager: RwLock::new(None),
            zk_client,
            kafka_clients: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_replica_manager(&self, replica_manager: Arc<ReplicaManager>) {
        let mut w = self.replica_manager.write().unwrap();
        (*w) = Some(replica_manager);
    }

    fn get_replica_manager(&self) -> Arc<ReplicaManager> {
        let r = self.replica_manager.read().unwrap();
        (*r).as_ref().unwrap().clone()
    }

    pub fn create_fetcher_thread(&self, topic_partition: &TopicPartition, offset: u64) {
        let mut g = self.fetcher_threads.lock().unwrap();
        (*g).insert(
            topic_partition.clone(),
            self._create_fetcher_thread(topic_partition, offset)
                .unwrap(),
        );
    }

    pub fn _create_fetcher_thread(
        &self,
        topic_partition: &TopicPartition,
        offset: u64,
    ) -> ReplicaResult<(JoinHandle<ReplicaResult<()>>, Arc<Watcher>)> {
        println!("-----CREATE FETCHER THREAD-----");
        println!("topic_partition: {:?}", topic_partition);
        println!();

        let notify = Arc::new(Notify::new());
        let replica_manager = self.get_replica_manager();
        let zk_client = self.zk_client.clone();
        let topic_partition = topic_partition.clone();
        let mut offset = offset;

        // Register Watcher
        let watcher = Arc::new(Watcher::init(topic_partition.clone(), notify.clone()));
        self.zk_client
            .register_znode_change_handler(watcher.clone());

        // Get Client for leader
        let leader_info = replica_manager.get_leader_info(&topic_partition)?;
        let client = self.get_kafka_client(&leader_info);

        // Start background fetch task
        let task = tokio::spawn(async move {
            let mut interval = time::interval(time::Duration::from_millis(POLL_INTERVAL));

            loop {
                // Wait for either next interval or notification
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = notify.notified() => {
                        println!("-----TOPIC NOTIFICATION-----");
                        println!("topic_partition: {:?}", topic_partition);
                        println!();
                    }
                };

                // fetch and append loop
                let mut iter = client
                    .consume(
                        topic_partition.topic.clone(),
                        topic_partition.partition,
                        offset,
                        127,
                    )
                    .await?;
                while let Some(res) = iter.message().await? {
                    let ConsumerOutput {
                        messages,
                        high_watermark,
                    } = res;
                    // 3) call log manager to append log
                    offset = replica_manager.append_local(&topic_partition, messages)?;
                    // replica_manager.checkpoint_high_watermark(&topic_partition, watermark)?;
                }

                // Rewatch (Zk only sets one watcher even w/ multiple gets so there's no
                // need to clear it)
                let _ = zk_client.get_topic_partition_offset(
                    &topic_partition.topic,
                    topic_partition.partition,
                )?;
            }
        });

        Ok((task, watcher.clone()))
    }

    pub fn get_kafka_client(&self, broker_info: &BrokerInfo) -> Arc<KafkaClient> {
        let mut g = self.kafka_clients.lock().unwrap();
        if let Some(client) = (*g).get(broker_info) {
            client.clone()
        } else {
            let client = Arc::new(KafkaClient::new(
                broker_info.hostname.clone(),
                broker_info.port.clone(),
            ));
            (*g).insert(broker_info.clone(), client.clone());
            client
        }
    }

    /// Removes and aborts the task and unregister change handlers
    pub fn delete_fetcher_thread(&self, topic_partition: &TopicPartition) {
        {
            let mut g = self.fetcher_threads.lock().unwrap();
            if let Some((task, watcher)) = (*g).remove(topic_partition) {
                task.abort();
                self.zk_client
                    .unregister_znode_change_handler(&watcher.path());
            }
        }
    }

    pub fn shutdown(&self) {
        println!("-----ABORT FETCHER THREAD-----");
        let g = self.fetcher_threads.lock().unwrap();
        (*g).iter().for_each(|(k, v)| {
            println!("topic_partition: {:?}", k);
            let (t, w) = v;
            t.abort();
            self.zk_client.unregister_znode_change_handler(&w.path());
        });
        println!();
    }
}

pub struct Watcher {
    topic_partition: TopicPartition,
    notify: Arc<Notify>,
}

impl Watcher {
    pub fn init(topic_partition: TopicPartition, notify: Arc<Notify>) -> Watcher {
        Watcher {
            topic_partition,
            notify,
        }
    }
}

impl ZkChangeHandler for Watcher {
    fn path(&self) -> String {
        TopicPartitionOffsetZNode::path(&self.topic_partition.topic, self.topic_partition.partition)
    }

    fn handle_create(&self) {
        self.notify.notify_one();
    }

    fn handle_delete(&self) {
        self.notify.notify_one();
    }

    fn handle_data_change(&self) {
        self.notify.notify_one();
    }
}
