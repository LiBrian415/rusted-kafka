use std::sync::Arc;

use tokio::{task::JoinHandle, time};

use crate::{
    core::{ack_manager::AckManager, config::ISR_INTERVAL, partition_manager::PartitionManager},
    zk::zk_client::KafkaZkClient,
};

pub fn isr_update_task(
    broker_id: u32,
    partition_manager: Arc<PartitionManager>,
    ack_manager: Arc<AckManager>,
    zk_client: Arc<KafkaZkClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(ISR_INTERVAL));

        loop {
            interval.tick().await;

            let partition_states = partition_manager.get_all_leaders();
            for (topic_partition, partition_state) in partition_states {
                if partition_state.update_isr() {
                    // TODO: Create isr_change_notification

                    // isr changed so isr acks might've also changed
                    let min_isr_ack = partition_state.get_isr_ack();
                    let max_isr_ack = partition_state.get_max_isr_ack();
                    if let Some(ack_handler) = ack_manager.get_handler(&topic_partition) {
                        ack_handler.notify(min_isr_ack, max_isr_ack)
                    }
                }
            }
        }
    })
}
