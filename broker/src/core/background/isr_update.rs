use std::sync::Arc;

use tokio::{task::JoinHandle, time};

use crate::{
    core::{config::ISR_INTERVAL, partition_manager::PartitionManager},
    zk::zk_client::KafkaZkClient,
};

pub fn isr_update_task(
    partition_manager: Arc<PartitionManager>,
    zk_client: Arc<KafkaZkClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(ISR_INTERVAL));

        loop {
            interval.tick().await;

            let partition_states = partition_manager.get_all_leaders();
            for partition_state in partition_states {
                if partition_state.update_isr() {
                    // TODO: Create isr_change_notification
                }
            }
        }
    })
}
