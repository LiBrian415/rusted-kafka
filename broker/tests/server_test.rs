// Tests for the BinStorage
use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
    }, thread, error::Error,
};
use std::time::Duration;

use broker::core::{kafka_server::KafkaServer, kafka_client::KafkaClient};
use tokio::{task::JoinHandle};

#[allow(unused_imports)]

const DEFAULT_ADDR: &str = "localhost";
const DEFAULT_PORT: &str = "3000";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(unused_variables)]
async fn simple_test() -> Result<(), Box<(dyn Error + Send + Sync)>> {
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);

    let server_handle = tokio::spawn(KafkaServer::startup(
        vec![format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)],
        0,
        Some(tx),
        Some(shut_rx)
    ));
    assert_eq!(true, rx.recv_timeout(Duration::from_secs(2)).unwrap());

    let produce_client = KafkaClient::new(DEFAULT_ADDR.to_owned(), DEFAULT_PORT.to_owned());
    let consume_client = KafkaClient::new(DEFAULT_ADDR.to_owned(), DEFAULT_PORT.to_owned());

    produce_client.create(vec![("greeting".to_owned(), 1)]).await?;
    println!("created");
    produce_client.produce("greeting".to_owned(), 0, "Hello, World!".as_bytes().to_vec()).await?;

    // let consume_handle = tokio::spawn(async move {
    //     match consume_client.consume("greeting".to_owned(), 0, 0).await {
    //         Ok(mut iter) => {
    //             match iter.message().await {
    //                 Ok(message) => {
    //                     if let Some(res) = &message {
    //                         println!("res = {:?} {:?}", res.messages, res.end);
    //                     }
    //                 }
    //                 Err(e) => {
    //                     eprintln!("{:?}", e);
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             eprintln!("{:?}", e);
    //         }
    //     }
    // });

    // consume_handle.await?;

    shut_tx.send(()).await?;
    server_handle.await?;

    Ok(())
}