// Tests for the BinStorage
use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
    }, error::Error,
};
use std::time::Duration;

use broker::{core::{kafka_server::KafkaServer, kafka_client::KafkaClient}, broker::ConsumerOutput};
use tonic::Streaming;

#[allow(unused_imports)]

const DEFAULT_ADDR: &str = "localhost";
const DEFAULT_PORT: &str = "3000";

fn serialize(message: &str) -> Vec<u8> {
    let mut serialized = Vec::new();
    serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
    serialized.append(&mut String::into_bytes(message.to_string()));
    serialized
}

// fn deserialize(messages: Vec<u8>) -> Vec<String> {
//     let mut deserialized = Vec::new();
//     let mut i = 0;

//     while i < messages.len() {
//         let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
//         let str = String::from_utf8(messages[(i + 4)..(i + 4 + len as usize)].to_vec()).unwrap();
//         deserialized.push(str);
//         i += 4 + len as usize;
//     }

//     deserialized
// }

fn deserialize(messages: Vec<u8>) -> (String, usize) {
    let len = u32::from_be_bytes(messages[0..4].try_into().unwrap()) as usize + 4;
    let l = std::cmp::min(len, messages.len());
    let mut padded:Vec<u8> = vec![0; 4];
    padded.append(&mut messages[4..l].to_vec());
    (String::from_utf8(padded).unwrap(), len)
}

async fn get_messages(mut iter: Streaming<ConsumerOutput>) -> Result<Vec<String>, Box<(dyn Error + Send + Sync)>> {
    let mut res = Vec::new();
    if let Some(output) = &iter.message().await? {
        let mut messages = output.messages.clone();
        while messages.len() > 0 {
            if messages.len() > 0 && messages.len() < 4 {
                let output = iter.message().await?;
                assert!(output.is_some());
                let output = output.unwrap();
                messages.append(&mut output.messages.clone());
            } else {
                let (m, l) = deserialize(messages.clone());
                if m.len() < l {
                    let output = iter.message().await?;
                    assert!(output.is_some());
                    let output = output.unwrap();
                    messages.append(&mut output.messages.clone());
                } else {
                    res.push(m.clone());
                    messages = messages[m.len()..].to_vec();
                }
            }
        }
    }
    Ok(res)
}

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

    let message = "Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World!";
    let message = &message[..128]; // 127 is max for utf8 conversion?
    produce_client.produce("greeting".to_owned(), 0, serialize(message)).await?;

    match consume_client.consume("greeting".to_owned(), 0, 0, 64).await {
        Ok(iter) => {
            let messages = get_messages(iter).await?;
            messages.iter().for_each(|message| println!("res = {:?} {:?}", message, message.len()));
        }
        Err(e) => {
            eprintln!("{:?}", e);
        }
    }

    shut_tx.send(()).await?;
    server_handle.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(unused_variables)]
async fn simple_test2() -> Result<(), Box<(dyn Error + Send + Sync)>> {
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
    
    let message1 = "produce 1";
    produce_client.produce("greeting".to_owned(), 0, serialize(message1)).await?;

    let message2 = "produce 2";
    produce_client.produce("greeting".to_owned(), 0, serialize(message2)).await?;

    match consume_client.consume("greeting".to_owned(), 0, 0, 128).await {
        Ok(iter) => {
            let messages = get_messages(iter).await?;
            messages.iter().for_each(|message| println!("res = {:?} {:?}", message, message.len()));
        }
        Err(e) => {
            eprintln!("{:?}", e);
        }
    }

    shut_tx.send(()).await?;
    server_handle.await?;

    Ok(())
}