use std::{
    sync::{
        mpsc::{self, Receiver, Sender}, Arc,
    },
    time::Duration, error::Error,
};

use tokio::task::JoinHandle;
#[allow(unused_imports)]
use broker::{core::{kafka_server::KafkaServer, kafka_client::KafkaClient}, broker::ConsumerOutput};
use tonic::Streaming;

struct ServerTester {
    addrs: Vec<String>,
    this: usize,
    shut_tx: Option<tokio::sync::mpsc::Sender<()>>,
    server: Option<tokio::task::JoinHandle<Result<(), Box<(dyn Error + Send + Sync)>>>>
}

impl ServerTester {
    pub fn new(addrs: Vec<String>, this: usize) -> Self {
        Self {
            addrs,
            this,
            shut_tx: None,
            server: None
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        match self.shut_tx {
            Some(_) => Err(Box::<dyn Error + Send + Sync>::from("Server already started.")),
            None => {
                let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
                let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);

                let server = tokio::spawn(KafkaServer::startup(
                    self.addrs.clone(),
                    // vec![format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)],
                    self.this,
                    Some(tx),
                    Some(shut_rx)
                ));
                assert_eq!(true, rx.recv_timeout(Duration::from_secs(2)).unwrap());

                self.shut_tx = Some(shut_tx);
                self.server = Some(server);

                Ok(())
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<(dyn Error + Send + Sync)>> {
        match &self.shut_tx {
            Some(_) => {
                self.shut_tx.take().map(|shut_tx| async move {
                    shut_tx.send(()).await;
                });
                self.server.take().map(|server| async move {
                    server.await;
                });
                Ok(())
            },
            None => Err(Box::<dyn Error + Send + Sync>::from("Server not started."))
        }
    }
}

async fn spawn_multi_server_testers(start_port: usize, count: usize) -> Vec<ServerTester> {
    let mut addrs = Vec::new();
    for c in 0..count {
        addrs.push(format!("localhost:{}", start_port + c));
    }

    let mut server_testers = Vec::new();
    for c in 0..count {
        let mut server_tester = ServerTester::new(addrs.clone(), c);
        assert!(!server_tester.start().await.is_err());
        server_testers.push(server_tester);
    }
    server_testers
}

// async fn spawn_multi_clients(addrs: Vec<String>) -> Vec<Box<dyn Storage>> {
//     let mut clients = Vec::new();
//     for addr in addrs {
//         clients.push(lab1::new_client(format!("http://{}", addr).as_str()).await.unwrap());
//     }
//     return clients;
// }

async fn join<T>(handles: Vec<JoinHandle<T>>) -> Vec<T> {
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("Join Error"));
    }
    results
}

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

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_tester_shutdown() -> Result<(), Box<(dyn Error + Send + Sync)>> {
//     let mut server_testers = spawn_multi_server_testers(3000, 1).await;

//     assert!(server_testers[0].start().await.is_err());

//     assert!(!server_testers[0].shutdown().await.is_err());
//     assert!(server_testers[0].shutdown().await.is_err());

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_tester_start() -> Result<(), Box<(dyn Error + Send + Sync)>> {
//     let mut server_testers = spawn_multi_server_testers(3000, 1).await;

//     assert!(!server_testers[0].shutdown().await.is_err());
//     assert!(!server_testers[0].start().await.is_err());
//     assert!(!server_testers[0].shutdown().await.is_err());

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_multi_simple() -> Result<(), Box<(dyn Error + Send + Sync)>> {
//     let mut server_testers = spawn_multi_server_testers(3000, 1).await;
//     tokio::time::sleep(Duration::from_secs(1)).await;

//     let produce_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());
//     let consume_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());

//     produce_client.create("greeting".to_owned(), 1, 2).await?;

//     let message = "Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World! Hello, World!";
//     let message = &message[..128]; // 127 is max for utf8 conversion?
//     produce_client.produce("greeting".to_owned(), 0, serialize(message)).await?;

//     match consume_client.consume("greeting".to_owned(), 0, 0, 64).await {
//         Ok(iter) => {
//             let messages = get_messages(iter).await?;
//             messages.iter().for_each(|message| println!("res = {:?} {:?}", message, message.len()));
//         }
//         Err(e) => {
//             eprintln!("{:?}", e);
//         }
//     }

//     assert!(!server_testers[0].shutdown().await.is_err());

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_multi_produce() -> Result<(), Box<(dyn Error + Send + Sync)>> {
//     let mut server_testers = spawn_multi_server_testers(3000, 1).await;
//     tokio::time::sleep(Duration::from_secs(1)).await;

//     let produce_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());
//     let consume_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());

//     produce_client.create("greeting".to_owned(), 1, 2).await?;
    
//     let message1 = "produce 1";
//     produce_client.produce("greeting".to_owned(), 0, serialize(message1)).await?;

//     let message2 = "produce 2";
//     produce_client.produce("greeting".to_owned(), 0, serialize(message2)).await?;

//     match consume_client.consume("greeting".to_owned(), 0, 0, 128).await {
//         Ok(iter) => {
//             let messages = get_messages(iter).await?;
//             messages.iter().for_each(|message| println!("res = {:?} {:?}", message, message.len()));
//         }
//         Err(e) => {
//             eprintln!("{:?}", e);
//         }
//     }

//     assert!(!server_testers[0].shutdown().await.is_err());

//     Ok(())
// }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_fail() -> Result<(), Box<(dyn Error + Send + Sync)>> {
    let mut server_testers = spawn_multi_server_testers(3000, 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let produce_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());
    let consume_client = KafkaClient::new("localhost".to_owned(), "3000".to_owned());

    produce_client.create("greeting".to_owned(), 1, 2).await?;
    
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

    assert!(!server_testers[0].shutdown().await.is_err());

    Ok(())
}