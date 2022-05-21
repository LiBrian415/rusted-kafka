use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
pub struct BrokerInfo {
    pub hostname: String,
    pub port: String
}

impl BrokerInfo {
    pub fn init(hostname: &str, port: &str) -> BrokerInfo {
        BrokerInfo { hostname: hostname.to_string(), port: port.to_string() }
    }
}