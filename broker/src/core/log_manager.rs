use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use crate::common::topic_partition::TopicPartition;

const DEFAULT_SEGMENT_SIZE: usize = 1024 * 1024;

/// Simple Log which can be visualized as a linkedlist of
/// segments, each containing 1 or more messages. The
/// high_watermark can be seen as a checkpoint. Also, note that
/// the log manager isn't responsible for updating the high_watermark,
/// just maintaining it.
///
/// lib - If we include GC of logs, then fetch messages should also
/// return the first offset. This is necessary for backfill events of
/// newly added replicas.
pub struct Log {
    segments: RwLock<VecDeque<LogSegment>>,
    high_watermark: RwLock<u128>,
    segment_size: usize,
}

impl Log {
    pub fn init() -> Log {
        Log {
            segments: RwLock::new(VecDeque::new()),
            high_watermark: RwLock::new(0),
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }

    pub fn init_test(segment_size: usize) -> Log {
        Log {
            segments: RwLock::new(VecDeque::new()),
            high_watermark: RwLock::new(0),
            segment_size,
        }
    }

    /// Fetch all messages >= start.
    /// Also, if the request was issued by a client, then also restrict the
    /// messages to the set of committed messages
    pub fn fetch_messages(
        &self,
        start: u128,
        fetch_max_bytes: u128,
        issued_by_client: bool,
    ) -> Vec<u8> {
        // Check if the log has available entries
        let upper_bound;
        if issued_by_client {
            let r = self.high_watermark.read().unwrap();
            upper_bound = *r;
        } else {
            upper_bound = u128::max_value();
        }

        let mut i = 0;

        let r = self.segments.read().unwrap();

        // Find the starting segment
        while (i + 1) < (*r).len() && (*r)[i + 1].get_byte_offset() < start {
            i += 1;
        }

        // Fetch the actual messages
        let mut messages = Vec::new();
        let mut remaining_space = fetch_max_bytes;

        while i < (*r).len() {
            let (mut msgs, reached_bound) =
                (*r)[i].fetch_messages(start, remaining_space, upper_bound);

            messages.append(&mut msgs);

            remaining_space = fetch_max_bytes - messages.len() as u128;
            i += 1;

            if reached_bound {
                break;
            }
        }

        messages
    }

    /// Appends the sequence of messages into the log and returns the
    /// log end offset
    pub fn append_messages(&self, mut messages: Vec<u8>) -> u128 {
        let mut w = self.segments.write().unwrap();
        let mut prev_end: u128 = 0;

        while messages.len() > 0 {
            if let Some(segment) = (*w).back() {
                messages = segment.append_messages(messages);
                prev_end = segment.byte_offset + segment.get_len() as u128;
            }

            if messages.len() > 0 {
                (*w).push_back(LogSegment::init(prev_end, self.segment_size));
            }
        }

        prev_end
    }

    pub fn checkpoint_high_watermark(&self, high_watermark: u128) {
        let mut w = self.high_watermark.write().unwrap();
        if *w < high_watermark {
            *w = high_watermark;
        }
    }
}

pub struct LogSegment {
    data: RwLock<Vec<u8>>,
    byte_offset: u128,
    segment_size: usize,
}

impl LogSegment {
    pub fn init(byte_offset: u128, segment_size: usize) -> LogSegment {
        LogSegment {
            data: RwLock::new(Vec::new()),
            byte_offset,
            segment_size,
        }
    }

    pub fn get_byte_offset(&self) -> u128 {
        self.byte_offset
    }

    pub fn get_len(&self) -> usize {
        let r = self.data.read().unwrap();
        (*r).len()
    }

    pub fn fetch_messages(
        &self,
        start: u128,
        fetch_max_bytes: u128,
        upper_limit: u128,
    ) -> (Vec<u8>, bool) {
        let offset = self.byte_offset;
        let mut i: usize = 0;
        let r = self.data.read().unwrap();

        // Find first message
        while i < (*r).len() && (offset + i as u128) < start {
            let len = u32::from_be_bytes((*r)[i..(i + 4)].try_into().unwrap());
            i += 4 + len as usize;
        }

        let mut messages = Vec::new();
        let mut reached_bound = false;

        // Add messages that are less than the upper_limit
        // Also, ensure that we don't fetch more than we can
        while i < (*r).len() {
            if (offset + i as u128) >= upper_limit {
                break;
            }
            let len = u32::from_be_bytes((*r)[i..(i + 4)].try_into().unwrap());
            if (4 + len as usize + messages.len()) as u128 > fetch_max_bytes {
                reached_bound = true;
                break;
            }
            messages.extend_from_slice(&(*r)[i..(i + 4 + len as usize)]);
            i += 4 + len as usize;
        }

        (messages, reached_bound)
    }

    /// Try to append as many messages as possible into the segment w/o
    /// overflowing.
    ///
    /// Returns the remaining messages that weren't able to fit.
    pub fn append_messages(&self, messages: Vec<u8>) -> Vec<u8> {
        let mut i: usize = 0;
        let mut w = self.data.write().unwrap();

        // Append messages into the log segment, stopping when the segment can't
        // hold the next message
        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            if (4 + len as usize + (*w).len()) > self.segment_size {
                break;
            }
            (*w).extend_from_slice(&messages[i..(i + 4 + len as usize)]);
            i += 4 + len as usize;
        }

        // Return remaining messages if necessary
        if i < messages.len() {
            messages[i..].to_vec()
        } else {
            Vec::new()
        }
    }
}

/// LogManager of in-memory, segmented logs.
pub struct LogManager {
    data: RwLock<HashMap<TopicPartition, Arc<Log>>>,
}

impl LogManager {
    pub fn init() -> LogManager {
        LogManager {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_log(&self, topic_partition: &TopicPartition) -> Option<Arc<Log>> {
        let r = self.data.read().unwrap();
        match (*r).get(topic_partition) {
            Some(log) => Some(log.clone()),
            None => None,
        }
    }

    pub fn add_log(&self, topic_partition: &TopicPartition) {
        let mut w = self.data.write().unwrap();
        if !(*w).contains_key(topic_partition) {
            (*w).insert(topic_partition.clone(), Arc::new(Log::init()));
        }
    }
}

#[cfg(test)]
mod log_segment_tests {
    use super::{LogSegment, DEFAULT_SEGMENT_SIZE};

    fn serialize(message: &str) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
        serialized.append(&mut String::into_bytes(message.to_string()));
        serialized
    }

    fn construct_message_set(messages: Vec<&str>) -> Vec<u8> {
        let mut message_set = Vec::new();
        for message in messages {
            message_set.append(&mut serialize(message));
        }
        message_set
    }

    fn deserialize(messages: Vec<u8>) -> Vec<String> {
        let mut deserialized = Vec::new();
        let mut i = 0;

        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            let str =
                String::from_utf8(messages[(i + 4)..(i + 4 + len as usize)].to_vec()).unwrap();
            deserialized.push(str);
            i += 4 + len as usize;
        }

        deserialized
    }

    fn assert_valid_append(
        remaining: &Vec<u8>,
        segment: &LogSegment,
        expected_remaining_len: usize,
        expected_data_len: usize,
    ) {
        assert_eq!(remaining.len(), expected_remaining_len);

        let r = segment.data.read().unwrap();
        assert_eq!((*r).len(), expected_data_len);
    }

    #[test]
    fn append() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set);
        assert_valid_append(&remaining, &segment, 0, 16);

        let message_set_1 = construct_message_set(vec!["msg3", "msg4"]);
        let remaining = segment.append_messages(message_set_1);
        assert_valid_append(&remaining, &segment, 0, 32);
    }

    #[test]
    fn append_full() {
        let segment = LogSegment::init(0, 0);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set);
        assert_valid_append(&remaining, &segment, 16, 0);
    }

    #[test]
    fn append_partial() {
        let segment = LogSegment::init(0, 10);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set);
        assert_valid_append(&remaining, &segment, 8, 8);
    }

    #[test]
    fn fetch() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set);

        let (bytes, reached_max) = segment.fetch_messages(0, u128::max_value(), u128::max_value());

        let expected: Vec<String> = ["msg1", "msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);

        // Make sure that fetching multiple times has no side-effects
        let (bytes, reached_max) = segment.fetch_messages(0, u128::max_value(), u128::max_value());

        let expected: Vec<String> = ["msg1", "msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_start() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set);

        let (bytes, reached_max) = segment.fetch_messages(8, u128::max_value(), u128::max_value());

        let expected: Vec<String> = ["msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_max() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set);

        let (bytes, reached_max) = segment.fetch_messages(0, 8, u128::max_value());

        let expected: Vec<String> = ["msg1"].iter().map(|&s| s.into()).collect();
        assert!(reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_upper() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set);

        let (bytes, reached_max) = segment.fetch_messages(0, u128::max_value(), 8);

        let expected: Vec<String> = ["msg1"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }
}

#[cfg(test)]
mod log_tests {
    use super::Log;

    fn serialize(message: &str) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
        serialized.append(&mut String::into_bytes(message.to_string()));
        serialized
    }

    fn construct_message_set(messages: Vec<&str>) -> Vec<u8> {
        let mut message_set = Vec::new();
        for message in messages {
            message_set.append(&mut serialize(message));
        }
        message_set
    }

    fn deserialize(messages: Vec<u8>) -> Vec<String> {
        let mut deserialized = Vec::new();
        let mut i = 0;

        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            let str =
                String::from_utf8(messages[(i + 4)..(i + 4 + len as usize)].to_vec()).unwrap();
            deserialized.push(str);
            i += 4 + len as usize;
        }

        deserialized
    }

    fn assert_valid_append(log: &Log, expected_segments: usize, expected_dist: Vec<Vec<String>>) {
        let r = log.segments.read().unwrap();
        assert_eq!((*r).len(), expected_segments);
        for i in 0..expected_segments {
            let rs = (*r)[i].data.read().unwrap();
            let messages = deserialize((*rs).clone());
            assert_eq!(messages, expected_dist[i]);
        }
    }

    #[test]
    fn append_single() {
        let log = Log::init();
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        log.append_messages(message_set);

        let expected: Vec<Vec<String>> = vec![["msg1", "msg2"].iter().map(|&s| s.into()).collect()];
        assert_valid_append(&log, 1, expected);
    }

    #[test]
    fn append_multiple() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        log.append_messages(message_set);

        let expected: Vec<Vec<String>> = vec![
            ["msg1"].iter().map(|&s| s.into()).collect(),
            ["msg2"].iter().map(|&s| s.into()).collect(),
        ];
        assert_valid_append(&log, 2, expected);
    }

    #[test]
    fn fetch_single() {
        let log = Log::init();
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u128::max_value(), false);

        let expected: Vec<String> = ["msg1", "msg2", "msg3", "msg4"]
            .iter()
            .map(|&s| s.into())
            .collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_multiple() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u128::max_value(), false);

        let expected: Vec<String> = ["msg1", "msg2", "msg3", "msg4"]
            .iter()
            .map(|&s| s.into())
            .collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_start() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(16, u128::max_value(), false);

        let expected: Vec<String> = ["msg3", "msg4"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_max() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(16, 8, false);

        let expected: Vec<String> = ["msg3"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_client_watermark() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u128::max_value(), true);

        let expected: Vec<String> = Vec::new();
        assert_eq!(deserialize(bytes), expected);

        log.checkpoint_high_watermark(24);
        let bytes = log.fetch_messages(0, u128::max_value(), true);

        let expected: Vec<String> = ["msg1", "msg2", "msg3"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }
}
