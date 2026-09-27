//! What clients send and what the gateway answers.
//!
//! **Text frames** are JSON publishes:
//!
//! ```json
//! {"id": 7, "topic": "orders", "key": "cart-42", "value": "{...}",
//!  "headers": {"trace": "abc"}}
//! ```
//!
//! Every field but the value is optional. `value_b64` carries binary
//! payloads; `"value": null` is a tombstone. With no `key` the record is
//! keyed by the connection's key (`?key=` at connect) or else by the
//! authenticated user, so one user's stream stays in one partition and in
//! order. Clients never choose a partition: the key does.
//!
//! **Binary frames** are the whole value, published to the connection's
//! topic under its key: the cheap path for high-rate telemetry.
//!
//! **Subscriptions** are text frames with an `op`:
//!
//! ```json
//! {"op": "subscribe", "id": 1, "topic": "prices", "keys": ["AAPL"], "snapshot": true}
//! {"op": "unsubscribe", "id": 2, "topic": "prices"}
//! ```
//!
//! A message with no `op` (or `"op": "publish"`) is a publish, so clients
//! written before subscriptions existed work unchanged.
//!
//! The gateway answers with JSON text frames: `welcome` once, `ack` per
//! message that carried an `id` (binary frames are numbered 1, 2, 3...),
//! `error`, and for subscriptions `subscribed`, `record`, `lagged` and
//! `unsubscribed`.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize};

/// Headers the gateway itself sets. Clients may not send them, so a
/// consumer can trust `x-gw-user` to be the authenticated subject.
pub const RESERVED_HEADER_PREFIX: &str = "x-gw-";
pub const USER_HEADER: &str = "x-gw-user";

const MAX_TOPIC_LEN: usize = 249;
const MAX_HEADERS: usize = 64;

/// Every field any client frame may carry; which ones are allowed depends
/// on `op`. One flat struct parses in one pass and still refuses fields
/// that belong to no operation.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ClientFrame {
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub key_b64: Option<String>,
    /// Absent, `null` (tombstone) or a string: the double Option keeps
    /// "not sent" and "sent as null" apart.
    #[serde(default, deserialize_with = "present")]
    pub value: Option<Option<String>>,
    #[serde(default)]
    pub value_b64: Option<String>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, Option<String>>>,
    /// Subscribe: only records with one of these keys.
    #[serde(default)]
    pub keys: Option<Vec<String>>,
    /// Subscribe: send the latest cached record of each (matching) key
    /// before the live stream.
    #[serde(default)]
    pub snapshot: Option<bool>,
}

/// A parsed client frame.
#[derive(Debug)]
pub enum Request {
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe { id: Option<u64>, topic: String },
}

#[derive(Debug)]
pub struct Subscribe {
    pub id: Option<u64>,
    pub topic: String,
    pub keys: Option<Vec<Bytes>>,
    pub snapshot: bool,
}

fn present<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

/// A publish after validation: exactly what goes to the producer.
#[derive(Debug)]
pub struct Publish {
    pub id: Option<u64>,
    pub topic: Option<String>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    TopicNotAllowed,
    RateLimited,
    Overloaded,
    BrokerError,
    ShuttingDown,
    TooManySubscriptions,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "BAD_REQUEST",
            ErrorCode::TopicNotAllowed => "TOPIC_NOT_ALLOWED",
            ErrorCode::RateLimited => "RATE_LIMITED",
            ErrorCode::Overloaded => "OVERLOADED",
            ErrorCode::BrokerError => "BROKER_ERROR",
            ErrorCode::ShuttingDown => "SHUTTING_DOWN",
            ErrorCode::TooManySubscriptions => "TOO_MANY_SUBSCRIPTIONS",
        }
    }

    /// Whether the same message may succeed if simply sent again later.
    /// Broker errors decide case by case (see `gateway::classify`).
    pub fn retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::RateLimited | ErrorCode::Overloaded | ErrorCode::ShuttingDown
        )
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerFrame<'a> {
    Welcome {
        user: &'a str,
        topic: Option<&'a str>,
        key: &'a str,
        max_message_bytes: usize,
        max_inflight: usize,
        /// Whether this connection may subscribe to anything at all.
        subscribe: bool,
    },
    Subscribed {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        topic: &'a str,
        /// How many snapshot records follow before the live stream.
        snapshot: usize,
    },
    Unsubscribed {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        topic: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<&'a str>,
    },
    /// This subscriber fell behind the feed and skipped `skipped` records.
    Lagged { topic: &'a str, skipped: u64 },
    Ack {
        id: u64,
        topic: &'a str,
        partition: i32,
        offset: i64,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        code: &'static str,
        message: &'a str,
        retryable: bool,
    },
}

impl ServerFrame<'_> {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("server frames always serialize")
    }
}

pub fn parse_request(text: &str) -> Result<Request, String> {
    let frame: ClientFrame =
        serde_json::from_str(text).map_err(|e| format!("invalid JSON frame: {e}"))?;
    match frame.op.as_deref() {
        None | Some("publish") => {
            if frame.keys.is_some() || frame.snapshot.is_some() {
                return Err("keys and snapshot belong to subscribe, not publish".into());
            }
            parse_publish(frame).map(Request::Publish)
        }
        Some(op @ ("subscribe" | "unsubscribe")) => {
            if frame.key.is_some()
                || frame.key_b64.is_some()
                || frame.value.is_some()
                || frame.value_b64.is_some()
                || frame.headers.is_some()
            {
                return Err(format!("{op} takes topic, keys and snapshot, not a record"));
            }
            let topic = frame.topic.ok_or_else(|| format!("{op} needs a topic"))?;
            validate_topic_name(&topic)?;
            if op == "unsubscribe" {
                if frame.keys.is_some() || frame.snapshot.is_some() {
                    return Err("unsubscribe takes only id and topic".into());
                }
                return Ok(Request::Unsubscribe {
                    id: frame.id,
                    topic,
                });
            }
            Ok(Request::Subscribe(Subscribe {
                id: frame.id,
                topic,
                keys: frame
                    .keys
                    .map(|keys| keys.into_iter().map(Bytes::from).collect()),
                snapshot: frame.snapshot.unwrap_or(false),
            }))
        }
        Some(other) => Err(format!(
            "unknown op {other:?} (publish, subscribe or unsubscribe)"
        )),
    }
}

/// Parse a frame that must be a publish.
pub fn parse_text(text: &str) -> Result<Publish, String> {
    match parse_request(text)? {
        Request::Publish(publish) => Ok(publish),
        _ => Err("not a publish".into()),
    }
}

fn parse_publish(frame: ClientFrame) -> Result<Publish, String> {
    let key = match (frame.key, frame.key_b64) {
        (Some(_), Some(_)) => return Err("send key or key_b64, not both".into()),
        (Some(k), None) => Some(Bytes::from(k)),
        (None, Some(b)) => Some(Bytes::from(
            STANDARD.decode(b).map_err(|_| "key_b64 is not base64")?,
        )),
        (None, None) => None,
    };
    let value = match (frame.value, frame.value_b64) {
        (Some(_), Some(_)) => return Err("send value or value_b64, not both".into()),
        (Some(v), None) => v.map(Bytes::from),
        (None, Some(b)) => Some(Bytes::from(
            STANDARD.decode(b).map_err(|_| "value_b64 is not base64")?,
        )),
        (None, None) => {
            return Err("a publish needs value (null for a tombstone) or value_b64".into())
        }
    };
    let mut headers = Vec::new();
    if let Some(map) = frame.headers {
        if map.len() > MAX_HEADERS {
            return Err(format!("at most {MAX_HEADERS} headers"));
        }
        for (name, value) in map {
            if name
                .to_ascii_lowercase()
                .starts_with(RESERVED_HEADER_PREFIX)
            {
                return Err(format!("header {name:?} is reserved for the gateway"));
            }
            headers.push((name, value.map(Bytes::from)));
        }
    }
    if let Some(topic) = &frame.topic {
        validate_topic_name(topic)?;
    }
    Ok(Publish {
        id: frame.id,
        topic: frame.topic,
        key,
        value,
        headers,
    })
}

pub fn validate_topic_name(topic: &str) -> Result<(), String> {
    if topic.is_empty() || topic.len() > MAX_TOPIC_LEN {
        return Err(format!("topic names are 1..={MAX_TOPIC_LEN} characters"));
    }
    if !topic
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!("topic {topic:?} may only contain [A-Za-z0-9._-]"));
    }
    Ok(())
}

/// Topic allow-list: exact names, `prefix.*` (or any `prefix*`), and `*`.
#[derive(Debug, Clone)]
pub struct TopicPolicy {
    patterns: Vec<String>,
}

impl TopicPolicy {
    pub fn new(patterns: Vec<String>) -> Self {
        TopicPolicy { patterns }
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn allows(&self, topic: &str) -> bool {
        // The broker's own topics (__consumer_offsets and the transaction
        // log) are never writable from the edge, whatever the patterns say.
        !topic.starts_with("__") && self.patterns.iter().any(|p| pattern_matches(p, topic))
    }
}

fn pattern_matches(pattern: &str, topic: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => topic.starts_with(prefix),
        None => pattern == topic,
    }
}

/// Parse `a=b&c=d` query strings with percent-decoding.
pub fn parse_query(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(hi), Some(lo)) => {
                    out.push(hi << 4 | lo);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_parse_and_keep_null_apart_from_absent() {
        let p = parse_text(
            r#"{"id":1,"topic":"t","key":"k","value":"v","headers":{"a":"b","n":null}}"#,
        )
        .unwrap();
        assert_eq!(p.id, Some(1));
        assert_eq!(p.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(p.value.as_deref(), Some(&b"v"[..]));
        assert_eq!(p.headers.len(), 2);
        assert!(p.headers.iter().any(|(k, v)| k == "n" && v.is_none()));

        let tomb = parse_text(r#"{"key":"k","value":null}"#).unwrap();
        assert!(tomb.value.is_none());
        assert!(
            parse_text(r#"{"key":"k"}"#).is_err(),
            "absent value is not a tombstone"
        );

        let bin = parse_text(r#"{"key_b64":"AAE=","value_b64":"/w=="}"#).unwrap();
        assert_eq!(bin.key.as_deref(), Some(&[0u8, 1][..]));
        assert_eq!(bin.value.as_deref(), Some(&[0xffu8][..]));
    }

    #[test]
    fn clients_cannot_spoof_gateway_headers_or_send_junk() {
        assert!(parse_text(r#"{"value":"v","headers":{"X-GW-User":"admin"}}"#).is_err());
        assert!(
            parse_text(r#"{"value":"v","partition":3}"#).is_err(),
            "partitions come from keys"
        );
        assert!(parse_text(r#"{"value":"v","topic":"bad topic"}"#).is_err());
        assert!(parse_text("not json").is_err());
    }

    #[test]
    fn subscriptions_parse_and_refuse_record_fields() {
        match parse_request(
            r#"{"op":"subscribe","id":3,"topic":"prices","keys":["AAPL","MSFT"],"snapshot":true}"#,
        )
        .unwrap()
        {
            Request::Subscribe(s) => {
                assert_eq!(s.id, Some(3));
                assert_eq!(s.topic, "prices");
                assert_eq!(s.keys.unwrap().len(), 2);
                assert!(s.snapshot);
            }
            other => panic!("{other:?}"),
        }
        match parse_request(r#"{"op":"unsubscribe","topic":"prices"}"#).unwrap() {
            Request::Unsubscribe { id: None, topic } => assert_eq!(topic, "prices"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse_request(r#"{"op":"publish","value":"v"}"#).unwrap(),
            Request::Publish(_)
        ));
        assert!(
            parse_request(r#"{"op":"subscribe"}"#).is_err(),
            "needs a topic"
        );
        assert!(parse_request(r#"{"op":"subscribe","topic":"t","value":"v"}"#).is_err());
        assert!(parse_request(r#"{"op":"unsubscribe","topic":"t","snapshot":true}"#).is_err());
        assert!(parse_request(r#"{"value":"v","keys":["a"]}"#).is_err());
        assert!(parse_request(r#"{"op":"delete","topic":"t"}"#).is_err());
        assert!(parse_request(r#"{"op":"subscribe","topic":"bad topic"}"#).is_err());
    }

    #[test]
    fn topic_policy_matches_patterns_and_guards_internal_topics() {
        let p = TopicPolicy::new(vec!["orders.*".into(), "events".into()]);
        assert!(p.allows("orders.eu"));
        assert!(p.allows("events"));
        assert!(!p.allows("events2"));
        assert!(!p.allows("payments"));
        let all = TopicPolicy::new(vec!["*".into()]);
        assert!(all.allows("anything"));
        assert!(!all.allows("__consumer_offsets"));
    }

    #[test]
    fn query_strings_decode() {
        let q = parse_query("topic=orders.eu&key=user%2F7&access_token=a.b-c_d&x");
        assert_eq!(q["topic"], "orders.eu");
        assert_eq!(q["key"], "user/7");
        assert_eq!(q["access_token"], "a.b-c_d");
        assert_eq!(q["x"], "");
        assert_eq!(parse_query("k=%zz%4")["k"], "%zz%4");
    }
}
