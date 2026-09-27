//! Load generator for the WebSocket gateway.
//!
//! Opens many authenticated connections (spread across several source
//! addresses, so one machine is not capped by one address's ~28k ephemeral
//! ports), optionally publishes from each at a steady rate, and reports
//! acknowledgement latency and the gateway's memory per connection, read
//! from its /metrics endpoint.
//!
//! ```text
//! ws-loadgen --url 'ws://127.0.0.1:8090/ws?topic=load' --secret S \
//!     --connections 20000 --ramp 2000 --rate 1 --duration 60 \
//!     --source-ips 127.0.0.2,127.0.0.3 --metrics http://127.0.0.1:8091/metrics
//! ```

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use brahmaputra_ws_gateway::auth::{self, Claims};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Args {
    /// Gateway URL including query, e.g. ws://127.0.0.1:8090/ws?topic=load
    #[arg(long)]
    url: String,
    /// HS256 secret shared with the gateway, used to mint one token per
    /// connection (subjects user-0, user-1, ...).
    #[arg(long)]
    secret: String,
    #[arg(long, default_value_t = 1000)]
    connections: usize,
    /// New connections per second.
    #[arg(long, default_value_t = 1000)]
    ramp: usize,
    /// Messages per second per connection; 0 holds connections idle.
    #[arg(long, default_value_t = 0.0)]
    rate: f64,
    /// Seconds to run once every connection is up.
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// Local addresses to connect from, round-robin (e.g. 127.0.0.2,127.0.0.3).
    #[arg(long, value_delimiter = ',')]
    source_ips: Vec<IpAddr>,
    /// Gateway /metrics URL (http://host:port/metrics) for memory figures.
    #[arg(long)]
    metrics: Option<String>,
}

#[derive(Default)]
struct Stats {
    connected: AtomicU64,
    failed: AtomicU64,
    sent: AtomicU64,
    acked: AtomicU64,
    errors: AtomicU64,
    closed: AtomicU64,
    /// Ack latency, 1 ms buckets up to 10 s.
    latency_ms: Vec<AtomicU64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let request = args.url.as_str().into_client_request()?;
    let host = request.uri().host().context("url host")?.to_owned();
    let port = request.uri().port_u16().unwrap_or(80);
    let target: SocketAddr = tokio::net::lookup_host((host.as_str(), port))
        .await?
        .next()
        .context("resolve gateway")?;
    let stats = Arc::new(Stats {
        latency_ms: (0..=10_000).map(|_| AtomicU64::new(0)).collect(),
        ..Stats::default()
    });
    let rss_before = match &args.metrics {
        Some(url) => gateway_rss(url).await.ok(),
        None => None,
    };
    let payload = "x".repeat(args.payload_bytes);
    let start = Instant::now();
    let stop_at = Arc::new(AtomicU64::new(u64::MAX));
    let mut tasks = Vec::with_capacity(args.connections);
    let gap = Duration::from_secs_f64(1.0 / args.ramp.max(1) as f64);
    let mut next = tokio::time::Instant::now();
    for c in 0..args.connections {
        tokio::time::sleep_until(next).await;
        next += gap;
        let source =
            (!args.source_ips.is_empty()).then(|| args.source_ips[c % args.source_ips.len()]);
        let token = auth::sign_hs256(
            &Claims {
                sub: format!("user-{c}"),
                exp: auth::now_secs() + 86_400,
                nbf: None,
                iat: None,
                iss: None,
                aud: None,
                topics: None,
                subscribe: None,
            },
            None,
            args.secret.as_bytes(),
        );
        tasks.push(tokio::spawn(connection(
            args.url.clone(),
            target,
            source,
            token,
            args.rate,
            payload.clone(),
            stats.clone(),
            start,
            stop_at.clone(),
        )));
        if c % 1000 == 999 {
            eprintln!(
                "{:>6.1}s  opened {:>7}  connected {:>7}  failed {}",
                start.elapsed().as_secs_f64(),
                c + 1,
                stats.connected.load(Relaxed),
                stats.failed.load(Relaxed)
            );
        }
    }
    // Let the last handshakes land.
    let settle = Instant::now();
    while stats.connected.load(Relaxed) + stats.failed.load(Relaxed) < args.connections as u64
        && settle.elapsed() < Duration::from_secs(30)
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let connected = stats.connected.load(Relaxed);
    let ramp_secs = start.elapsed().as_secs_f64();
    eprintln!(
        "ramp done in {ramp_secs:.1}s: {connected} connected, {} failed",
        stats.failed.load(Relaxed)
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rss_connected = match &args.metrics {
        Some(url) => gateway_rss(url).await.ok(),
        None => None,
    };

    let run_start = Instant::now();
    let (sent0, acked0) = (stats.sent.load(Relaxed), stats.acked.load(Relaxed));
    for second in 1..=args.duration {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if second % 5 == 0 || second == args.duration {
            eprintln!(
                "{second:>4}s  open {:>7}  sent {:>9}  acked {:>9}  errors {}  closed {}",
                stats.connected.load(Relaxed) - stats.closed.load(Relaxed),
                stats.sent.load(Relaxed),
                stats.acked.load(Relaxed),
                stats.errors.load(Relaxed),
                stats.closed.load(Relaxed)
            );
        }
    }
    let elapsed = run_start.elapsed().as_secs_f64();
    let sent = stats.sent.load(Relaxed) - sent0;
    let acked = stats.acked.load(Relaxed) - acked0;
    let rss_loaded = match &args.metrics {
        Some(url) => gateway_rss(url).await.ok(),
        None => None,
    };
    stop_at.store(0, Relaxed);

    println!("\n== ws-loadgen results ==");
    println!("connections requested   {}", args.connections);
    println!("connections established {connected}");
    println!("handshakes failed       {}", stats.failed.load(Relaxed));
    println!("closed by gateway       {}", stats.closed.load(Relaxed));
    println!("ramp time               {ramp_secs:.1} s");
    println!(
        "messages sent           {sent} ({:.0}/s)",
        sent as f64 / elapsed
    );
    println!(
        "messages acknowledged   {acked} ({:.0}/s)",
        acked as f64 / elapsed
    );
    println!("error frames            {}", stats.errors.load(Relaxed));
    if let Some((p50, p99, p999, max)) = percentiles(&stats.latency_ms) {
        println!("ack latency ms          p50 {p50}  p99 {p99}  p99.9 {p999}  max {max}");
    }
    if let (Some(before), Some(connected_rss)) = (rss_before, rss_connected) {
        let per = (connected_rss.saturating_sub(before)) as f64 / connected.max(1) as f64;
        println!(
            "gateway RSS idle        {:.1} MiB",
            before as f64 / 1048576.0
        );
        println!(
            "gateway RSS connected   {:.1} MiB",
            connected_rss as f64 / 1048576.0
        );
        println!("gateway bytes/conn      {per:.0} (idle sockets)");
        println!(
            "=> 1,000,000 sockets    ~{:.1} GiB gateway memory at this footprint",
            per * 1e6 / 1073741824.0
        );
    }
    if let Some(loaded) = rss_loaded {
        println!(
            "gateway RSS under load  {:.1} MiB",
            loaded as f64 / 1048576.0
        );
    }
    for task in tasks {
        task.abort();
    }
    if connected < args.connections as u64 {
        bail!(
            "only {connected} of {} connections established",
            args.connections
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn connection(
    url: String,
    target: SocketAddr,
    source: Option<IpAddr>,
    token: String,
    rate: f64,
    payload: String,
    stats: Arc<Stats>,
    start: Instant,
    stop_at: Arc<AtomicU64>,
) {
    let result = async {
        let socket = if target.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        if let Some(ip) = source {
            socket.bind(SocketAddr::new(ip, 0))?;
        }
        let stream: TcpStream = socket.connect(target).await?;
        stream.set_nodelay(true)?;
        let mut request = url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        let config = WebSocketConfig::default()
            .read_buffer_size(4096)
            .write_buffer_size(0);
        let (ws, _) =
            tokio_tungstenite::client_async_with_config(request, stream, Some(config)).await?;
        anyhow::Ok(ws)
    }
    .await;
    let ws = match result {
        Ok(ws) => ws,
        Err(_) => {
            stats.failed.fetch_add(1, Relaxed);
            return;
        }
    };
    stats.connected.fetch_add(1, Relaxed);
    let (mut sink, mut stream) = ws.split();
    let mut ticker = (rate > 0.0).then(|| {
        let mut t = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        t
    });
    loop {
        tokio::select! {
            _ = async { ticker.as_mut().unwrap().tick().await }, if ticker.is_some() => {
                if stop_at.load(Relaxed) == 0 {
                    ticker = None;
                    continue;
                }
                // The id is the send time, so latency needs no per-message state.
                let id = start.elapsed().as_micros() as u64;
                let frame = format!("{{\"id\":{id},\"value\":\"{payload}\"}}");
                if sink.send(Message::text(frame)).await.is_err() {
                    break;
                }
                stats.sent.fetch_add(1, Relaxed);
            }
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    if text.contains("\"type\":\"ack\"") {
                        stats.acked.fetch_add(1, Relaxed);
                        if let Some(id) = text
                            .split("\"id\":")
                            .nth(1)
                            .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                            .and_then(|n| n.parse::<u64>().ok())
                        {
                            let ms = (start.elapsed().as_micros() as u64).saturating_sub(id) / 1000;
                            stats.latency_ms[(ms as usize).min(10_000)].fetch_add(1, Relaxed);
                        }
                    } else if text.contains("\"type\":\"error\"") {
                        stats.errors.fetch_add(1, Relaxed);
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                    stats.closed.fetch_add(1, Relaxed);
                    break;
                }
                Some(Ok(_)) => {}
            }
        }
    }
}

fn percentiles(buckets: &[AtomicU64]) -> Option<(usize, usize, usize, usize)> {
    let counts: Vec<u64> = buckets.iter().map(|b| b.load(Relaxed)).collect();
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    let at = |q: f64| {
        let target = (total as f64 * q).ceil() as u64;
        let mut seen = 0;
        for (ms, c) in counts.iter().enumerate() {
            seen += c;
            if seen >= target {
                return ms;
            }
        }
        counts.len() - 1
    };
    let max = counts.iter().rposition(|&c| c > 0).unwrap_or(0);
    Some((at(0.50), at(0.99), at(0.999), max))
}

async fn gateway_rss(url: &str) -> anyhow::Result<u64> {
    let rest = url
        .strip_prefix("http://")
        .context("metrics url must be http://")?;
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, "metrics"), |(a, p)| (a, p));
    let mut stream = TcpStream::connect(authority).await?;
    stream
        .write_all(
            format!("GET /{path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut body = String::new();
    stream.read_to_string(&mut body).await?;
    body.lines()
        .find_map(|l| l.strip_prefix("process_resident_memory_bytes "))
        .and_then(|v| v.trim().parse().ok())
        .context("no process_resident_memory_bytes in /metrics")
}
