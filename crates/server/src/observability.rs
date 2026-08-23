//! Wiring for the metrics sampler and the dashboard HTTP server
//! (DESIGN.md §9).

use std::sync::Arc;

use anyhow::{Context, Result};
use brahmaputra_broker::Broker;
use brahmaputra_dashboard::{ControllerClient, DashboardState};
use brahmaputra_metadata::{ClusterMetadata, MetadataCache, MetadataCommand, Role, UserRecord};
use brahmaputra_metrics::{names, Metrics};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info, warn};

/// Register descriptions once so `/metrics` carries HELP lines.
pub fn describe_metrics(metrics: &Metrics) {
    metrics.describe(names::REQUESTS, "Data-plane requests served, by api");
    metrics.describe(names::PRODUCE_REQUESTS, "Produce requests served");
    metrics.describe(names::PRODUCE_RECORDS, "Records appended");
    metrics.describe(names::PRODUCE_BYTES, "Record bytes appended");
    metrics.describe(names::FETCH_REQUESTS, "Fetch requests served");
    metrics.describe(names::FETCH_BYTES, "Record bytes served to consumers");
    metrics.describe(names::THROTTLED_REQUESTS, "Requests delayed by a quota");
    metrics.describe(names::THROTTLE_MS, "Milliseconds of quota delay applied");
    metrics.describe(names::LOG_END_OFFSET, "Partition log end offset");
    metrics.describe(names::LOG_START_OFFSET, "Partition log start offset");
    metrics.describe(names::HIGH_WATERMARK, "Partition high watermark");
    metrics.describe(names::ISR_SIZE, "In-sync replica count");
    metrics.describe(
        names::UNDER_REPLICATED,
        "Partitions led by this broker whose ISR is short of its replica set",
    );
    metrics.describe(names::LEADER_PARTITIONS, "Partitions led by this broker");
}

/// Refresh partition gauges and take one sample of every metric, on a timer.
///
/// Sampling on a timer rather than per request is what keeps the hot path
/// free: an append costs an atomic increment, and the expensive part —
/// reading every partition's offsets — happens once per interval.
pub async fn run_metrics_sampler(
    broker: Arc<Broker>,
    mut shutdown: watch::Receiver<bool>,
    period: Duration,
) {
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {
                broker.sample_partition_metrics().await;
                broker.metrics().sample();
            }
        }
    }
}

/// Submits metadata commands through the local controller node.
struct LocalController {
    node: Arc<brahmaputra_controller::ControllerNode>,
}

#[async_trait::async_trait]
impl ControllerClient for LocalController {
    async fn submit(&self, command: serde_json::Value) -> Result<(), String> {
        let command: MetadataCommand = serde_json::from_value(command)
            .map_err(|error| format!("malformed metadata command: {error}"))?;
        self.node
            .write_metadata(command)
            .await
            .map(|_| ())
            .map_err(|error| format!("{}: {}", error.code, error.message))
    }
}

/// Start the dashboard HTTP server. Returns the address it bound.
pub async fn start_dashboard(
    host: &str,
    port: u16,
    broker: Arc<Broker>,
    metadata: Option<MetadataCache>,
    controller: Option<Arc<brahmaputra_controller::ControllerNode>>,
    shutdown: watch::Receiver<bool>,
) -> Result<std::net::SocketAddr> {
    let listener = TcpListener::bind((host, port))
        .await
        .with_context(|| format!("cannot bind dashboard on {host}:{port}"))?;
    let addr = listener.local_addr()?;
    let state = DashboardState {
        broker,
        metadata,
        controller: controller
            .map(|node| Arc::new(LocalController { node }) as Arc<dyn ControllerClient>),
    };
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        let signal = async move {
            while !*shutdown.borrow_and_update() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        };
        if let Err(error) = brahmaputra_dashboard::serve(listener, state, signal).await {
            warn!(%error, "dashboard server stopped");
        }
    });
    Ok(addr)
}

/// Create the cluster's signing secret and first admin, once, on first boot.
///
/// Both live in the Raft metadata so every broker can verify a session
/// without extra configuration. This is idempotent: if either already
/// exists it is left alone, so restarting a node never resets a password or
/// invalidates outstanding sessions.
pub async fn bootstrap_admin(
    controller: &brahmaputra_controller::ControllerNode,
    image: &ClusterMetadata,
    admin_user: &str,
    admin_password: Option<&str>,
) -> Result<()> {
    if image.jwt_secret.is_none() {
        let secret = generate_secret();
        controller
            .write_metadata(MetadataCommand::SetJwtSecret { secret })
            .await
            .map_err(|error| anyhow::anyhow!("cannot set jwt secret: {}", error.message))?;
        info!("generated the cluster session-signing secret");
    }

    if image.users.values().any(|user| user.role == Role::Admin) {
        return Ok(());
    }

    let (password, generated) = match admin_password {
        Some(password) if password.len() >= 8 => (password.to_owned(), false),
        Some(_) => {
            anyhow::bail!("BRAHMAPUTRA_ADMIN_PASSWORD must be at least 8 characters")
        }
        None => (generate_secret(), true),
    };
    let user = UserRecord::new(admin_user, &password, Role::Admin, generated)
        .map_err(|_| anyhow::anyhow!("cannot hash the initial admin password"))?;
    controller
        .write_metadata(MetadataCommand::PutUser { user })
        .await
        .map_err(|error| anyhow::anyhow!("cannot create the admin user: {}", error.message))?;

    if generated {
        // Printed once, never stored in the clear. An operator who misses
        // it has to reset the password rather than read it back.
        info!(
            password = %password,
            "created the initial admin user; this password is shown once and must be changed"
        );
    } else {
        info!("created the initial admin user from BRAHMAPUTRA_ADMIN_PASSWORD");
    }
    Ok(())
}

/// A 256-bit random value, hex encoded, from the OS RNG.
fn generate_secret() -> String {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).expect("OS random number generator");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
