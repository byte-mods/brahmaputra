use anyhow::Context;
use brahmaputra_ws_gateway::auth::{self, Claims};
use brahmaputra_ws_gateway::GatewayConfig;
use clap::{Parser, Subcommand};

/// Stateless WebSocket gateway into Brahmaputra.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    config: Option<GatewayConfig>,
}

#[derive(Subcommand)]
enum Command {
    /// Print an HS256 token for testing. Production tokens come from your
    /// identity provider.
    MintToken {
        #[arg(long)]
        secret: String,
        #[arg(long)]
        sub: String,
        #[arg(long, default_value_t = 3600)]
        ttl_secs: i64,
        /// Restrict the token to these topic patterns.
        #[arg(long = "topic")]
        topics: Vec<String>,
        /// Topic patterns the token may subscribe to.
        #[arg(long = "subscribe")]
        subscribe: Vec<String>,
        /// A token that may not publish anywhere (`"topics": []`), e.g.
        /// for a price-feed viewer that only subscribes.
        #[arg(long, conflicts_with = "topics")]
        read_only: bool,
        #[arg(long)]
        kid: Option<String>,
        #[arg(long)]
        iss: Option<String>,
        #[arg(long)]
        aud: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    if let Some(Command::MintToken {
        secret,
        sub,
        ttl_secs,
        topics,
        subscribe,
        read_only,
        kid,
        iss,
        aud,
    }) = cli.command
    {
        let now = auth::now_secs();
        let claims = Claims {
            sub,
            exp: now + ttl_secs,
            nbf: None,
            iat: Some(now),
            iss,
            aud: aud.map(auth::Audience::One),
            topics: if read_only {
                Some(Vec::new())
            } else {
                (!topics.is_empty()).then_some(topics)
            },
            subscribe: (!subscribe.is_empty()).then_some(subscribe),
        };
        println!(
            "{}",
            auth::sign_hs256(&claims, kid.as_deref(), secret.as_bytes())
        );
        return Ok(());
    }
    let config = cli.config.context("missing configuration")?;
    let gateway = brahmaputra_ws_gateway::start(config).await?;

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutting down: draining connections");
    gateway.shutdown().await;
    Ok(())
}
