use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::message::LogEntry;
use crate::worker::ShardedPool;

pub async fn run(
    port: u16,
    tx: tokio::sync::mpsc::Sender<LogEntry>,
    pool: Option<Arc<ShardedPool>>,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("server listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let tx = tx.clone();
                let pool = pool.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(socket, tx, pool).await {
                        error!(%peer_addr, error = %err, "connection handler failed");
                    }
                });
            }
            Err(err) => {
                error!(error = %err, "failed to accept connection");
            }
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    tx: tokio::sync::mpsc::Sender<LogEntry>,
    pool: Option<Arc<ShardedPool>>,
) -> Result<()> {
    let peer_addr = socket.peer_addr().ok();
    let reader = BufReader::new(socket);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<LogEntry>(&line) {
            Ok(entry) => {
                if let Some(sharded_pool) = &pool {
                    sharded_pool.route(entry);
                } else if tx.send(entry).await.is_err() {
                    warn!(?peer_addr, "message channel closed; stopping connection handler");
                    break;
                }
            }
            Err(err) => {
                debug!(?peer_addr, error = %err, "failed to parse log entry");
            }
        }
    }

    Ok(())
}
