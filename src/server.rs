use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, warn};

use crate::message::LogEntry;

pub async fn run(port: u16, tx: Sender<LogEntry>) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("server listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(socket, tx).await {
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

async fn handle_connection(socket: TcpStream, tx: Sender<LogEntry>) -> Result<()> {
    let peer_addr = socket.peer_addr().ok();
    let reader = BufReader::new(socket);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<LogEntry>(&line) {
            Ok(entry) => {
                if tx.send(entry).await.is_err() {
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
