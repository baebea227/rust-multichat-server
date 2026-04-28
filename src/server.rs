use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    client::handle_client,
    metrics::Metrics,
    room::Room,
    vote::VoteBoard,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub async fn run(addr: &str, room: Arc<Room>, vote: Arc<VoteBoard>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("서버 시작: {addr}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                let room = room.clone();
                let vote = vote.clone();
                let metrics = metrics.clone();
                info!(id, %peer, "클라이언트 접속");
                tokio::spawn(async move {
                    if let Err(e) = handle_client(id, stream, room, vote, metrics).await {
                        warn!(id, "클라이언트 오류: {e}");
                    }
                });
            }
            Err(e) => {
                warn!("accept 실패: {e}");
            }
        }
    }
}
