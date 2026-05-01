use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    client::handle_client,
    metrics::Metrics,
    protocol::ServerMsg,
    room::Room,
    vote::VoteBoard,
};

pub const MAX_CONNECTIONS: usize = 500;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
pub static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);

pub async fn run(addr: &str, room: Arc<Room>, vote: Arc<VoteBoard>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("서버 시작: {addr}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                // 연결 수 상한 확인: 초과 시 오류 응답 후 즉시 소켓 닫기
                if CONN_COUNT.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
                    warn!(%peer, "최대 연결 수 초과 — 접속 거절");
                    tokio::spawn(async move {
                        let (_, mut writer) = stream.into_split();
                        let msg = ServerMsg::Error { msg: "서버가 가득 찼습니다 (최대 500인)".into() };
                        if let Ok(mut line) = serde_json::to_string(&msg) {
                            line.push('\n');
                            let _ = writer.write_all(line.as_bytes()).await;
                        }
                    });
                    continue;
                }

                CONN_COUNT.fetch_add(1, Ordering::Relaxed);
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                let room = room.clone();
                let vote = vote.clone();
                let metrics = metrics.clone();
                info!(id, %peer, "클라이언트 접속 (현재 {}명)", CONN_COUNT.load(Ordering::Relaxed));
                tokio::spawn(async move {
                    if let Err(e) = handle_client(id, stream, room, vote, metrics).await {
                        warn!(id, "클라이언트 오류: {e}");
                    }
                    CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) => {
                warn!("accept 실패: {e}");
            }
        }
    }
}
