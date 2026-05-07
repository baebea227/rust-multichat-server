use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::{
    client::handle_client,
    metrics::Metrics,
    protocol::{BroadcastEvent, ServerMsg},
    room::Room,
    vote::VoteBoard,
};

pub const MAX_CONNECTIONS: usize = 500;
/// SYN burst(수백 개 봇 동시 connect)에서 backlog overflow → ECONNREFUSED 방지.
const LISTEN_BACKLOG: u32 = 8192;

pub async fn run(addr: &str, room: Arc<Room>, vote: Arc<VoteBoard>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let sock_addr: std::net::SocketAddr = addr.parse()?;
    let socket = if sock_addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.bind(sock_addr)?;
    let listener = socket.listen(LISTEN_BACKLOG)?;
    run_with_listener(listener, room, vote, metrics).await
}

pub async fn run_with_listener(
    listener: TcpListener,
    room: Arc<Room>,
    vote: Arc<VoteBoard>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let addr = listener.local_addr()?;
    info!("서버 시작: {addr}");

    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let next_id = Arc::new(AtomicU64::new(1));
    let mut handles = Vec::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        // permit 획득 실패 시 연결 거절 — load/add 분리 없이 원자적으로 상한 보장
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
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
                        };

                        let id = next_id.fetch_add(1, Ordering::Relaxed);
                        let room = room.clone();
                        let vote = vote.clone();
                        let metrics = metrics.clone();
                        let active = MAX_CONNECTIONS - semaphore.available_permits();
                        info!(id, %peer, "클라이언트 접속 (현재 {active}명)");
                        let handle = tokio::spawn(async move {
                            let _permit = permit; // task 종료(패닉 포함) 시 자동 반납
                            if let Err(e) = handle_client(id, stream, room, vote, metrics).await {
                                warn!(id, "클라이언트 오류: {e}");
                            }
                        });
                        handles.push(handle);
                        // 종료된 핸들 회수 — 장기 가동 시 unbounded 누적 방지
                        handles.retain(|h| !h.is_finished());
                    }
                    Err(e) => {
                        warn!("accept 실패: {e}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT 수신 — graceful shutdown 시작");
                let _ = room.tx.send(BroadcastEvent::Shutdown);
                break;
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
    info!("모든 클라이언트 태스크 종료 — shutdown 완료");

    Ok(())
}
