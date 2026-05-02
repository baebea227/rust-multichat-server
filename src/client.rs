use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::broadcast,
};
use tracing::{debug, warn};

use crate::{
    metrics::Metrics,
    protocol::{BroadcastEvent, ClientMsg, MAX_LINE_LEN, ServerMsg},
    room::Room,
    vote::VoteBoard,
};

const TOKEN_CAPACITY: f64 = 10.0;
const TOKEN_RATE: f64 = 10.0;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn handle_client(
    id: u64,
    stream: TcpStream,
    room: Arc<Room>,
    vote: Arc<VoteBoard>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut rx: broadcast::Receiver<BroadcastEvent> = room.subscribe();

    // 이슈 4: 입장 후 현재 참여자 수를 받아 Welcome 메시지로 즉시 전송 (broadcast 아님)
    let peer_count = room.join(id).await;
    let welcome = ServerMsg::Welcome { peer_count };
    let mut welcome_line = serde_json::to_string(&welcome).unwrap_or_default();
    welcome_line.push('\n');
    writer.write_all(welcome_line.as_bytes()).await?;

    // write task: broadcast 수신 → 소켓 송신
    let mut write_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(BroadcastEvent::Server(msg)) => {
                    let mut line = serde_json::to_string(&msg).unwrap_or_default();
                    line.push('\n');
                    if writer.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Ok(BroadcastEvent::Shutdown) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // ghost 봇 등 수신 지연 시 메시지 드롭 — 서버는 계속 동작
                    debug!("broadcast lagged, {n} messages dropped");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut current_vote: Option<usize> = None;
    let mut tokens: f64 = TOKEN_CAPACITY;
    let mut last_refill = Instant::now();
    let mut nick: Option<String> = None; // 이슈 5: 닉네임

    // read task (현재 task): 소켓 수신 → 처리
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(raw)) => {
                        if raw.len() > MAX_LINE_LEN {
                            continue;
                        }
                        metrics.record_recv();

                        let msg: ClientMsg = match serde_json::from_str(&raw) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };

                        // 클라이언트별 rate limiting: 토큰 버킷 (10 token/s, burst 10)
                        let elapsed = last_refill.elapsed().as_secs_f64();
                        last_refill = Instant::now();
                        tokens = (tokens + elapsed * TOKEN_RATE).min(TOKEN_CAPACITY);
                        if tokens < 1.0 {
                            warn!(id, "rate limit 초과 — 메시지 드롭");
                            continue;
                        }
                        tokens -= 1.0;

                        match msg {
                            ClientMsg::SetNick { name } => {
                                // 이슈 5: 닉네임 로컬 저장 + room 메타 업데이트
                                room.set_nick(id, name.clone()).await;
                                nick = Some(name);
                            }
                            ClientMsg::Chat { text, client_ts } => {
                                // 이슈 7: 클라이언트 송신 시각(client_ts) 기준으로 latency 측정
                                metrics.record_latency(client_ts);
                                let sent_at = now_ms();
                                room.broadcast(ServerMsg::Chat {
                                    from: id,
                                    nick: nick.clone(), // 이슈 5
                                    text,
                                    sent_at,
                                });
                                metrics.record_sent();
                            }
                            ClientMsg::Vote { option } => {
                                vote.vote(current_vote, option);
                                current_vote = Some(option);
                                room.broadcast(ServerMsg::VoteSnapshot {
                                    counts: vote.snapshot(),
                                });
                            }
                            ClientMsg::Unvote => {
                                if let Some(prev) = current_vote.take() {
                                    vote.unvote(prev);
                                    room.broadcast(ServerMsg::VoteSnapshot {
                                        counts: vote.snapshot(),
                                    });
                                }
                            }
                        }
                    }
                    // 연결 종료
                    Ok(None) | Err(_) => break,
                }
            }
            _ = &mut write_task => break,
        }
    }

    write_task.abort();
    if let Some(prev) = current_vote {
        vote.unvote(prev);
    }
    room.leave(id).await;

    Ok(())
}
