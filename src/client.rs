use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::broadcast,
};
use tracing::debug;

use crate::{
    metrics::Metrics,
    protocol::{BroadcastEvent, ClientMsg, ServerMsg, MAX_LINE_LEN},
    room::Room,
    vote::VoteBoard,
};

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

    room.join(id).await;

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
                        let sent_at = now_ms();

                        let msg: ClientMsg = match serde_json::from_str(&raw) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };

                        match msg {
                            ClientMsg::Chat { text } => {
                                metrics.record_latency(sent_at);
                                room.broadcast(ServerMsg::Chat { from: id, text, sent_at });
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
