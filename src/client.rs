use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
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

/// '\n'으로 종료된 한 줄을 읽되 max_len 바이트로 상한을 강제한다.
/// 초과 라인은 다음 '\n'까지 드레인하고 빈 문자열을 반환 — 호출측 JSON 파싱에서 자연스럽게 스킵됨.
async fn read_line_bounded<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> std::io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut overflow = false;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if buf.is_empty() && !overflow {
                return Ok(None);
            }
            if overflow {
                return Ok(Some(String::new()));
            }
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        if let Some(pos) = chunk.iter().position(|&b| b == b'\n') {
            if !overflow {
                let avail = max_len.saturating_sub(buf.len());
                let take = pos.min(avail);
                buf.extend_from_slice(&chunk[..take]);
                if pos > avail {
                    overflow = true;
                }
            }
            let consume_n = pos + 1;
            reader.consume(consume_n);
            if overflow {
                return Ok(Some(String::new()));
            }
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        } else {
            if !overflow {
                let avail = max_len.saturating_sub(buf.len());
                let take = chunk.len().min(avail);
                buf.extend_from_slice(&chunk[..take]);
                if chunk.len() > avail {
                    overflow = true;
                }
            }
            let n = chunk.len();
            reader.consume(n);
        }
    }
}

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
    let mut reader = BufReader::new(reader);
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
            line = read_line_bounded(&mut reader, MAX_LINE_LEN) => {
                match line {
                    Ok(Some(raw)) => {
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
                        metrics.record_recv();

                        match msg {
                            ClientMsg::SetNick { name } => {
                                // 이슈 5: 닉네임 로컬 저장 + room 메타 업데이트
                                room.set_nick(id, name.clone()).await;
                                nick = Some(name);
                            }
                            ClientMsg::Chat { text, client_ts } => {
                                // client_ts를 echo하여 발신자가 자체 RTT 계산 가능 (#6)
                                // — wall-clock 시계 스큐 영향을 받지 않음
                                let sent_at = now_ms();
                                room.broadcast(ServerMsg::Chat {
                                    from: id,
                                    nick: nick.clone(),
                                    text,
                                    sent_at,
                                    client_ts,
                                });
                                metrics.record_sent();
                            }
                            ClientMsg::Vote { option } => {
                                vote.vote(current_vote, option);
                                current_vote = Some(option);
                                // 이슈 6: percentages 함께 전송
                                let (counts, percentages) = vote.snapshot_with_percentages();
                                room.broadcast(ServerMsg::VoteSnapshot { counts, percentages });
                            }
                            ClientMsg::Unvote => {
                                if let Some(prev) = current_vote.take() {
                                    vote.unvote(prev);
                                    let (counts, percentages) = vote.snapshot_with_percentages();
                                    room.broadcast(ServerMsg::VoteSnapshot { counts, percentages });
                                }
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(id, err = %e, "소켓 읽기 오류 — 연결 종료");
                        break;
                    }
                }
            }
            _ = &mut write_task => break,
        }
    }

    write_task.abort();
    if let Some(prev) = current_vote {
        vote.unvote(prev);
        let (counts, percentages) = vote.snapshot_with_percentages();
        room.broadcast(ServerMsg::VoteSnapshot { counts, percentages });
    }
    room.leave(id).await;

    Ok(())
}
