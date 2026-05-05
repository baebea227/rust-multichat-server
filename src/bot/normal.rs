use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tracing::warn;

use super::{connect, recv_until_count_with_timeout, send_msg, RttCounter, BOT_RECV_TIMEOUT_SECS};
use crate::protocol::ClientMsg;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn run(
    id: u64,
    msg_count: usize,
    recv_counter: Arc<AtomicU64>,
    rtt_counter: Arc<RttCounter>,
) -> Result<()> {
    let stream = connect().await?;
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);

    // 송신 시각을 seq별로 기록
    let mut send_timestamps: Vec<u64> = Vec::with_capacity(msg_count);

    // 송신: 클라이언트 송신 시각 client_ts 포함
    for seq in 0..msg_count {
        let ts = now_ms();
        send_timestamps.push(ts);
        let text = format!("bot_{id}_msg_{seq}");
        send_msg(
            &mut writer,
            &ClientMsg::Chat {
                text,
                client_ts: ts,
            },
        )
        .await?;
    }

    // 공용 recv 루프 사용 — RTT 기록은 on_match 콜백으로 주입
    let target = format!("bot_{id}_msg_");
    let recv_task = tokio::spawn(async move {
        let target_owned = target.clone();
        let on_match = |line: &str| {
            let recv_ts = now_ms();
            if let Some(seq) = extract_seq(line, &target_owned) {
                if let Some(&send_ts) = send_timestamps.get(seq) {
                    if recv_ts >= send_ts {
                        rtt_counter.record(recv_ts - send_ts);
                    }
                }
            }
        };
        let received = recv_until_count_with_timeout(
            &mut lines,
            &target,
            msg_count as u64,
            Duration::from_secs(BOT_RECV_TIMEOUT_SECS),
            on_match,
        )
        .await;
        if received < msg_count as u64 {
            warn!(
                bot_id = id,
                expected = msg_count,
                received,
                "recv 루프 조기 종료(타임아웃 또는 EOF)"
            );
        }
        received
    });

    // recv 완료 대기 후 writer 종료
    let received = recv_task.await.unwrap_or(0);
    drop(writer);
    recv_counter.fetch_add(received, Ordering::Relaxed);

    Ok(())
}

/// 수신된 라인에서 target 접두사 이후의 seq 번호를 추출
fn extract_seq(line: &str, target: &str) -> Option<usize> {
    let idx = line.find(target)?;
    let after = &line[idx + target.len()..];
    let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    num_str.parse().ok()
}
