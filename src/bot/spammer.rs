use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};

use super::{connect, send_msg, RttCounter};
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

    // 수신 task: msg_count개 수신하면 스스로 종료
    let target = format!("bot_{id}_msg_");
    let recv_task = tokio::spawn(async move {
        let mut count = 0u64;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains(&target) {
                // RTT 계산: 수신 시점 - 송신 시점
                let recv_ts = now_ms();
                if let Some(seq) = extract_seq(&line, &target) {
                    if let Some(&send_ts) = send_timestamps.get(seq) {
                        if recv_ts >= send_ts {
                            rtt_counter.record(recv_ts - send_ts);
                        }
                    }
                }

                count += 1;
                if count >= msg_count as u64 {
                    break;
                }
            }
        }
        count
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
