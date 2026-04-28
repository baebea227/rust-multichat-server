use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use anyhow::Result;

use crate::protocol::ClientMsg;
use super::{connect, send_msg};

pub async fn run(id: u64, msg_count: usize, recv_counter: Arc<AtomicU64>) -> Result<()> {
    let stream = connect().await?;
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);

    // 수신 task: msg_count개 수신하면 스스로 종료
    let target = format!("bot_{id}_msg_");
    let recv_task = tokio::spawn(async move {
        let mut count = 0u64;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains(&target) {
                count += 1;
                if count >= msg_count as u64 {
                    break;
                }
            }
        }
        count
    });

    // 송신
    for seq in 0..msg_count {
        let text = format!("bot_{id}_msg_{seq}");
        send_msg(&mut writer, &ClientMsg::Chat { text }).await?;
    }

    // recv 완료 대기 후 writer 종료
    let received = recv_task.await.unwrap_or(0);
    drop(writer);
    recv_counter.fetch_add(received, Ordering::Relaxed);

    Ok(())
}
