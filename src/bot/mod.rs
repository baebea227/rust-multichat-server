pub mod fickle;
pub mod ghost;
pub mod normal;
pub mod quitter;
pub mod spammer;

use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tracing::info;

use crate::protocol::ClientMsg;

pub const SERVER_ADDR: &str = "127.0.0.1:8080";

/// 봇이 소켓으로 JSON 메시지 한 줄을 전송하는 헬퍼
pub async fn send_msg(
    stream: &mut tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    msg: &ClientMsg,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn connect() -> Result<TcpStream> {
    Ok(TcpStream::connect(SERVER_ADDR).await?)
}

/// 전체 봇 시나리오 실행기
pub async fn run_scenario(mode: &str, count: usize, msg_per_bot: usize) {
    let recv_counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(count);

    for i in 0..count {
        let recv_counter = recv_counter.clone();
        let mode = mode.to_string();

        let handle = tokio::spawn(async move {
            let result = match mode.as_str() {
                "normal" => normal::run(i as u64, msg_per_bot, recv_counter).await,
                "fickle" => fickle::run(i as u64, msg_per_bot).await,
                "spammer" => spammer::run(i as u64, msg_per_bot, recv_counter).await,
                "ghost" => ghost::run(i as u64, msg_per_bot).await,
                "quitter" => quitter::run(i as u64).await,
                other => {
                    tracing::warn!("알 수 없는 봇 모드: {other}");
                    Ok(())
                }
            };
            if let Err(e) = result {
                tracing::warn!("봇 {i} 오류: {e}");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    if mode == "normal" {
        let expected = (count * msg_per_bot) as u64;
        let received = recv_counter.load(Ordering::SeqCst);
        info!(expected, received, "누락 검증");
        assert_eq!(
            expected, received,
            "메시지 누락 발생! expected={expected} received={received}"
        );
        info!("누락 없음 확인");
    }
}
