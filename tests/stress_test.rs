//! 통합 부하 테스트 — 100 / 300 / 500명 시나리오
//!
//! 각 테스트 케이스는 서버를 별도 tokio task로 기동하고,
//! Normal_Bot N개를 실행한 뒤 메시지 누락률 0%를 assert한다.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use rust_projects::metrics::Metrics;
use rust_projects::room::Room;
use rust_projects::server::run_with_listener;
use rust_projects::vote::VoteBoard;

// ── 헬퍼 함수 ──────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 서버를 spawn하고 실제 바인딩된 주소를 반환
async fn spawn_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("서버 바인딩 실패");
    let addr = listener.local_addr().unwrap();
    // 부하 테스트용 대용량 broadcast 채널
    // 500봇 × 10메시지 = 5000 + Presence 이벤트 등 여유
    let room = Room::new_with_capacity(65536);
    let vote = VoteBoard::new();
    let metrics = Metrics::new();

    let handle = tokio::spawn(async move {
        let _ = run_with_listener(listener, room, vote, metrics).await;
    });

    wait_for_server(addr).await;
    (addr, handle)
}

/// TCP connect 재시도로 서버 준비 대기
async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("서버가 시작되지 않음: {addr}");
}

/// 경량 봇: 메시지 전송 및 자신의 메시지 수신 카운트 반환
async fn run_bot(id: usize, addr: SocketAddr, msg_count: usize) -> u64 {
    let stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("봇 {id} 연결 실패: {e}"));

    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);

    let target = format!("bot_{id}_msg_");
    let msg_count_u64 = msg_count as u64;

    // 수신 task: 자신의 메시지 패턴과 일치하는 수신 메시지 카운트
    let recv_target = target.clone();
    let recv_task = tokio::spawn(async move {
        let mut count = 0u64;
        // 타임아웃: 충분한 시간 (180초)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, lines.next_line()).await {
                Ok(Ok(Some(line))) => {
                    if line.contains(&recv_target) {
                        count += 1;
                        if count >= msg_count_u64 {
                            break;
                        }
                    }
                }
                // 연결 종료 또는 에러
                Ok(Ok(None)) | Ok(Err(_)) => break,
                // 타임아웃
                Err(_) => break,
            }
        }
        count
    });

    // 송신: 모든 메시지를 빠르게 전송
    for seq in 0..msg_count {
        let msg = format!(
            r#"{{"type":"chat","text":"bot_{id}_msg_{seq}","client_ts":{ts}}}"#,
            ts = now_ms()
        );
        writer.write_all(msg.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
    }
    writer.flush().await.unwrap();

    recv_task.await.unwrap_or(0)
}

/// 공통 테스트 로직: 서버 기동 → 봇 실행 → 누락 assert → 서버 정리
async fn stress_test(bot_count: usize, msg_per_bot: usize) {
    let (addr, server_handle) = spawn_server().await;
    let recv_counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(bot_count);

    // 봇을 배치로 spawn하여 동시 연결 폭주 방지
    let batch_size = 50;
    for batch_start in (0..bot_count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(bot_count);
        for i in batch_start..batch_end {
            let counter = recv_counter.clone();
            handles.push(tokio::spawn(async move {
                let received = run_bot(i, addr, msg_per_bot).await;
                counter.fetch_add(received, Ordering::Relaxed);
            }));
        }
        // 배치 간 딜레이: 연결 안정화
        if batch_end < bot_count {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    for h in handles {
        let _ = h.await;
    }

    let expected = (bot_count * msg_per_bot) as u64;
    let received = recv_counter.load(Ordering::SeqCst);
    assert_eq!(
        expected, received,
        "메시지 누락! expected={expected} received={received} (bot_count={bot_count}, msg_per_bot={msg_per_bot})"
    );

    server_handle.abort();
}

// ── 시나리오별 테스트 케이스 ────────────────────────────────────────

#[tokio::test]
async fn stress_100() {
    stress_test(100, 10).await;
}

#[tokio::test]
async fn stress_300() {
    stress_test(300, 10).await;
}

#[tokio::test]
async fn stress_500() {
    stress_test(500, 10).await;
}
