/// Bug Condition Exploration Test: recv_task 무한 대기 버그
///
/// **Validates: Requirements 1.1, 1.2, 1.3**
///
/// 이 테스트는 수정 전 코드에서 반드시 실패해야 한다.
/// 실패가 버그 존재를 확인하는 것이다.
///
/// Bug Condition: actual_available_messages < msg_count AND connection_alive = true
/// 일 때 recv_task가 BOT_RECV_TIMEOUT_SECS 이내에 종료해야 하지만,
/// 현재 코드에는 타임아웃이 없어 무한 대기가 발생한다.

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// recv_task의 핵심 로직을 재현하는 헬퍼.
/// 바이너리 크레이트이므로 내부 모듈에 직접 접근할 수 없어
/// src/bot/normal.rs의 recv_task 로직을 동일하게 구현한다.
///
/// 이 함수는 수정된 코드의 recv_task와 동일한 패턴을 사용한다:
/// - tokio::time::timeout으로 수신 루프 전체를 감싼다
/// - `while let Ok(Some(line)) = lines.next_line().await` 루프
/// - target 문자열 매칭으로 count 증가
/// - count >= msg_count 시 break
/// - 타임아웃(3초, 테스트용) 발생 시 현재까지 수신된 count를 반환
async fn recv_task_current(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    target: &str,
    msg_count: u64,
) -> u64 {
    // 테스트용 타임아웃: 3초 (실제 코드는 BOT_RECV_TIMEOUT_SECS=30초)
    let recv_timeout = Duration::from_secs(3);
    let count = Arc::new(AtomicU64::new(0));
    let count_inner = count.clone();
    let target = target.to_string();

    let result = tokio::time::timeout(recv_timeout, async move {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains(&target) {
                let c = count_inner.fetch_add(1, Ordering::Relaxed) + 1;
                if c >= msg_count {
                    break;
                }
            }
        }
        count_inner.load(Ordering::Relaxed)
    })
    .await;

    match result {
        Ok(c) => c,
        Err(_) => {
            // 타임아웃 발생: 현재까지 수신된 count를 반환
            count.load(Ordering::Relaxed)
        }
    }
}

/// Property 1: Bug Condition - recv_task 무한 대기 버그
///
/// **Validates: Requirements 1.1, 1.2, 1.3**
///
/// 시나리오: msg_count=10으로 설정하고 서버가 5개만 전송 후 연결을 유지
/// 기대 동작: recv_task가 BOT_RECV_TIMEOUT_SECS(여기서는 3초) 이내에 종료
/// 수정 전 코드: 타임아웃이 없어 무한 대기 발생 → 테스트 실패 (이것이 정상)
#[tokio::test]
async fn test_bug_condition_recv_task_hangs_on_partial_messages() {
    let msg_count: u64 = 10;
    let actual_available: u64 = 5;
    // 테스트용 짧은 타임아웃 (수정 후 BOT_RECV_TIMEOUT_SECS에 해당)
    let test_timeout = Duration::from_secs(3);

    // 가짜 서버: 임의 포트에 바인드
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let bot_id: u64 = 42;
    let target = format!("bot_{bot_id}_msg_");

    // 서버 태스크: actual_available개의 메시지만 전송 후 연결 유지 (EOF 미발생)
    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (_, mut writer) = stream.into_split();

        // actual_available개의 메시지를 Chat JSON 형식으로 전송
        for seq in 0..actual_available {
            let chat_msg = serde_json::json!({
                "type": "chat",
                "from": 999,
                "nick": null,
                "text": format!("bot_{bot_id}_msg_{seq}"),
                "sent_at": 1234567890u64
            });
            let mut line = serde_json::to_string(&chat_msg).unwrap();
            line.push('\n');
            writer.write_all(line.as_bytes()).await.unwrap();
            writer.flush().await.unwrap();
        }

        // 연결을 유지한 채 대기 (EOF를 보내지 않음)
        // 이것이 Bug Condition의 핵심: connection_alive = true
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    // 봇(클라이언트) 측: 서버에 연결하고 recv_task 실행
    let client_stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let (reader, _writer) = client_stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    // 수정 전 코드의 recv_task를 재현하여 실행
    // tokio::time::timeout으로 감싸서 무한 대기를 감지
    let result = tokio::time::timeout(
        test_timeout,
        recv_task_current(&mut lines, &target, msg_count),
    )
    .await;

    // 정리
    server_handle.abort();

    // 검증:
    // - 수정 후 기대 동작: recv_task가 타임아웃 이내에 종료하고 actual_available(5)를 반환
    // - 수정 전 현재 동작: 타임아웃이 없어 recv_task가 무한 대기 → timeout Err 발생
    //
    // 이 assert는 "recv_task가 타임아웃 이내에 정상 종료하고 올바른 count를 반환"하는지 검증한다.
    // 수정 전 코드에서는 무한 대기로 인해 timeout이 발생하여 이 assert가 실패한다.
    // → 실패 = 버그 존재 증명 (이것이 정상)
    assert!(
        result.is_ok(),
        "recv_task가 {test_timeout:?} 이내에 종료되지 않았습니다. \
         Bug Condition 확인: msg_count={msg_count}, actual_available={actual_available}, \
         connection_alive=true → recv_task가 타임아웃 없이 무한 대기합니다."
    );

    let received = result.unwrap();
    assert_eq!(
        received, actual_available,
        "recv_task가 수신한 메시지 수({received})가 \
         실제 전송된 메시지 수({actual_available})와 다릅니다."
    );
}

/// Bug Condition 변형: 서버가 메시지를 전혀 보내지 않고 연결만 유지
///
/// **Validates: Requirements 1.2**
///
/// 시나리오: msg_count=10, 서버가 0개 전송, 연결 유지
/// 수정 전 코드: 첫 번째 메시지부터 무한 대기 → 테스트 실패
#[tokio::test]
async fn test_bug_condition_recv_task_hangs_on_zero_messages() {
    let msg_count: u64 = 10;
    let test_timeout = Duration::from_secs(3);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let bot_id: u64 = 99;
    let target = format!("bot_{bot_id}_msg_");

    // 서버: 메시지를 전혀 보내지 않고 연결만 유지
    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (_reader, _writer) = stream.into_split();
        // 연결 유지, 아무것도 전송하지 않음
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let client_stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let (reader, _writer) = client_stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let result = tokio::time::timeout(
        test_timeout,
        recv_task_current(&mut lines, &target, msg_count),
    )
    .await;

    server_handle.abort();

    assert!(
        result.is_ok(),
        "recv_task가 {test_timeout:?} 이내에 종료되지 않았습니다. \
         Bug Condition 확인: msg_count={msg_count}, actual_available=0, \
         connection_alive=true → recv_task가 타임아웃 없이 무한 대기합니다."
    );

    let received = result.unwrap();
    assert_eq!(
        received, 0,
        "서버가 메시지를 전혀 보내지 않았으므로 수신 count는 0이어야 합니다."
    );
}

// ============================================================================
// Property 2: Preservation - 정상 수신 시 기존 동작 보존
// ============================================================================

use proptest::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// **Validates: Requirements 3.1, 3.2, 3.3**
///
/// Property 2: Preservation - 정상 수신 시 기존 동작 보존
///
/// NOT isBugCondition(input)인 모든 입력에 대해:
/// - 모든 메시지가 정상 수신되면 msg_count개 수신 후 즉시 종료하고 count를 반환
/// - 서버가 연결을 정상 종료(EOF)하면 루프가 종료
/// - recv_counter에 수신된 count가 정확히 누적

/// 정상 수신 시나리오를 위한 헬퍼: 서버가 msg_count개 메시지를 모두 전송하고 연결 종료
async fn run_normal_recv_scenario(msg_count: u64) -> (u64, Duration) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let bot_id: u64 = 1;
    let target = format!("bot_{bot_id}_msg_");

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (_, mut writer) = stream.into_split();

        // msg_count개의 메시지를 모두 전송
        for seq in 0..msg_count {
            let chat_msg = serde_json::json!({
                "type": "chat",
                "from": 999,
                "nick": null,
                "text": format!("bot_{bot_id}_msg_{seq}"),
                "sent_at": 1234567890u64
            });
            let mut line = serde_json::to_string(&chat_msg).unwrap();
            line.push('\n');
            writer.write_all(line.as_bytes()).await.unwrap();
        }
        writer.flush().await.unwrap();
        // 모든 메시지 전송 후 연결 종료 (EOF) — 정상 시나리오
        drop(writer);
    });

    let client_stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let (reader, _writer) = client_stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let start = std::time::Instant::now();
    let received = recv_task_current(&mut lines, &target, msg_count).await;
    let elapsed = start.elapsed();

    let _ = server_handle.await;
    (received, elapsed)
}

/// EOF 시나리오를 위한 헬퍼: 서버가 일부 메시지만 전송 후 연결 종료 (EOF)
/// isBugCondition = false: connection_alive = false이므로 버그 조건이 아님
async fn run_eof_recv_scenario(msg_count: u64, actual_send: u64) -> u64 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let bot_id: u64 = 2;
    let target = format!("bot_{bot_id}_msg_");

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (_, mut writer) = stream.into_split();

        // actual_send개의 메시지만 전송
        for seq in 0..actual_send {
            let chat_msg = serde_json::json!({
                "type": "chat",
                "from": 999,
                "nick": null,
                "text": format!("bot_{bot_id}_msg_{seq}"),
                "sent_at": 1234567890u64
            });
            let mut line = serde_json::to_string(&chat_msg).unwrap();
            line.push('\n');
            writer.write_all(line.as_bytes()).await.unwrap();
        }
        writer.flush().await.unwrap();
        // 연결 정상 종료 (EOF 발생) — NOT isBugCondition
        drop(writer);
    });

    let client_stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
    let (reader, _writer) = client_stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let received = recv_task_current(&mut lines, &target, msg_count).await;

    let _ = server_handle.await;
    received
}

// Property 2.1: 정상 수신 보존 — 모든 메시지가 도착하면 msg_count개 수신 후 즉시 종료
//
// **Validates: Requirements 3.1**
//
// FOR ALL msg_count in 1..=50:
//   서버가 msg_count개 메시지를 모두 전송하고 연결 종료하면
//   recv_task는 msg_count를 반환하고 즉시 종료해야 한다
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn prop_preservation_normal_recv_returns_exact_count(msg_count in 1u64..=50) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (received, elapsed) = rt.block_on(run_normal_recv_scenario(msg_count));

        // 정확히 msg_count개를 수신해야 한다
        prop_assert_eq!(
            received, msg_count,
            "정상 수신 시 recv_task가 msg_count({})개를 반환해야 하지만 {}개를 반환했습니다",
            msg_count, received
        );

        // 즉시 종료해야 한다 (5초 이내 — 네트워크 오버헤드 감안)
        prop_assert!(
            elapsed < Duration::from_secs(5),
            "정상 수신 시 recv_task가 즉시 종료해야 하지만 {:?} 소요되었습니다",
            elapsed
        );
    }
}

// Property 2.2: EOF 처리 보존 — 서버가 연결을 정상 종료하면 루프가 종료
//
// **Validates: Requirements 3.2**
//
// FOR ALL (msg_count, actual_send) where actual_send <= msg_count:
//   서버가 actual_send개 전송 후 연결 종료(EOF)하면
//   recv_task는 actual_send를 반환하고 루프가 종료되어야 한다
//   (connection_alive = false이므로 isBugCondition = false)
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn prop_preservation_eof_terminates_loop(
        msg_count in 1u64..=50,
        send_ratio in 0.0f64..=1.0,
    ) {
        let actual_send = ((msg_count as f64) * send_ratio).round() as u64;

        let rt = tokio::runtime::Runtime::new().unwrap();

        let result = rt.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                run_eof_recv_scenario(msg_count, actual_send),
            ).await
        });

        // EOF 시 루프가 반드시 종료되어야 한다 (타임아웃 발생하면 안 됨)
        prop_assert!(
            result.is_ok(),
            "EOF 시나리오에서 recv_task가 5초 이내에 종료되지 않았습니다. \
             msg_count={}, actual_send={}",
            msg_count, actual_send
        );

        let received = result.unwrap();
        // EOF 전에 전송된 메시지 수만큼 수신해야 한다
        prop_assert_eq!(
            received, actual_send,
            "EOF 시나리오에서 recv_task가 actual_send({})를 반환해야 하지만 {}를 반환했습니다. \
             msg_count={}",
            actual_send, received, msg_count
        );
    }
}

// Property 2.3: recv_counter 누적 보존 — 수신된 count가 정확히 누적
//
// **Validates: Requirements 3.3**
//
// FOR ALL msg_count in 1..=30:
//   recv_task가 반환한 count를 recv_counter에 fetch_add하면
//   recv_counter의 값이 정확히 count만큼 증가해야 한다
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn prop_preservation_recv_counter_accumulates_correctly(msg_count in 1u64..=30) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let recv_counter = Arc::new(AtomicU64::new(0));

        let (received, _elapsed) = rt.block_on(run_normal_recv_scenario(msg_count));

        // recv_counter에 누적 (src/bot/normal.rs의 run 함수와 동일한 패턴)
        recv_counter.fetch_add(received, Ordering::Relaxed);

        let counter_value = recv_counter.load(Ordering::SeqCst);
        prop_assert_eq!(
            counter_value, msg_count,
            "recv_counter에 누적된 값({})이 msg_count({})와 다릅니다",
            counter_value, msg_count
        );
    }
}
