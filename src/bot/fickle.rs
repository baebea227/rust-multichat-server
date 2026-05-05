use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::protocol::{ClientMsg, ServerMsg, N_OPTIONS};
use super::{connect, send_msg, FickleResult};

/// fickle 봇 정합성 측정 단계 동기화 시간
const SETTLE_AFTER_BARRIER_MS: u64 = 1500;
const SNAPSHOT_PROPAGATION_MS: u64 = 1000;

pub async fn run(
    id: u64,
    vote_count: usize,
    barrier: Arc<Barrier>,
) -> Result<FickleResult> {
    let stream = connect().await?;
    let (reader, writer) = stream.into_split();
    let mut writer = BufWriter::new(writer);
    let mut rng = StdRng::from_entropy();

    let last_snapshot: Arc<Mutex<Option<[u64; N_OPTIONS]>>> = Arc::new(Mutex::new(None));
    let snapshot_clone = last_snapshot.clone();

    let recv_task = tokio::spawn(async move {
        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(msg) = serde_json::from_str::<ServerMsg>(&line) {
                if let ServerMsg::VoteSnapshot { counts, .. } = msg {
                    *snapshot_clone.lock().await = Some(counts);
                }
            }
        }
    });

    // 서버의 토큰 버킷(10 token/s, burst 10) 한도에 맞춰 간격을 두지 않으면
    // 봇이 보낸 투표가 서버에서 드롭되어 expected/actual 분포가 어긋난다.
    // 110ms 간격은 정상상태 refill 주기(100ms)보다 약간 여유를 둔 값.
    const VOTE_INTERVAL_MS: u64 = 110;
    let mut last_vote: Option<usize> = None;
    for _ in 0..vote_count {
        let option = rng.gen_range(0..N_OPTIONS);
        send_msg(&mut writer, &ClientMsg::Vote { option }).await?;
        last_vote = Some(option);
        sleep(Duration::from_millis(VOTE_INTERVAL_MS)).await;
    }

    // 모든 fickle 봇이 마지막 투표를 마칠 때까지 동기화
    let wait_result = barrier.wait().await;

    // quitter 등의 disconnect→unvote 전파를 위한 정착 대기
    sleep(Duration::from_millis(SETTLE_AFTER_BARRIER_MS)).await;

    // 동일 옵션 재투표로 net-zero 변화를 주어 fresh VoteSnapshot 브로드캐스트 유도.
    // 봇 수가 많을 때 모두 재투표하면 동시 burst가 broadcast 채널을 lagged 상태로 몰아 일부
    // 봇이 갱신 스냅샷을 놓친다 → leader 한 명만 트리거하고 나머지는 broadcast를 수신만 함.
    //
    // 또한 1회 재투표만 하면 settle 직후 늦게 도착한 stragglers 의 vote가 다음 fresh snapshot
    // 없이 묻혀 actual 합계가 expected보다 작아진다. RE_VOTE_ROUNDS 회 반복하여 매 round 마다
    // fresh snapshot을 트리거함으로써 늦게 처리된 votes 도 결국 broadcast에 실리게 한다.
    const RE_VOTE_ROUNDS: usize = 3;
    const RE_VOTE_INTERVAL_MS: u64 = 300;
    if wait_result.is_leader() {
        if let Some(opt) = last_vote {
            for _ in 0..RE_VOTE_ROUNDS {
                send_msg(&mut writer, &ClientMsg::Vote { option: opt }).await?;
                sleep(Duration::from_millis(RE_VOTE_INTERVAL_MS)).await;
            }
        }
    } else {
        // 비-리더는 리더의 re-vote 윈도우와 동일한 시간을 대기해야 한다.
        // 이 동기화가 없으면 비-리더가 리더의 마지막 재투표 snapshot이 도착하기 전에
        // recv_task를 abort하여 stale snapshot으로 끝나고 → pick_actual_snapshot의
        // majority가 stale 분포로 끌려간다.
        sleep(Duration::from_millis(RE_VOTE_ROUNDS as u64 * RE_VOTE_INTERVAL_MS)).await;
    }

    // 정착 후 스냅샷 전파 대기
    sleep(Duration::from_millis(SNAPSHOT_PROPAGATION_MS)).await;
    recv_task.abort();

    let snapshot = *last_snapshot.lock().await;

    let _ = id;
    Ok(FickleResult {
        last_vote,
        last_snapshot: snapshot,
    })
}
