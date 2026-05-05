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
const SETTLE_AFTER_BARRIER_MS: u64 = 500;
const SNAPSHOT_PROPAGATION_MS: u64 = 200;

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

    let mut last_vote: Option<usize> = None;
    for _ in 0..vote_count {
        let option = rng.gen_range(0..N_OPTIONS);
        send_msg(&mut writer, &ClientMsg::Vote { option }).await?;
        last_vote = Some(option);
        sleep(Duration::from_millis(10)).await;
    }

    // 모든 fickle 봇이 마지막 투표를 마칠 때까지 동기화
    barrier.wait().await;

    // quitter 등의 disconnect→unvote 전파를 위한 정착 대기
    sleep(Duration::from_millis(SETTLE_AFTER_BARRIER_MS)).await;

    // 동일 옵션 재투표로 net-zero 변화를 주어 fresh VoteSnapshot 브로드캐스트 유도
    if let Some(opt) = last_vote {
        send_msg(&mut writer, &ClientMsg::Vote { option: opt }).await?;
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
