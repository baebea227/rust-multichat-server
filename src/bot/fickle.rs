use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tokio::time::{sleep, Duration};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::protocol::{ClientMsg, ServerMsg, N_OPTIONS};
use super::{connect, send_msg, FickleResult};

pub async fn run(id: u64, vote_count: usize) -> Result<FickleResult> {
    let stream = connect().await?;
    let (reader, writer) = stream.into_split();
    let mut writer = BufWriter::new(writer);
    let mut rng = StdRng::from_entropy();

    let last_snapshot: Arc<Mutex<Option<[u64; N_OPTIONS]>>> = Arc::new(Mutex::new(None));
    let snapshot_clone = last_snapshot.clone();

    // 수신 태스크: VoteSnapshot 저장
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

    // 투표 루프: 마지막 투표 옵션 추적
    let mut last_vote: Option<usize> = None;
    for _ in 0..vote_count {
        let option = rng.gen_range(0..N_OPTIONS);
        send_msg(&mut writer, &ClientMsg::Vote { option }).await?;
        last_vote = Some(option);
        // 10ms 간격으로 투표 변경
        sleep(Duration::from_millis(10)).await;
    }

    // 마지막 VoteSnapshot 수신 대기
    sleep(Duration::from_millis(200)).await;
    recv_task.abort();

    let snapshot = last_snapshot.lock().await.clone();

    let _ = id; // id는 로그용 예약
    Ok(FickleResult {
        last_vote,
        last_snapshot: snapshot,
    })
}
