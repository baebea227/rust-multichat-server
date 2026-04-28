use tokio::io::BufWriter;
use tokio::time::{sleep, Duration};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use crate::protocol::{ClientMsg, N_OPTIONS};
use super::{connect, send_msg};

pub async fn run(id: u64, vote_count: usize) -> Result<()> {
    let stream = connect().await?;
    let (_reader, writer) = stream.into_split();
    let mut writer = BufWriter::new(writer);
    let mut rng = StdRng::from_entropy();

    for _ in 0..vote_count {
        let option = rng.gen_range(0..N_OPTIONS);
        send_msg(&mut writer, &ClientMsg::Vote { option }).await?;
        // 10ms 간격으로 투표 변경
        sleep(Duration::from_millis(10)).await;
    }

    let _ = id; // id는 로그용 예약
    Ok(())
}
