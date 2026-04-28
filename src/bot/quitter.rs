use tokio::io::BufWriter;
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use crate::protocol::{ClientMsg, N_OPTIONS};
use super::{connect, send_msg};

/// 투표 직후 강제 종료 — stream drop으로 서버 측 정상 cleanup 유발
pub async fn run(id: u64) -> Result<()> {
    let stream = connect().await?;
    let (_reader, writer) = stream.into_split();
    let mut writer = BufWriter::new(writer);

    let option = StdRng::from_entropy().gen_range(0..N_OPTIONS);
    send_msg(&mut writer, &ClientMsg::Vote { option }).await?;

    // writer drop → 소켓 종료
    let _ = id;
    Ok(())
}
