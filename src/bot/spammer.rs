use tokio::io::BufWriter;
use anyhow::Result;

use crate::protocol::ClientMsg;
use super::{connect, send_msg};

/// 초당 수백 패킷: 딜레이 없이 msg_count개 전송
pub async fn run(id: u64, msg_count: usize) -> Result<()> {
    let stream = connect().await?;
    let (_reader, writer) = stream.into_split();
    let mut writer = BufWriter::new(writer);

    for seq in 0..msg_count {
        send_msg(&mut writer, &ClientMsg::Chat {
            text: format!("spam_{id}_{seq}"),
        }).await?;
    }

    Ok(())
}
