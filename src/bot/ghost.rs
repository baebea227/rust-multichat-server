use tokio::time::{sleep, Duration};
use anyhow::Result;

use super::{connect, GHOST_HOLD_SECS};

/// 연결만 유지하고 데이터를 수신하지 않음 — broadcast lagged 유발
pub async fn run(id: u64) -> Result<()> {
    let _stream = connect().await?;
    sleep(Duration::from_secs(GHOST_HOLD_SECS)).await;
    let _ = id;
    Ok(())
}
