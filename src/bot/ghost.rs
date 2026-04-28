use tokio::time::{sleep, Duration};
use anyhow::Result;

use super::connect;

/// 연결만 유지하고 데이터를 수신하지 않음 — broadcast lagged 유발
pub async fn run(id: u64, hold_secs: usize) -> Result<()> {
    let _stream = connect().await?;
    sleep(Duration::from_secs(hold_secs as u64)).await;
    let _ = id;
    Ok(())
}
