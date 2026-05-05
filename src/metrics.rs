use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use sysinfo::System;
use tokio::sync::oneshot;
use tokio::time;
use tracing::info;

pub struct Metrics {
    recv_count: AtomicU64,
    sent_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            recv_count: AtomicU64::new(0),
            sent_count: AtomicU64::new(0),
        })
    }

    pub fn record_recv(&self) {
        self.recv_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            recv: self.recv_count.load(Ordering::Relaxed),
            sent: self.sent_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub recv: u64,
    pub sent: u64,
}

/// 주기적으로 메트릭 + CPU/메모리 로그 출력.
/// 반환된 `oneshot::Sender`에 값을 보내면(또는 drop하면) 리포터 태스크가 종료된다.
pub fn start_reporter(metrics: Arc<Metrics>, interval: Duration) -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut ticker = time::interval(interval);

        let mut prev_recv: u64 = 0;
        let mut prev_sent: u64 = 0;
        let interval_secs = interval.as_secs_f64();

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = &mut shutdown_rx => break,
            }

            sys.refresh_all();

            let snap = metrics.snapshot();
            let cpu: f32 = sys.global_cpu_info().cpu_usage();
            let mem_mb = sys.used_memory() / 1024 / 1024;

            let recv_mps = ((snap.recv.saturating_sub(prev_recv)) as f64 / interval_secs) as u64;
            let sent_mps = ((snap.sent.saturating_sub(prev_sent)) as f64 / interval_secs) as u64;
            prev_recv = snap.recv;
            prev_sent = snap.sent;

            info!(
                recv = snap.recv,
                sent = snap.sent,
                recv_mps,
                sent_mps,
                cpu_pct = format!("{:.1}", cpu),
                mem_mb,
                "metrics"
            );
        }
    });

    shutdown_tx
}
