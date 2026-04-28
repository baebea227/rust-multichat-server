use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::System;
use tokio::time;
use tracing::info;

pub struct Metrics {
    recv_count: AtomicU64,
    sent_count: AtomicU64,
    latency_sum_ms: AtomicU64,
    latency_count: AtomicU64,
    /// p99 근사: 최근 수신 latency 버킷 (ms 단위, 최대값 추적)
    latency_max: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            recv_count: AtomicU64::new(0),
            sent_count: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_max: AtomicU64::new(0),
        })
    }

    pub fn record_recv(&self) {
        self.recv_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
    }

    /// sent_at: 메시지 생성 시각 (Unix ms). 서버 수신 시점과의 차이를 latency로 기록.
    pub fn record_latency(&self, sent_at: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let latency = now.saturating_sub(sent_at);
        self.latency_sum_ms.fetch_add(latency, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        // max 갱신 (CAS loop)
        let mut cur = self.latency_max.load(Ordering::Relaxed);
        while latency > cur {
            match self.latency_max.compare_exchange_weak(cur, latency, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let count = self.latency_count.load(Ordering::Relaxed);
        let avg = if count > 0 {
            self.latency_sum_ms.load(Ordering::Relaxed) / count
        } else {
            0
        };
        MetricsSnapshot {
            recv: self.recv_count.load(Ordering::Relaxed),
            sent: self.sent_count.load(Ordering::Relaxed),
            avg_latency_ms: avg,
            max_latency_ms: self.latency_max.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub recv: u64,
    pub sent: u64,
    pub avg_latency_ms: u64,
    pub max_latency_ms: u64,
}

/// 주기적으로 메트릭 + CPU/메모리 로그 출력
pub fn start_reporter(metrics: Arc<Metrics>, interval: Duration) {
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut ticker = time::interval(interval);
        loop {
            ticker.tick().await;
            sys.refresh_all();

            let snap = metrics.snapshot();
            let cpu: f32 = sys.global_cpu_info().cpu_usage();
            let mem_mb = sys.used_memory() / 1024 / 1024;

            info!(
                recv = snap.recv,
                sent = snap.sent,
                avg_lat = snap.avg_latency_ms,
                max_lat = snap.max_latency_ms,
                cpu_pct = format!("{:.1}", cpu),
                mem_mb,
                "metrics"
            );
        }
    });
}
