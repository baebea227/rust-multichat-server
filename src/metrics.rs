use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::System;
use tokio::time;
use tracing::info;

// ── 이슈 8: 히스토그램 버킷 정의 ─────────────────────────────────────────────
// 경계값(ms): [0,1), [1,5), [5,10), [10,50), [50,100), [100,∞)
const BUCKET_BOUNDS: [u64; 5] = [1, 5, 10, 50, 100];
const N_BUCKETS: usize = BUCKET_BOUNDS.len() + 1; // 6개

fn bucket_index(latency_ms: u64) -> usize {
    for (i, &bound) in BUCKET_BOUNDS.iter().enumerate() {
        if latency_ms < bound {
            return i;
        }
    }
    N_BUCKETS - 1
}

pub struct Metrics {
    recv_count: AtomicU64,
    sent_count: AtomicU64,
    latency_sum_ms: AtomicU64,
    latency_count: AtomicU64,
    latency_max: AtomicU64,
    /// 이슈 8: ms 단위 히스토그램 버킷 카운터
    hist: [AtomicU64; N_BUCKETS],
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            recv_count: AtomicU64::new(0),
            sent_count: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_max: AtomicU64::new(0),
            hist: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }

    pub fn record_recv(&self) {
        self.recv_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 이슈 7: client_ts(클라이언트 송신 시각, Unix ms) 기준으로 latency 기록
    /// 이슈 8: 버킷 카운트 증가
    pub fn record_latency(&self, client_ts: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let latency = now.saturating_sub(client_ts);

        self.latency_sum_ms.fetch_add(latency, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // max 갱신 (CAS loop)
        let mut cur = self.latency_max.load(Ordering::Relaxed);
        while latency > cur {
            match self.latency_max.compare_exchange_weak(
                cur,
                latency,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }

        // 이슈 8: 버킷 카운트
        self.hist[bucket_index(latency)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let count = self.latency_count.load(Ordering::Relaxed);
        let avg = if count > 0 {
            self.latency_sum_ms.load(Ordering::Relaxed) / count
        } else {
            0
        };

        // 이슈 8: p99 계산 — 누적 버킷을 순회해 99번째 백분위 버킷 상한을 반환
        let p99_ms = if count > 0 {
            let threshold = (count as f64 * 0.99).ceil() as u64;
            let mut cumulative = 0u64;
            let mut p99 = 0u64;
            for (i, bucket) in self.hist.iter().enumerate() {
                cumulative += bucket.load(Ordering::Relaxed);
                if cumulative >= threshold {
                    // 해당 버킷의 상한을 p99 근사값으로 사용
                    p99 = if i < BUCKET_BOUNDS.len() {
                        BUCKET_BOUNDS[i]
                    } else {
                        self.latency_max.load(Ordering::Relaxed)
                    };
                    break;
                }
            }
            p99
        } else {
            0
        };

        MetricsSnapshot {
            recv: self.recv_count.load(Ordering::Relaxed),
            sent: self.sent_count.load(Ordering::Relaxed),
            avg_latency_ms: avg,
            max_latency_ms: self.latency_max.load(Ordering::Relaxed),
            p99_latency_ms: p99_ms,
        }
    }
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub recv: u64,
    pub sent: u64,
    pub avg_latency_ms: u64,
    pub max_latency_ms: u64,
    /// 이슈 8: p99 latency (ms)
    pub p99_latency_ms: u64,
}

/// 주기적으로 메트릭 + CPU/메모리 로그 출력
pub fn start_reporter(metrics: Arc<Metrics>, interval: Duration) {
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut ticker = time::interval(interval);

        // 이슈 9: 이전 주기 누적값 저장 (delta 계산용)
        let mut prev_recv: u64 = 0;
        let mut prev_sent: u64 = 0;
        let interval_secs = interval.as_secs_f64();

        loop {
            ticker.tick().await;
            sys.refresh_all();

            let snap = metrics.snapshot();
            let cpu: f32 = sys.global_cpu_info().cpu_usage();
            let mem_mb = sys.used_memory() / 1024 / 1024;

            // 이슈 9: 순간 처리량 계산
            let recv_mps = ((snap.recv.saturating_sub(prev_recv)) as f64 / interval_secs) as u64;
            let sent_mps = ((snap.sent.saturating_sub(prev_sent)) as f64 / interval_secs) as u64;
            prev_recv = snap.recv;
            prev_sent = snap.sent;

            info!(
                recv = snap.recv,
                sent = snap.sent,
                recv_mps,
                sent_mps,
                avg_lat = snap.avg_latency_ms,
                p99_lat = snap.p99_latency_ms,
                max_lat = snap.max_latency_ms,
                cpu_pct = format!("{:.1}", cpu),
                mem_mb,
                "metrics"
            );
        }
    });
}
