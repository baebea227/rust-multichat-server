use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::protocol::N_OPTIONS;

pub struct VoteBoard {
    counts: [AtomicI64; N_OPTIONS],
}

impl VoteBoard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            counts: std::array::from_fn(|_| AtomicI64::new(0)),
        })
    }

    /// 투표. 이전 선택이 있으면 먼저 철회 후 새 옵션에 추가.
    pub fn vote(&self, prev: Option<usize>, next: usize) {
        if let Some(p) = prev {
            if p < N_OPTIONS {
                self.counts[p].fetch_add(-1, Ordering::Relaxed);
            }
        }
        if next < N_OPTIONS {
            self.counts[next].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn unvote(&self, prev: usize) {
        if prev < N_OPTIONS {
            self.counts[prev].fetch_add(-1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> [u64; N_OPTIONS] {
        std::array::from_fn(|i| self.counts[i].load(Ordering::Relaxed).max(0) as u64)
    }

    /// 이슈 6: counts와 percentages(0.0~1.0)를 함께 반환
    pub fn snapshot_with_percentages(&self) -> ([u64; N_OPTIONS], [f32; N_OPTIONS]) {
        let counts = self.snapshot();
        let total: u64 = counts.iter().sum();
        let percentages = std::array::from_fn(|i| {
            if total > 0 {
                counts[i] as f32 / total as f32
            } else {
                0.0
            }
        });
        (counts, percentages)
    }
}
