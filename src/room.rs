use std::{collections::HashMap, sync::Arc};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, RwLock};

use crate::protocol::{BroadcastEvent, ServerMsg};

pub const BROADCAST_CAP: usize = 524288;
/// VoteSnapshot broadcast 최소 간격 (ms). 매 투표마다 브로드캐스트하면
/// 500봇 × 20표 = 10,000개 backlog가 쌓여 write_task가 settle 윈도우 안에
/// 처리하지 못한다 → 50ms 당 최대 1회로 제한 (20Hz, TUI에도 충분)
pub const VOTE_BROADCAST_THROTTLE_MS: u64 = 50;

#[derive(Debug, Clone)]
pub struct ClientMeta {
    pub id: u64,
    /// 닉네임 (이슈 5)
    pub name: Option<String>,
}

pub struct Room {
    pub tx: broadcast::Sender<BroadcastEvent>,
    pub clients: Arc<RwLock<HashMap<u64, ClientMeta>>>,
    /// 마지막 VoteSnapshot broadcast 시각 (Unix ms). throttle 구현용.
    last_vote_broadcast_ms: AtomicU64,
    pending_vote_broadcast: StdMutex<Option<ServerMsg>>,
    vote_flush_scheduled: AtomicBool,
}

impl Room {
    pub fn new() -> Arc<Self> {
        Self::new_with_capacity(BROADCAST_CAP)
    }

    /// 지정된 broadcast 채널 용량으로 Room 생성
    pub fn new_with_capacity(capacity: usize) -> Arc<Self> {
        let (tx, _) = broadcast::channel(capacity);
        Arc::new(Self {
            tx,
            clients: Arc::new(RwLock::new(HashMap::new())),
            last_vote_broadcast_ms: AtomicU64::new(0),
            pending_vote_broadcast: StdMutex::new(None),
            vote_flush_scheduled: AtomicBool::new(false),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastEvent> {
        self.tx.subscribe()
    }

    /// 입장 처리 후 현재 참여자 수 반환 (이슈 4: Welcome 전송용)
    pub async fn join(&self, id: u64) -> u64 {
        let mut clients = self.clients.write().await;
        clients.insert(id, ClientMeta { id, name: None });
        let count = clients.len() as u64;
        drop(clients);
        let _ = self.tx.send(BroadcastEvent::Server(ServerMsg::Presence {
            id,
            joined: true,
        }));
        count
    }

    pub async fn leave(&self, id: u64) {
        self.clients.write().await.remove(&id);
        let _ = self.tx.send(BroadcastEvent::Server(ServerMsg::Presence {
            id,
            joined: false,
        }));
    }

    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// 닉네임 설정 (이슈 5)
    pub async fn set_nick(&self, id: u64, name: String) {
        if let Some(meta) = self.clients.write().await.get_mut(&id) {
            meta.name = Some(name);
        }
    }

    pub fn broadcast(&self, msg: ServerMsg) {
        let _ = self.tx.send(BroadcastEvent::Server(msg));
    }

    /// VoteSnapshot 전용 throttled broadcast.
    /// VOTE_BROADCAST_THROTTLE_MS 간격 안에 이미 broadcast가 나갔으면 스킵.
    /// compare_exchange로 슬롯을 선점한 쪽만 실제 전송하여 burst를 억제.
    pub fn broadcast_vote_throttled(self: &Arc<Self>, msg: ServerMsg) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.last_vote_broadcast_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) >= VOTE_BROADCAST_THROTTLE_MS {
            if self.last_vote_broadcast_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                let _ = self.tx.send(BroadcastEvent::Server(msg));
                return;
            }
        }

        if let Ok(mut pending) = self.pending_vote_broadcast.lock() {
            *pending = Some(msg);
        }
        self.schedule_vote_flush();
    }

    fn schedule_vote_flush(self: &Arc<Self>) {
        if self.vote_flush_scheduled.swap(true, Ordering::Relaxed) {
            return;
        }

        let room = self.clone();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.last_vote_broadcast_ms.load(Ordering::Relaxed);
        let delay_ms = VOTE_BROADCAST_THROTTLE_MS
            .saturating_sub(now_ms.saturating_sub(last))
            .max(1);

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            room.flush_pending_vote_snapshot();
        });
    }

    fn flush_pending_vote_snapshot(&self) {
        self.vote_flush_scheduled.store(false, Ordering::Relaxed);

        let pending = self
            .pending_vote_broadcast
            .lock()
            .ok()
            .and_then(|mut pending| pending.take());

        if let Some(msg) = pending {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.last_vote_broadcast_ms.store(now_ms, Ordering::Relaxed);
            let _ = self.tx.send(BroadcastEvent::Server(msg));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::protocol::N_OPTIONS;

    use super::*;

    fn vote_snapshot(counts: [u64; N_OPTIONS]) -> ServerMsg {
        let total: u64 = counts.iter().sum();
        let percentages = std::array::from_fn(|i| {
            if total == 0 {
                0.0
            } else {
                counts[i] as f32 / total as f32
            }
        });
        ServerMsg::VoteSnapshot {
            counts,
            percentages,
        }
    }

    #[tokio::test]
    async fn throttled_vote_broadcast_flushes_latest_pending_snapshot() {
        let room = Room::new_with_capacity(16);
        let mut rx = room.subscribe();

        room.broadcast_vote_throttled(vote_snapshot([1, 0, 0, 0]));
        let first = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("first snapshot timed out")
            .expect("broadcast channel closed");

        assert!(matches!(
            first,
            BroadcastEvent::Server(ServerMsg::VoteSnapshot {
                counts: [1, 0, 0, 0],
                ..
            })
        ));

        room.broadcast_vote_throttled(vote_snapshot([0, 0, 0, 0]));
        let flushed = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("pending snapshot timed out")
            .expect("broadcast channel closed");

        assert!(matches!(
            flushed,
            BroadcastEvent::Server(ServerMsg::VoteSnapshot {
                counts: [0, 0, 0, 0],
                ..
            })
        ));
    }
}
