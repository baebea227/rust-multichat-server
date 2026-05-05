use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, RwLock};

use crate::protocol::{BroadcastEvent, ServerMsg};

pub const BROADCAST_CAP: usize = 32768;

#[derive(Debug, Clone)]
pub struct ClientMeta {
    pub id: u64,
    /// 닉네임 (이슈 5)
    pub name: Option<String>,
}

pub struct Room {
    pub tx: broadcast::Sender<BroadcastEvent>,
    pub clients: Arc<RwLock<HashMap<u64, ClientMeta>>>,
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
}
