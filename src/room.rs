use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, RwLock};

use crate::protocol::{BroadcastEvent, ServerMsg};

pub const BROADCAST_CAP: usize = 2048;

#[derive(Debug, Clone)]
pub struct ClientMeta {
    pub id: u64,
}

pub struct Room {
    pub tx: broadcast::Sender<BroadcastEvent>,
    pub clients: Arc<RwLock<HashMap<u64, ClientMeta>>>,
}

impl Room {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Arc::new(Self {
            tx,
            clients: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastEvent> {
        self.tx.subscribe()
    }

    pub async fn join(&self, id: u64) {
        self.clients.write().await.insert(id, ClientMeta { id });
        let _ = self.tx.send(BroadcastEvent::Server(ServerMsg::Presence {
            id,
            joined: true,
        }));
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

    pub fn broadcast(&self, msg: ServerMsg) {
        let _ = self.tx.send(BroadcastEvent::Server(msg));
    }
}
