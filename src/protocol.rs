use serde::{Deserialize, Serialize};

pub const N_OPTIONS: usize = 4;

/// 클라이언트 → 서버
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// 채팅 메시지
    Chat {
        text: String,
        /// 클라이언트 송신 시각 (Unix ms) — 서버에서 latency 계산 기준
        client_ts: u64,
    },
    /// 투표 (option: 0..N_OPTIONS)
    Vote { option: usize },
    /// 투표 철회
    Unvote,
}

/// 서버 → 클라이언트
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// 신규 접속자에게만 전송 — 접속 시점의 참여자 수 초기값 전달 (이슈 4)
    Welcome { peer_count: u64 },
    /// 브로드캐스트 채팅
    Chat {
        from: u64,
        text: String,
        /// 전송 시각 (Unix ms)
        sent_at: u64,
    },
    /// 투표 현황 스냅샷
    VoteSnapshot { counts: [u64; N_OPTIONS] },
    /// 참여자 입장/퇴장 알림
    Presence { id: u64, joined: bool },
    /// 서버 측 오류 메시지
    Error { msg: String },
}

/// 채널에 흐르는 내부 이벤트 (서버 내부 전용)
#[derive(Debug, Clone)]
pub enum BroadcastEvent {
    Server(ServerMsg),
    /// 서버 종료 신호
    Shutdown,
}

/// 줄 단위(\n) 프레임 코덱에서 사용하는 최대 라인 길이
pub const MAX_LINE_LEN: usize = 65536;
