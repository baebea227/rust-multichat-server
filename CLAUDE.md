# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

500인 동시접속 멀티채팅 서버. tokio 기반 비동기 TCP 서버로, Lock-Free 브로드캐스트, Atomic 투표 집계, 4종 악성 봇 시나리오 테스트를 포함한다.

## Commands

```bash
# 빌드
cargo build

# 릴리즈 빌드 (성능 측정 시 필수)
cargo build --release

# 서버 실행
cargo run -- server

# 봇 테스트 실행 (서버가 먼저 실행 중이어야 함)
cargo run -- bot-test --count 500 --mode normal

# 테스트
cargo test

# 단일 테스트
cargo test <test_name>

# 린트
cargo clippy -- -D warnings

# 포맷
cargo fmt
```

## Architecture

### 핵심 설계 원칙

**상태 관리와 메시지 전달의 완전 분리(Decoupling)**가 핵심이다.
- 메시지 브로드캐스트: `broadcast::channel` — Lock 없이 전송
- 참여자 목록: `Arc<RwLock<HashMap>>` — 입퇴장 시에만 write lock 점유

### 모듈 구조

```
src/
├── main.rs        # 진입점: CLI 파싱 (server / bot-test 모드 분기)
├── server.rs      # TCP accept 루프, 클라이언트 task spawn
├── client.rs      # 클라이언트 핸들러: read_task / write_task 분리 spawn
├── room.rs        # 채팅방 전역 상태 보유
├── vote.rs        # 투표 시스템
├── protocol.rs    # 메시지 타입 (직렬화/역직렬화)
├── metrics.rs     # 성능 지표 수집 및 리포팅
└── bot/
    ├── mod.rs     # 봇 공통 트레이트, 시나리오 실행기
    ├── normal.rs  # 일반 봇
    ├── fickle.rs  # 변덕쟁이 봇
    ├── spammer.rs # 도배 봇
    ├── ghost.rs   # 잠수 봇
    └── quitter.rs # 탈주 봇
```

### 주요 타입

**`Room`** (`room.rs`): 서버 전체에서 `Arc`로 공유되는 채팅방 상태.
- `tx: broadcast::Sender<Arc<Message>>` — 전역 브로드캐스트 채널 송신단
- `clients: Arc<RwLock<HashMap<u64, ClientMeta>>>` — 참여자 메타데이터

**`ClientHandler`** (`client.rs`): 클라이언트 1명당 두 개의 task로 분리.
- `read_task`: 클라이언트 소켓 수신 → `Room::tx.send()`
- `write_task`: `broadcast::Receiver`에서 수신 → 소켓 송신

**`VoteBoard`** (`vote.rs`): `[AtomicUsize; N_OPTIONS]` 배열. CAS 없이 fetch_add/fetch_sub만 사용하므로 lock 불필요.

**`Metrics`** (`metrics.rs`): 메시지마다 타임스탬프 찍어 latency 계산. `AtomicU64`로 처리량 집계. `sysinfo` 크레이트로 CPU/메모리 주기적 폴링.

### 데이터 흐름

```
[Client] --TCP--> read_task --> Room.tx.send(msg)
                                      |
                              broadcast::channel
                               /      |      \
                          rx_A     rx_B     rx_C
                            |        |        |
                        write_task write_task write_task
                            |        |        |
                         [ClientA] [B]     [C]
```

### 봇 테스트 구조

모든 봇은 `bot::Bot` 트레이트를 구현한다. `bot::mod`의 시나리오 실행기가 `--count`, `--mode` 인자에 따라 지정 수의 봇 task를 spawn한다.

메시지 누락 검증: 각 봇은 `bot_{id}_msg_{seq}` 형식으로 고유 ID를 부여해 전송하고, 수신 측에서 전체 집계 후 `expected == received`를 assert한다.

### 주의사항

- `broadcast::channel`의 capacity를 초과하면 오래된 메시지가 드롭된다. 도배 봇 테스트 시 capacity 조정이 필요할 수 있다.
- `ghost` 봇은 수신 버퍼를 비우지 않으므로 broadcast 수신기가 lagged error를 발생시킨다. 이를 정상으로 처리해야 한다.
- 성능 측정은 반드시 `--release` 빌드로 수행한다.
