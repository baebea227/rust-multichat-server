# RustChat 프로젝트 보고서

> 국민대학교 시스템최신기술 26-1 Part 2 / C조 도전과제
> Rust 기반 500인 멀티채팅 서버

## 1. 프로젝트 개요

### 1.1. 목표

Rust의 비동기 런타임 `tokio`를 기반으로 **500인 동시접속을 안정적으로 처리하는 멀티채팅 서버**를 구현합니다.
단순한 메시지 전달을 넘어, **실시간 투표 시스템**을 통해 Write-Heavy 환경에서도 Race Condition 없이 정합성을 보장함을 증명합니다.

### 1.2. 기술 스택

- **언어**: Rust (edition 2024)
- **비동기 런타임**: `tokio`
- **프레이밍**: `tokio-util` LinesCodec (`\n` 단위)
- **직렬화**: `serde` + `serde_json`
- **TUI 모니터링**: `ratatui` + `crossterm`
- **자원 모니터링**: `sysinfo`
- **테스트**: `proptest`, 통합 테스트(`tests/`)

## 2. 팀 구성 및 역할

| GitHub | 이름 | 메인 역할 | 담당 파일 |
| -------- | ------ | ------ | ------ |
| @baebea227 | 배상혁 | 서버 / 클라이언트 | `server.rs`, `client.rs` |
| @Goldbori | 선현승 | 봇 / 통합 테스트 | `bot/`, `tests/` |
| @zouea4879 | 손승완 | 메트릭 / 프로토콜 | `metrics.rs`, `protocol.rs` |
| @syeon111 | 박수연 | 방 관리 / 투표 | `room.rs`, `vote.rs`, `README.md` |

### 배상혁 @baebea227 — 서버 / 클라이언트 (Server Core)

서버 코어의 동시성 모델 설계와 안정화, race condition 추적 / 수정을 전담했습니다.

- **초기 스캐폴드 작성** — `tokio` 기반 채팅 서버 골격, 모듈 분리(`server`, `client`, `room`, `vote`, `protocol`, `metrics`, `bot`, `tui`)
- **연결 상한 강제** — `MAX_CONNECTIONS = 500` 초과 접속 거절 → 단순 카운터에서 `Arc<Semaphore>` `try_acquire_owned()` 기반으로 교체해 `load → check → add` race 제거
- **Graceful Shutdown** — SIGINT 수신 시 `BroadcastEvent::Shutdown` 전파, 클라이언트 task `JoinHandle` 전체 await, 메트릭 리포터 별도 shutdown 신호
- **투표 가시 race 차단** — `vote()`의 두 `fetch_add` 순서를 `(next +1, prev -1)`로 뒤집고 `snapshot()`에 `.max(0)` 클램프 적용 → 음수 노출 race 제거
- **종료된 client task reap** — `handles.retain(|h| !h.is_finished())`로 장기 가동 메모리 누수 방지
- **DoS 내성** — LinesCodec read 단계에서 라인 길이 강제 제한(`MAX_LINE_LEN = 65536`)
- **Disconnect 경로 보강** — disconnect 시 `VoteSnapshot` 브로드캐스트 누락 수정, `record_recv` 위치 조정 + 소켓 읽기 오류 로깅
- **봇 정합성 안정화** — fickle leader 재투표 N회 반복 + 비-리더 동기 대기로 straggler vote 누락 완화, Barrier 동기화 + fresh snapshot 트리거 도입

### 선현승 @Goldbori — 봇 / 통합 테스트 (Adversary & QA)

서버를 깨뜨리는 봇 시나리오와 통합 테스트 골격을 담당했습니다.

- **봇 혼합 모드 추가** — 한 프로세스에서 normal / fickle / spammer 등 다종 봇을 비율로 섞어 실행하는 mixed 모드
- **통합 테스트 골격 작성** — `tests/stress_test.rs` 신규 + `src/lib.rs` 노출로 binary와 테스트가 동일 모듈 공유
- **결과 리포트 출력** — 봇 측 누락률 / 정합성 PASS-FAIL 리포팅 포맷 정의
- **투표 결과 항목 추가** — 봇 측 검증 로직에 옵션별 카운트 비교 항목 추가
- **타임아웃 테스트** — `tests/recv_task_timeout_test.rs` 신규로 수신 태스크의 행 방지 보장

### 손승완 @zouea4879 — 메트릭 / 프로토콜 (Observability & Protocol)

지연·처리량 측정 인프라와 그에 필요한 프로토콜 확장을 담당했습니다.

- **Latency 측정 기준 수정** — 기존 서버 수신 시점 기준(`sent_at = now_ms()`)이 항상 ~0ms로 측정되던 버그를 수정. `ClientMsg::Chat`에 `client_ts: u64` 필드를 추가하고 클라이언트 송신 시각 기준으로 전환. `protocol.rs` / `client.rs` / `bot` / `tui.rs` 일괄 수정
- **p99 latency 측정** — `metrics.rs`에 6단 버킷 정의(`[0,1)` ~ `[100,∞)` ms), 누적 버킷 순회로 p99 근사값 계산, `MetricsSnapshot`에 `p99_latency_ms` 필드 추가
- **순간 처리량(msg/s) 측정** — 매 tick마다 recv/sent 누적값 delta로 `recv_mps` / `sent_mps` 계산해 리포터에 출력
- **README 타이틀 정리**

### 박수연 @syeon111 — 방관리 / 투표 (Room & Vote)

`Room`의 사용자 노출 메시지(Welcome / 닉네임 / VoteSnapshot)와 `protocol-room` PR 라인을 담당했습니다. 영역 경계에서 빠지기 쉬운 사용자 노출 정합성을 채웠습니다.

- **README 초안 작성** — 프로젝트 소개, 기술 스택, 테스트 전략, 실행 방법 문서화
- **Welcome 메시지** — `ServerMsg::Welcome { peer_count }` 신설, `Room::join()`이 입장 처리 후 현재 참여자 수를 반환하도록 변경. 신규 접속자가 입장 직후 자기만 받는 1회성 메시지로 현재 인원 즉시 인지 가능
- **닉네임 지원** — `ClientMsg::SetNick { name }` 추가, `ClientMeta`에 `name: Option<String>` 필드 + `Room::set_nick()` 메서드, `ServerMsg::Chat`에 `nick: Option<String>` 필드 추가 (닉네임 없으면 ID로 fallback)
- **VoteSnapshot percentages 필드** — `VoteSnapshot`에 `percentages: [f32; N_OPTIONS]` 추가, `VoteBoard::snapshot_with_percentages()` 신설로 서버에서 비율을 한 번 계산해 전송. 클라이언트 측 중복 연산 제거
- **VoteSnapshot broadcast throttle** — 500봇 환경에서 매 vote마다 broadcast가 채널을 폭주시켜 정합성 측정이 깨지는 문제를 일정 간격 throttling으로 해결
- **protocol-room PR 통합 머지** — `protocol.rs`와 `room.rs`가 동시에 손이 가야 하는 변경들을 묶어 통합 사이클 책임

## 3. 시스템 아키텍처

![RustChat 핵심 동시성 구조](./diagram.png)

위 다이어그램은 클라이언트별 read/write task 분리, AtomicI64 기반 투표 집계, CAS 기반 VoteSnapshot broadcast throttle이 어떻게 연결되는지 보여 줍니다.

### 3.1. 모듈 구성

```text
src/
├── main.rs        — 진입점, CLI 파싱(clap)
├── server.rs      — TcpListener, Semaphore 기반 연결 상한 제어
├── client.rs      — per-connection 핸들러 (read/write 분리)
├── room.rs        — broadcast 채널 + 참여자 메타데이터(RwLock)
├── vote.rs        — Atomic 카운터 기반 투표 집계
├── protocol.rs    — JSON 메시지 스키마 (ClientMsg / ServerMsg)
├── metrics.rs     — 처리량/지연/누락률 측정
├── tui.rs         — 실시간 상태 대시보드
└── bot/           — 5종 봇 (normal, fickle, spammer, ghost, quitter)
```

### 3.2. 클라이언트 처리 구조 — read/write task 분리

[client.rs](../src/client.rs)에서는 `stream.into_split()`으로 TCP 스트림을 reader와 writer로 분리하고, writer 쪽은 별도 `tokio::spawn` task로 실행합니다. 덕분에 클라이언트의 송신 대기와 수신 처리가 서로를 막지 않으며, broadcast 메시지를 받는 동안에도 입력 처리가 독립적으로 진행됩니다.

이 구조는 500명 동시 접속 상황에서 특정 클라이언트의 느린 write가 read loop 전체를 지연시키는 문제를 줄여 줍니다.

### 3.3. 핵심 설계 원칙: **상태와 전달의 분리**

500명에게 메시지를 전달하는 동안 참여자 목록의 Lock을 잡고 있으면 입장/퇴장이 블로킹되어 심각한 병목이 발생합니다. 이를 해결하기 위해 다음과 같이 책임을 분리했습니다.

| 책임 | 자료구조 | Lock 전략 |
| ------ | --------- | ---------- |
| 메시지 브로드캐스트 | `tokio::sync::broadcast::Sender` | **Lock-Free** — `tx.send()` 1회로 N명에게 전달 |
| 참여자 목록 관리 | `Arc<RwLock<HashMap<u64, ClientMeta>>>` | 입장/퇴장 시에만 짧게 Write Lock 점유 |
| 투표 집계 | `[AtomicI64; N_OPTIONS]` | **Lock-Free** — `fetch_add`만 사용 |

[room.rs](../src/room.rs)에서 두 책임이 같은 구조체 `Room`에 묶여 있지만, `tx`는 broadcast 채널이고 `clients`는 RwLock으로 분리되어 서로 간섭하지 않습니다.

### 3.4. 연결 상한 제어 — Semaphore

[server.rs:32-56](../src/server.rs#L32-L56)에서 `Arc<Semaphore>`의 `try_acquire_owned()`로 연결 상한(500)을 원자적으로 강제합니다.

- `load → check → add` 분리 패턴은 race를 발생시키므로, Semaphore 한 번의 호출로 점유/거절을 결정합니다.
- 거절된 연결에는 즉시 에러 메시지를 보내고 종료합니다.
- 클라이언트 task가 패닉으로 죽어도 `_permit` drop으로 자동 반납됩니다.
- accept 단계 burst 흡수를 위해 [server.rs](../src/server.rs)에서 `TcpSocket`로 직접 바인딩 후 **listen backlog를 8192로 확장**합니다 ([커밋 99474af](../#)). 500봇이 동시에 SYN을 던지면 OS 기본 backlog가 가득 차 ECONNREFUSED가 즉시 회신되는 문제를 방지합니다.

## 4. 핵심 기술 이슈와 해결

### 4.1. 투표 집계의 가시 race — 음수 노출 차단

`vote(prev, next)`는 두 번의 `fetch_add` 호출로 구성됩니다. 두 호출 사이에 `snapshot()`이 끼어들면 일시적으로 카운트가 어긋납니다.

구현은 [vote.rs](../src/vote.rs)의 `counts: [AtomicI64; N_OPTIONS]` 필드를 중심으로 구성했습니다. 각 선택지 카운터를 독립적인 atomic 값으로 두고, 투표 변경 시 `fetch_add(+1)`을 먼저 수행한 뒤 이전 선택지에 `fetch_add(-1)`을 적용합니다.

**해결** ([vote.rs:21-30](../src/vote.rs#L21-L30)):

- 순서를 `(next +1, prev -1)`로 두어 옵저버가 보는 일시 상태를 항상 **over-count(과집계)** 로 만든다.
- `snapshot()`에서 `.max(0)` 클램프와 결합해 음수가 외부로 노출되는 race를 차단.
- 총합은 잠시 +1이 되었다가 회복되지만, **음수는 절대 보이지 않습니다**.

### 4.2. 변덕쟁이 봇(fickle)과 element-wise 정합성

500명이 0.01초 단위로 투표를 바꾸는 환경에서 단순 총합 비교는 의미가 없습니다. 봇이 보낸 마지막 투표 분포와 서버 집계가 **옵션별로 정확히 일치**해야 정합성이 보장됩니다.

**해결**:

- Barrier 동기화 + fresh snapshot 트리거로 측정 시점을 정렬 ([커밋 d54f16e](../#)).
- 리더 봇이 N회 재투표를 반복하고 비-리더는 동기 대기 ([커밋 a6357ae](../#)).

### 4.3. Disconnect 시 VoteSnapshot 누락

기존 코드는 클라이언트 disconnect 시 unvote만 수행하고 스냅샷 브로드캐스트를 하지 않아, 다른 클라이언트는 변경을 인지하지 못했습니다. [커밋 2cca813](../#)에서 disconnect 경로에도 스냅샷 송신을 추가했습니다.

### 4.4. 종료된 task 핸들 reap

장기 가동 시 `tokio::spawn`으로 만든 핸들이 누적되면 메모리 누수의 원인이 됩니다.
[server.rs:72](../src/server.rs#L72)에서 매 accept 마다 `handles.retain(|h| !h.is_finished())`로 회수합니다.

### 4.5. DoS 내성 — 라인 길이 제한

LinesCodec read 단계에서 라인 길이를 강제 제한해 거대한 입력으로 메모리를 고갈시키는 공격을 차단합니다 ([커밋 faabcb1](../#)). `MAX_LINE_LEN = 65536`바이트.

### 4.6. Graceful Shutdown

- SIGINT 수신 시 `BroadcastEvent::Shutdown`을 모든 클라이언트에 전파.
- 서버는 미완료 클라이언트 task의 `JoinHandle`을 모두 await ([커밋 34dfbad](../#)).
- 메트릭 리포터 태스크도 별도 shutdown 신호로 정리 ([커밋 6079e07](../#)).

## 5. 성능 개선

### 5.1. Broadcast 채널 용량 확장

[room.rs](../src/room.rs)의 `tokio::sync::broadcast` 채널은 lagged receiver를 detect하면 메시지를 drop합니다. 500봇이 동시에 송신하면 일부 클라이언트의 write_task가 일시적으로 처지면서 drop이 발생하므로, capacity exploration 테스트(§7.4.5)를 돌려 한계점을 추적하고 단계적으로 상향했습니다.

- **32,768 → 131,072** ([커밋 b1f4069](../#), [0c9ffbf](../#)) — 500봇 × 50–100 메시지 구간에서 drop 제거.
- **131,072 → 524,288** ([커밋 baa4091](../#)) — 500봇 × 300 메시지 구간 zero-drop 확보.

### 5.2. VoteSnapshot broadcast throttle + pending flush

500봇 환경에서는 매 vote마다 `VoteSnapshot`을 broadcast하면 채널에 너무 많은 갱신이 몰려 정합성 측정 자체가 흔들릴 수 있습니다. 이를 막기 위해 [room.rs](../src/room.rs)의 `last_vote_broadcast_ms: AtomicU64`를 기준으로 50ms 단위 broadcast 슬롯을 둡니다.

각 vote 처리 시 현재 시각과 마지막 broadcast 시각을 비교하고, 50ms 이상 지난 경우에만 `compare_exchange`로 슬롯 선점을 시도합니다. CAS에 성공한 task만 snapshot을 전송하고, 나머지는 이미 다른 task가 최신 broadcast를 맡았다고 보고 스킵합니다. 이 방식은 별도 lock 없이 broadcast 빈도를 제한하면서도 최신 상태 전파는 유지합니다 ([커밋 e33cc16](../#)).

다만 단순 throttle만 적용하면 **마지막 vote가 영영 누락**되는 문제가 남습니다. 특히 봇 퇴장 시 발생하는 unvote가 throttle 윈도우 끝자락에 떨어지면 클라이언트는 옛 카운트를 그대로 보게 됩니다. 이를 막기 위해 `pending_vote_broadcast: StdMutex<Option<ServerMsg>>` 와 `vote_flush_scheduled: AtomicBool`을 두어 — throttle에 막힌 최신 스냅샷을 저장해 두고, 다음 슬롯에 자동 flush하는 지연 전송 경로를 추가했습니다 ([커밋 d551de5](../#)). throttle은 빈도만 줄이고, **최신 상태는 절대 버리지 않습니다**.

### 5.3. TCP accept burst 흡수

500봇이 동시에 connect SYN을 던질 때 OS 기본 backlog(보통 128~256)를 초과하면 커널이 즉시 RST를 회신해 ECONNREFUSED가 산발적으로 발생합니다 ([커밋 99474af](../#)).

- **listen backlog 8192** — `TcpSocket`을 직접 만들어 `listen(8192)`로 backlog를 확장. SYN burst를 커널 큐 단계에서 흡수.
- **봇 connect 재시도** — 그래도 backlog가 일시 포화되면 `bot::connect()`가 ECONNREFUSED를 catch해 **지수 백오프 + 지터로 최대 8회 재시도**. Windows에서 backlog 포화 시 즉시 RST가 회신되는 특성을 흡수.

## 6. 테스트 전략

### 6.1. 봇 5종

| 봇 | 목적 | 검증 항목 |
| ---- | ------ | --------- |
| normal | 일반 채팅 부하 | 누락률 0% |
| fickle | 변덕쟁이 투표 | 옵션별 정합성 |
| spammer | 도배 공격 | 서버 크래시 없음 |
| ghost | 잠수 클라이언트 | 메모리 누수 없음 |
| quitter | 비정상 종료 | 서버 패닉 없음 |

### 6.2. 통합 테스트

- [tests/stress_test.rs](../tests/stress_test.rs) — 500인 부하
- [tests/recv_task_timeout_test.rs](../tests/recv_task_timeout_test.rs) — 수신 task 타임아웃

### 6.3. Property-based Testing

`proptest` 1.x를 dev-dependency로 도입하여, 무작위 입력에 대한 invariant 검증을 가능하게 합니다.

### 6.4. 실제 테스트 실행 항목

아래 항목은 발표 및 재현을 위해 실제로 실행 가능한 시나리오와 명령어를 정리한 것입니다. `bot-test` 명령은 별도 터미널에서 서버가 먼저 실행 중이어야 합니다.

#### 수동 실행 확인

서버와 TUI 클라이언트를 각각 실행하여 기본 접속, 채팅 송수신, 종료 흐름을 확인합니다.

```bash
cargo run -- server
cargo run -- client
```

`count 499` 봇 시나리오는 수동 클라이언트 1명과 봇 499명을 합쳐 총 500 접속 상태를 확인하기 위한 구성입니다.

#### 봇 시나리오 검증

일반 채팅, 투표 정합성, 혼합 적대 상황을 서버에 연결된 봇으로 검증합니다.

```bash
# 일반 봇 채팅 누락률 확인
cargo run -- bot-test --mode normal --count 499 --msg 10

# 변덕쟁이 봇 투표 정합성 확인
cargo run -- bot-test --mode fickle --count 499 --msg 100

# normal / spammer / fickle / ghost / quitter 혼합 적대 시나리오
cargo run -- bot-test --mode mixed --count 499 --msg 50 --ratio "normal:40,spammer:20,fickle:20,ghost:10,quitter:10"
```

#### 자동화 테스트

테스트 목록을 먼저 확인한 뒤, 통합 부하 테스트와 수신 task 타임아웃 회귀 테스트를 실행합니다.

```bash
# 전체 테스트 목록 확인
cargo test -- --list

# 통합 부하 테스트 전체 실행
cargo test --test stress_test -- --nocapture

# recv task timeout 회귀 테스트
cargo test --test recv_task_timeout_test -- --nocapture
```

#### 고부하 및 한계 탐색 테스트

빠른 검증에는 `stress_500` 또는 `stress_500_msg_200`처럼 정규 테스트를 사용합니다. `stress_500_msg_300`, `stress_500_msg_400`은 현재 zero-drop 목표를 넘는 capacity exploration 용도의 `#[ignore]` 테스트이므로 한계 탐색 시에만 별도로 실행합니다.

```bash
# 특정 고부하 정규 테스트
cargo test stress_500 --test stress_test -- --nocapture
cargo test stress_500_msg_200 --test stress_test -- --nocapture

# ignored 한계 탐색 테스트
cargo test stress_500_msg_300 --test stress_test -- --ignored --nocapture
cargo test stress_500_msg_400 --test stress_test -- --ignored --nocapture
```

## 7. 메트릭

서버 측 ([metrics.rs](../src/metrics.rs))과 봇/통합 테스트 측([bot/mod.rs](../src/bot/mod.rs), [tests/stress_test.rs](../tests/stress_test.rs))에서 측정 지표를 분담해 수집합니다.

### 7.1. 서버 측 메트릭 (`Metrics` + `start_reporter`)

서버는 `Arc<Metrics>`에 `recv_count` / `sent_count` 두 개의 `AtomicU64`를 두고, `client.rs`의 read/write 경로에서 각각 `record_recv()` / `record_sent()`로 누적합니다. `start_reporter`가 일정 tick마다 다음을 출력합니다.

- **`recv` / `sent`** — 누적 카운터. 시작 이후 서버가 받은/보낸 라인 수.
- **`recv_mps` / `sent_mps`** — 직전 tick과의 delta를 interval로 나눈 **순간 처리량(msg/s)**. 누적값만 보면 서버가 멈춰도 큰 수가 그대로 보이므로 delta 기반 MPS를 함께 출력합니다.
- **`cpu_pct`** — `sysinfo::System::global_cpu_info().cpu_usage()` (호스트 전체 CPU 사용률 %).
- **`mem_mb`** — 호스트 used memory (MB).

### 7.2. 봇/시나리오 측 메트릭 (`ScenarioReport`)

`bot-test` CLI가 종료 시 출력하는 `=== Scenario Report ===` 블록입니다.

- **`mode`** — `normal` / `fickle` / `spammer` / `ghost` / `quitter` / `mixed`.
- **`total_bots` / `success` / `failure`** — 모든 봇 task에 대해 `JoinHandle::await` 결과를 집계. `Ok(Ok(()))`만 success.
- **`elapsed`** — 시나리오 시작부터 모든 봇 종료까지 wall-clock 시간(초).
- **`avg_rtt`** — `RttCounter`(lock-free `AtomicU64` 합/카운트) 평균. 봇이 자기 메시지를 echo로 받을 때 `now_ms() - client_ts`로 RTT를 기록합니다. `client_ts` echo 방식은 wall-clock 차이에 영향받지 않고 발신자 단방향 RTT를 정확히 측정합니다 ([커밋 4fb4c54](../#)).
- **`vote_integrity`** — fickle 봇이 있는 모드에서만 측정. 봇이 마지막으로 보낸 투표(`tally_votes`로 옵션별 집계)를 **expected**, 다수 봇이 동일하게 본 마지막 `VoteSnapshot`(`pick_actual_snapshot` majority vote)을 **actual**로 두고 element-wise 비교. 단순 총합이 아니라 옵션별 분포까지 정확히 일치해야 PASS.

### 7.3. 통합 부하 테스트 측 메트릭 (`Stress Report`)

`tests/stress_test.rs`가 출력하는 `=== Stress Report ===` 블록입니다.

- **`expected` / `received` / `dropped` / `drop_rate`** — 봇이 보낸 자기 메시지 중 자기에게 echo로 돌아온 수를 매칭(봇 ID + 시퀀스 번호 패턴). zero-drop 목표를 `assert_eq!`로 강제합니다.
- **`elapsed` / `throughput`** — 시나리오 wall-clock과 `received / elapsed_secs`로 계산한 평균 throughput(msg/s).
- **`avg_rtt` / `p95_rtt` / `p99_rtt`** — 봇 측에서 메시지마다 `client_ts` echo로 측정한 RTT를 모두 모아 정렬 후 percentile 계산. `((n-1) * p).ceil()` 인덱스로 근사. 평균만 보면 long-tail이 가려지므로 p95/p99를 함께 출력합니다.

### 7.4. 실제 측정 결과

#### 7.4.1. `bot-test --mode normal --count 499 --msg 10`

```text
누락 검증 expected=4990 received=4990
누락 없음 확인

=== Scenario Report ===
mode: normal
total_bots: 499
success: 499
failure: 0
elapsed: 10.57s
avg_rtt: 6865ms
vote_integrity: N/A
```

499봇 × 10메시지 = 4,990건이 전부 echo로 돌아와 누락률 0%. 봇 시나리오는 모든 봇이 동시에 송신을 끝낸 후 수신 카운트를 기다리므로 통합 테스트의 batch 송신 대비 큐가 더 깊게 쌓여 `avg_rtt`가 길게 측정됩니다(아래 Stress Report 6.4.4와 비교).

#### 7.4.2. `bot-test --mode fickle --count 499 --msg 3`

```text
=== Scenario Report ===
mode: fickle
total_bots: 499
success: 499
failure: 0
elapsed: 3.83s
avg_rtt: N/A
vote_integrity: PASS (expected=[131, 131, 118, 119], actual=[131, 131, 118, 119], fickle_bots=499)
```

499봇이 동시에 0/1/2/3번 옵션을 변덕스럽게 갈아타며 투표한 후, 봇이 본 마지막 투표 분포와 서버가 broadcast한 마지막 `VoteSnapshot`이 **옵션별로 정확히 일치**(`element-wise`). §4.1 `vote()` `fetch_add` 순서 race, §5.2 broadcast throttle 누락 race가 모두 차단되었음을 보여줍니다. fickle 봇은 채팅 메시지를 보내지 않으므로 `avg_rtt`는 N/A.

#### 7.4.3. `bot-test --mode mixed --count 499 --msg 5 --ratio "normal:40,spammer:20,fickle:20,ghost:10,quitter:10"` (default ratio)

```text
mixed 모드 봇 배분 bot_type="normal" count=200
mixed 모드 봇 배분 bot_type="spammer" count=100
mixed 모드 봇 배분 bot_type="fickle" count=100
mixed 모드 봇 배분 bot_type="ghost" count=50
mixed 모드 봇 배분 bot_type="quitter" count=49

=== Scenario Report ===
mode: mixed
total_bots: 499
success: 499
failure: 0
elapsed: 5.10s
avg_rtt: 1871ms
vote_integrity: PASS (expected=[19, 29, 26, 26], actual=[19, 29, 26, 26], fickle_bots=100)
```

normal/spammer/fickle/ghost/quitter 5종이 동시에 섞여 있는 적대 시나리오에서도 서버 패닉/메모리 누수 없이 모든 봇이 success, fickle 100봇 기준 vote integrity PASS.

#### 7.4.4. `cargo test --release --test stress_test -- --nocapture`

100/300/500봇 × {10, 50, 100, 200} 메시지 정규 시나리오. 모두 zero-drop.

```text
=== Stress Report ===                === Stress Report ===
bots: 100                             bots: 300
msg_per_bot: 10                       msg_per_bot: 10
expected: 1000                        expected: 3000
received: 1000                        received: 3000
dropped: 0                            dropped: 0
drop_rate: 0.00%                      drop_rate: 0.00%
elapsed: 0.59s                        elapsed: 1.49s
throughput: 1684 msg/s                throughput: 2014 msg/s
avg_rtt: 177 ms                       avg_rtt: 151 ms
p95_rtt: 548 ms                       p95_rtt: 308 ms
p99_rtt: 570 ms                       p99_rtt: 616 ms

=== Stress Report ===                === Stress Report ===
bots: 500                             bots: 500
msg_per_bot: 10                       msg_per_bot: 50
expected: 5000                        expected: 25000
received: 5000                        received: 25000
dropped: 0                            dropped: 0
drop_rate: 0.00%                      drop_rate: 0.00%
elapsed: 2.29s                        elapsed: 2.88s
throughput: 2179 msg/s                throughput: 8670 msg/s
avg_rtt: 135 ms                       avg_rtt: 378 ms
p95_rtt: 224 ms                       p95_rtt: 480 ms
p99_rtt: 621 ms                       p99_rtt: 2810 ms

=== Stress Report ===                === Stress Report ===
bots: 500                             bots: 500
msg_per_bot: 100                      msg_per_bot: 200
expected: 50000                       expected: 100000
received: 50000                       received: 100000
dropped: 0                            dropped: 0
drop_rate: 0.00%                      drop_rate: 0.00%
elapsed: 3.90s                        elapsed: 5.44s
throughput: 12812 msg/s               throughput: 18370 msg/s
avg_rtt: 699 ms                       avg_rtt: 1183 ms
p95_rtt: 1016 ms                      p95_rtt: 1842 ms
p99_rtt: 3858 ms                      p99_rtt: 5375 ms

test result: ok. 6 passed; 0 failed; 2 ignored
```

500봇 기준 **msg_per_bot이 늘수록 throughput이 2.2k → 18.4k msg/s까지 선형에 가깝게 증가**합니다. 메시지가 짧을수록 connection setup / barrier 동기화 overhead가 throughput에 그대로 노출되고, 메시지가 길수록 broadcast 채널의 fan-out 효율이 dominate하기 때문입니다. RTT는 평균 대비 p99가 4–5배 길어지는 long-tail 특성을 보여 — 평균 RTT만으로는 사용자 체감 지연을 가릴 수 없음을 시각화합니다.

#### 7.4.5. `cargo test --release --test stress_test -- --ignored --nocapture` (한계 탐색)

```text
=== Stress Report ===                === Stress Report ===
bots: 500                             bots: 500
msg_per_bot: 300                      msg_per_bot: 400
expected: 150000                      expected: 200000
received: 150000                      received: 200000
dropped: 0                            dropped: 0
drop_rate: 0.00%                      drop_rate: 0.00%
elapsed: 7.22s                        elapsed: 12.46s
throughput: 20782 msg/s               throughput: 16051 msg/s
avg_rtt: 1026 ms                      avg_rtt: 2023 ms
p95_rtt: 3800 ms                      p95_rtt: 9947 ms
p99_rtt: 7107 ms                      p99_rtt: 11676 ms

test result: ok. 2 passed; 0 failed
```

`#[ignore]`로 분리된 capacity exploration 시나리오 역시 zero-drop을 유지했습니다. **300 메시지 시점에서 throughput이 ~20.8k msg/s 로 정점**을 찍고, 400 메시지에서는 큐 적체로 throughput이 16.1k msg/s로 떨어지며 p95/p99 RTT가 10초 근처까지 늘어납니다. 이는 현재 `BROADCAST_CAP` + 단일 broadcast 채널 fan-out이 500봇 동시 송신 기준 약 20k msg/s 부근에서 saturate함을 의미하며, 이 수치를 zero-drop을 보장하는 최대 처리량으로 해석할 수 있습니다.

## 8. GitHub 협업 흔적

- 총 52 커밋
- 브랜치 전략: feature 브랜치 → main 머지
- 커밋 컨벤션: `fix(scope): 한글 설명 — 보충`
- **이슈 → PR → close 사이클 운용**: 버그/개선 항목을 먼저 이슈로 등록해 재현 조건과 수정 방향을 합의한 뒤,
  PR 본문에 `Fixes #N`을 명시해 머지와 동시에 자동 close. PR #8(protocol-room)과 PR #1(server core)이 대표 예.

대표 사이클 — **PR #1 (feat: 서버 안정성 및 동시성 제어 개선)** 가 이슈 3건을 한 번에 close:

| 이슈 | 제목 | PR #1에서의 해결 |
| --- | --- | --- |
| #2 | 최대 연결 수 제한 없음 — 500인 초과 접속 허용됨 | `AtomicUsize CONN_COUNT` 도입 → accept 단계에서 `MAX_CONNECTIONS` 검사, 초과 시 `ServerMsg::Error` 후 소켓 close |
| #3 | Graceful Shutdown 누락 | `tokio::signal::ctrl_c()` 감지 → `BroadcastEvent::Shutdown` 전파로 write_task 클린 종료 |
| #4 | Rate Limiter 윈도우 경계 burst | Fixed Window → Token Bucket(10회/초)로 교체, 초과 메시지 즉시 drop |

실 컨벤션 적용 예시

```text
e33cc16 fix: VoteSnapshot broadcast throttle로 500봇 정합성 복구
6703d8e fix(vote): vote() 두 fetch_add 순서 변경 — 음수 가시 race 차단
faabcb1 fix(client): 라인 길이 제한을 read 단계에서 강제 (DoS 방지)
4fb4c54 refactor(metrics): wall-clock latency 측정 제거 + ServerMsg::Chat에 client_ts echo
```

## 9. 결론

Rust의 `tokio` 비동기 런타임을 기반으로 **500인 동시접속 멀티채팅 서버**를 성공적으로 구현했습니다.

핵심 목표였던 **동시성 정합성**은 read/write task 분리, Lock-Free 상태 관리, atomic 기반 투표 집계를 조합해 달성했습니다. 메시지 전달은 `broadcast::Sender`로 Lock 없이 처리하고, 연결 상한은 `Arc<Semaphore>`로 원자적으로 강제하며, 투표 집계는 `[AtomicI64; N_OPTIONS]`와 fetch_add 순서 보장으로 음수 노출 race를 원천 차단했습니다. 또한 CAS 기반 `VoteSnapshot` broadcast throttle로 고빈도 투표 상황에서도 갱신 폭주를 완화했습니다.

5종 봇(normal / fickle / spammer / ghost / quitter)으로 구성한 통합 테스트에서 **element-wise 투표 정합성 PASS**와 **채팅 누락률 0%** 를 확인했습니다. 정량적으로는 **500봇 × 200메시지(10만 건) 기준 zero-drop**을 유지하며 **최대 약 20k msg/s**의 처리량까지 saturate함을 측정했습니다(§7.4). Rust의 타입 시스템과 소유권 모델이 설계 단계에서 다수의 race condition 가능성을 컴파일 타임에 제거해 주었고, 이는 고부하 환경에서 서버 안정성으로 직결되었습니다.

## 10. 회고

[RETROSPECTIVE.md](./RETROSPECTIVE.md) 참조
