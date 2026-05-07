# RustChat 프로젝트 회고

> 시스템최신기술 26-1 Part 2 / C조

## 1. 잘한 점

### 1.1. 처음부터 "Lock을 줄이는" 설계로 시작했다
프로젝트 초반에 가장 잘 결정한 것은 **단일 broadcast 채널 + 별도 RwLock**이라는 구조를 일찍 확정한 것이다. 만약 `Vec<TcpStream>`을 Mutex로 감싸 순회 송신하는 흔한 구조로 시작했다면, 500명 환경에서 발생하는 병목을 후반에 다시 갈아엎어야 했을 것이다. tokio의 broadcast 채널이 정확히 이 use-case를 위해 설계되어 있다는 점을 일찍 알아본 것이 결정적이었다.

### 1.2. 투표 집계를 Atomic으로 가져간 판단
"투표 = Mutex로 감싼 카운터"는 너무 자연스러운 첫 번째 발상이지만, 500명이 0.01초 단위로 투표를 바꾸는 fickle 봇 시나리오에서는 Mutex 경합이 그대로 처리량 한계가 된다. `[AtomicI64; N]`로 가져간 덕분에 봇 테스트를 통과할 수 있었다.

### 1.3. 봇을 일찍, 다양하게 만들었다
adversarial 봇 5종(normal/fickle/spammer/ghost/quitter)을 일찍 갖춘 덕분에, 구현 도중 발견한 버그가 대부분 **서버를 고치면 봇 테스트로 즉시 검증**되는 사이클이 만들어졌다. 회귀 테스트가 자연스럽게 따라온 것이 컸다.

## 2. 어려웠던 점과 해결 방법

### 2.1. 두 fetch_add 사이의 가시 race
가장 오래 붙잡고 있었던 문제다. `vote(prev, next)`에서 prev를 먼저 빼면 다른 스레드의 snapshot이 음수를 보게 된다. CAS 루프나 Mutex로 가는 길도 있었지만, **순서를 (+1 먼저, -1 나중)으로 뒤집고 `.max(0)` 클램프를 끼우는** 단순한 해법이 가장 깔끔했다. 일시적으로 +1 over-count가 보일 수 있지만, 음수가 노출되지 않는 invariant가 훨씬 가치 있었다.

### 2.2. fickle 봇의 정합성 측정
봇이 0.01초 단위로 투표를 바꾸는데, 측정 시점에 누가 어떤 옵션에 있는지 어떻게 확정할 것인가? 처음엔 "총합만 맞으면 OK"로 시작했다가, "총합은 맞는데 분포가 drift하는" 케이스를 발견하고 element-wise 비교로 강화했다. 그러자 이번엔 "투표 간격이 서버 rate-limit보다 빨라서 일부가 drop되는" 문제가 드러났고, 봇 간격 조정 → Barrier 동기화 → 리더의 N회 재투표까지 단계적으로 정합성을 확보했다. 마지막에는 클라이언트별 rate-limit 자체를 제거하는 것이 근본 해결이었다 — 간격을 아무리 조정해도 burst가 허용되지 않으면 zero-drop 검증이 불가능했다.

### 2.3. VoteSnapshot broadcast 폭주
throttle을 넣기 전에는 500봇이 매 vote마다 broadcast를 트리거하니 VoteSnapshot이 약 10,000회 쏟아졌다. write_task backlog가 3초짜리 settle 윈도우 안에 소진되지 않아, fickle 봇들이 stale snapshot을 마지막 상태로 캡처하는 버그가 생겼다 — 정합성 테스트가 이유를 알 수 없이 FAIL하는 상황이었다.

CAS 기반 50ms throttle로 10,000회를 ~68회로 줄이니 바로 해결됐다. 그런데 throttle만 넣으면 "마지막 vote가 영영 누락"되는 새 문제가 생겼고, pending flush 경로를 별도로 추가해야 했다. 하나를 고치면 다음 문제가 드러나는 레이어드 버그였다.

### 2.4. 종료 처리(graceful shutdown)

SIGINT에서 단순히 `process::exit()`을 부르면 metrics 리포터가 마지막 누락률을 출력하지 못하고, 클라이언트 task가 중간에 끊겨 봇 검증 결과가 부정확해진다. 결국 다음 순서로 정리됨:
1. broadcast로 `Shutdown` 이벤트 전파
2. 클라이언트 task `JoinHandle`을 모두 await
3. 메트릭 리포터에 별도 shutdown 신호 송신

## 3. 뭘 배웠는가

- **tokio broadcast 채널의 성격**: backpressure(lagged) 시 receiver가 메시지를 잃을 수 있다. "크게 잡으면 되겠지"로 32k에서 시작했지만 부하가 커질수록 부족해져 결국 524k까지 세 번을 올렸다. capacity 조정만으로는 한계가 있고, 근본적으로 producer 빈도를 throttle해야 한다는 걸 직접 겪었다.
- **Lock-Free의 진짜 비용**: Mutex보다 빠른 게 아니라, "Mutex의 경합 비용이 뼈아플 때만" 빠르다. 이번 프로젝트에서는 투표 카운터 4개에 500명이 동시 접근하는 좁은 핫스팟이 정확히 그 케이스였다.
- **Property-based testing의 가치**: 손으로 짠 케이스 10개보다 proptest의 임의 입력 1000개가 race를 잘 잡는다.
- **측정 경로도 설계해야 한다**: 처음에 서버 수신 시점 기준으로 latency를 쟀더니 항상 ~0ms가 나왔다. 클라이언트 송신 시각(`client_ts`)을 broadcast에 echo해서 발신자가 자기 monotonic 시계로 RTT를 계산하도록 바꾸고 나서야 의미 있는 수치가 나왔다. 구현이 맞아도 측정이 틀리면 아무것도 모르는 것과 같다.
- **Rust의 ownership이 동시성에서 진짜 강력하다**: `Send + Sync` 요구 덕분에, "이 데이터를 task 간에 어떻게 공유하지?"라는 질문이 컴파일 타임에 강제로 답해진다. C++/Go였으면 런타임에서 한참 헤맸을 race가 컴파일 시점에 막혔다.

## 4. 아쉬운 점

1. **메시지 영속화를 처음부터 고려한 인터페이스로 만든다**. 지금은 broadcast 채널에 직접 묶여 있어서, 디스크 쓰기를 추가하려면 구조 변경이 필요하다. trait 기반 sink 추상화를 넣었어야.
2. **`tracing` 스팬을 더 적극 활용한다**. 지금은 info/warn 단발 로그만 찍는데, span으로 client lifecycle을 묶으면 디버깅이 훨씬 쉬워졌을 것.