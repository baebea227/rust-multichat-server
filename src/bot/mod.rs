pub mod fickle;
pub mod ghost;
pub mod normal;
pub mod quitter;
pub mod spammer;

use anyhow::Result;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::{Barrier, Mutex};
use tracing::info;

use crate::protocol::{ClientMsg, N_OPTIONS};

pub const SERVER_ADDR: &str = "127.0.0.1:8080";
pub const BOT_RECV_TIMEOUT_SECS: u64 = 30;
/// ghost 봇이 lagged-receiver 정리 검증을 위해 연결을 유지하는 시간(초)
/// — msg_per_bot에 영향받지 않도록 고정 상수로 분리
pub const GHOST_HOLD_SECS: u64 = 5;

// ── Scenario Report: RttCounter ─────────────────────────────────────

/// Lock-free RTT 누적 카운터 (AtomicU64 기반)
pub struct RttCounter {
    pub sum_ms: AtomicU64,
    pub count: AtomicU64,
}

impl RttCounter {
    /// 새 RttCounter를 Arc로 감싸서 생성
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sum_ms: AtomicU64::new(0),
            count: AtomicU64::new(0),
        })
    }

    /// RTT 값(밀리초)을 누적 기록
    pub fn record(&self, rtt_ms: u64) {
        self.sum_ms.fetch_add(rtt_ms, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// 평균 RTT 계산. 기록이 없으면 None 반환
    pub fn average(&self) -> Option<u64> {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 {
            None
        } else {
            Some(self.sum_ms.load(Ordering::Relaxed) / c)
        }
    }
}

// ── Vote Integrity: FickleResult 구조체 ─────────────────────────────

/// fickle 봇의 실행 결과
#[derive(Debug, Clone)]
pub struct FickleResult {
    /// 마지막으로 투표한 옵션 (0..N_OPTIONS)
    pub last_vote: Option<usize>,
    /// 마지막으로 수신한 VoteSnapshot의 counts
    pub last_snapshot: Option<[u64; N_OPTIONS]>,
}

// ── Vote Integrity: VoteIntegrityResult 구조체 ──────────────────────

/// 투표 정합성 검증 결과
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteIntegrityResult {
    /// 정합성 통과 여부 — 옵션별 분포까지 정확히 일치할 때만 true
    pub passed: bool,
    /// 봇 측 기대 투표 배열
    pub expected: [u64; N_OPTIONS],
    /// 서버 측 VoteSnapshot counts 배열
    pub actual: [u64; N_OPTIONS],
    /// 검증에 참여한 fickle 봇 수
    pub fickle_count: usize,
}

// ── Vote Integrity: 집계 순수 함수 ──────────────────────────────────

/// 마지막 투표 옵션 목록을 옵션별 카운트 배열로 집계
pub fn tally_votes(last_votes: &[Option<usize>]) -> [u64; N_OPTIONS] {
    let mut counts = [0u64; N_OPTIONS];
    for vote in last_votes {
        if let Some(opt) = vote {
            if *opt < N_OPTIONS {
                counts[*opt] += 1;
            }
        }
    }
    counts
}

/// 봇 측 기대 배열과 서버 측 실제 배열을 비교하여 정합성 결과 생성
pub fn check_vote_integrity(
    expected: [u64; N_OPTIONS],
    actual: [u64; N_OPTIONS],
    fickle_count: usize,
) -> VoteIntegrityResult {
    VoteIntegrityResult {
        passed: expected == actual,
        expected,
        actual,
        fickle_count,
    }
}

// ── Scenario Report: ScenarioReport 구조체 ──────────────────────────

/// 시나리오 실행 결과 요약 리포트
#[derive(Debug)]
pub struct ScenarioReport {
    pub mode: String,
    pub total_bots: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub elapsed_secs: f64,
    pub avg_rtt_ms: Option<u64>,
    /// 투표 정합성 검증 결과 (fickle 봇이 있는 경우에만 Some)
    pub vote_integrity: Option<VoteIntegrityResult>,
}

impl fmt::Display for ScenarioReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rtt_str = match self.avg_rtt_ms {
            Some(ms) => format!("{ms}ms"),
            None => "N/A".to_string(),
        };
        let vote_integrity_str = match &self.vote_integrity {
            Some(vi) => {
                let status = if vi.passed { "PASS" } else { "FAIL" };
                format!(
                    "{} (expected={:?}, actual={:?}, fickle_bots={})",
                    status, vi.expected, vi.actual, vi.fickle_count
                )
            }
            None => "N/A".to_string(),
        };
        write!(
            f,
            "=== Scenario Report ===\n\
             mode: {}\n\
             total_bots: {}\n\
             success: {}\n\
             failure: {}\n\
             elapsed: {:.2}s\n\
             avg_rtt: {}\n\
             vote_integrity: {}",
            self.mode, self.total_bots, self.success_count,
            self.failure_count, self.elapsed_secs, rtt_str, vote_integrity_str
        )
    }
}

// ── Task 1: BotType 열거형 ──────────────────────────────────────────

/// 봇 동작 유형을 나타내는 열거형
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BotType {
    Normal,
    Fickle,
    Spammer,
    Ghost,
    Quitter,
}

impl BotType {
    /// 문자열에서 BotType으로 변환
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "normal" => Ok(BotType::Normal),
            "fickle" => Ok(BotType::Fickle),
            "spammer" => Ok(BotType::Spammer),
            "ghost" => Ok(BotType::Ghost),
            "quitter" => Ok(BotType::Quitter),
            other => Err(format!("유효하지 않은 봇 타입: '{other}'")),
        }
    }

    /// BotType을 문자열로 변환
    pub fn as_str(&self) -> &'static str {
        match self {
            BotType::Normal => "normal",
            BotType::Fickle => "fickle",
            BotType::Spammer => "spammer",
            BotType::Ghost => "ghost",
            BotType::Quitter => "quitter",
        }
    }
}

// ── Task 1: RatioSpec 구조체 ────────────────────────────────────────

/// 봇 타입별 비율을 저장하는 구조체
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RatioSpec {
    /// 순서 보존을 위해 Vec 사용
    pub entries: Vec<(BotType, u32)>,
}

impl RatioSpec {
    /// 기본 비율 문자열
    pub const DEFAULT: &str = "normal:40,spammer:20,fickle:20,ghost:10,quitter:10";

    /// 비율 문자열을 파싱하여 RatioSpec을 생성
    ///
    /// 형식: `타입:숫자,타입:숫자,...`
    /// 예: `"normal:40,spammer:20,fickle:20,ghost:10,quitter:10"`
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("비율 문자열이 비어 있습니다".to_string());
        }

        let mut entries = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            let (type_str, ratio_str) = part
                .split_once(':')
                .ok_or_else(|| format!("잘못된 형식: '{part}' ('타입:숫자' 형식이어야 합니다)"))?;

            let type_str = type_str.trim();
            let ratio_str = ratio_str.trim();

            let bot_type = BotType::from_str(type_str)?;

            let ratio: u32 = ratio_str
                .parse()
                .map_err(|_| format!("비율 값이 유효한 숫자가 아닙니다: '{ratio_str}'"))?;

            if ratio == 0 {
                return Err(format!("비율 값은 1 이상이어야 합니다: '{type_str}:{ratio}'"));
            }

            entries.push((bot_type, ratio));
        }

        Ok(RatioSpec { entries })
    }
}

impl fmt::Display for RatioSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .entries
            .iter()
            .map(|(bt, r)| format!("{}:{}", bt.as_str(), r))
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

// ── Task 2: BotAllocator 배분 로직 ──────────────────────────────────

/// 총 봇 수를 비율에 따라 각 BotType별로 배분
///
/// 알고리즘:
/// 1. 비율 합계 계산
/// 2. 각 타입별 `floor(total * ratio / sum)` 계산
/// 3. 나머지 = `total - 배분 합계`
/// 4. 나머지를 비율이 높은 순서대로 1개씩 배분
pub fn allocate(total: usize, spec: &RatioSpec) -> Vec<(BotType, usize)> {
    if spec.entries.is_empty() {
        return Vec::new();
    }

    let ratio_sum: u32 = spec.entries.iter().map(|(_, r)| *r).sum();

    // 기본 floor 배분
    let mut result: Vec<(BotType, usize, u32)> = spec
        .entries
        .iter()
        .map(|(bt, r)| {
            let allocated = (total as u64 * *r as u64 / ratio_sum as u64) as usize;
            (*bt, allocated, *r)
        })
        .collect();

    let allocated_sum: usize = result.iter().map(|(_, a, _)| *a).sum();
    let mut remainder = total.saturating_sub(allocated_sum);

    // 나머지를 비율이 높은 순서대로 1개씩 배분
    // 인덱스를 비율 내림차순으로 정렬 (비율이 같으면 원래 순서 유지)
    let mut indices: Vec<usize> = (0..result.len()).collect();
    indices.sort_by(|&a, &b| result[b].2.cmp(&result[a].2));

    let mut idx = 0;
    while remainder > 0 {
        result[indices[idx]].1 += 1;
        remainder -= 1;
        idx = (idx + 1) % indices.len();
    }

    result.into_iter().map(|(bt, a, _)| (bt, a)).collect()
}

// ── 기존 헬퍼 함수 ─────────────────────────────────────────────────

/// 자기 자신의 메시지를 `msg_count`개 수신할 때까지 카운트하는 공용 recv 루프.
///
/// - `line.contains(target)`을 만족하는 라인이 들어올 때마다 `on_match` 콜백을 호출하고 카운트 증가
/// - `msg_count` 도달 시 즉시 종료
/// - 입력이 끊기거나(EOF) `timeout_dur` 경과 시에도 종료, 현재까지 수신한 카운트를 반환
///
/// normal/spammer 봇과 통합 테스트가 동일한 구현을 공유하기 위해 lib에 노출.
pub async fn recv_until_count_with_timeout<R, F>(
    lines: &mut tokio::io::Lines<tokio::io::BufReader<R>>,
    target: &str,
    msg_count: u64,
    timeout_dur: std::time::Duration,
    mut on_match: F,
) -> u64
where
    R: tokio::io::AsyncRead + Unpin,
    F: FnMut(&str),
{
    let mut count: u64 = 0;
    let _ = tokio::time::timeout(timeout_dur, async {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains(target) {
                on_match(&line);
                count += 1;
                if count >= msg_count {
                    break;
                }
            }
        }
    })
    .await;
    count
}

/// 봇이 소켓으로 JSON 메시지 한 줄을 전송하는 헬퍼
pub async fn send_msg(
    stream: &mut tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    msg: &ClientMsg,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn connect() -> Result<TcpStream> {
    Ok(TcpStream::connect(SERVER_ADDR).await?)
}

// ── Task 3: run_scenario (mixed 분기 추가) ──────────────────────────

/// 전체 봇 시나리오 실행기
///
/// `ratio`는 mixed 모드에서만 사용된다. 단일 모드에서는 무시된다.
pub async fn run_scenario(
    mode: &str,
    count: usize,
    msg_per_bot: usize,
    ratio: Option<&str>,
) -> anyhow::Result<()> {
    if mode == "mixed" {
        run_mixed_scenario(count, msg_per_bot, ratio).await?;
    } else {
        run_single_scenario(mode, count, msg_per_bot).await;
    }
    Ok(())
}

/// mixed 모드: 비율에 따라 여러 봇 타입을 혼합 실행
async fn run_mixed_scenario(
    count: usize,
    msg_per_bot: usize,
    ratio: Option<&str>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let spec = match ratio {
        Some(r) => RatioSpec::parse(r)
            .map_err(|e| anyhow::anyhow!("유효하지 않은 ratio: {e}"))?,
        None => RatioSpec::parse(RatioSpec::DEFAULT).unwrap(),
    };
    let allocation = allocate(count, &spec);

    // 각 타입별 배분 수 로그 출력
    for (bt, n) in &allocation {
        info!(bot_type = bt.as_str(), count = n, "mixed 모드 봇 배분");
    }

    let recv_counter = Arc::new(AtomicU64::new(0));
    let rtt_counter = RttCounter::new();
    let fickle_results: Arc<Mutex<Vec<FickleResult>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    let mut bot_id: u64 = 0;

    // fickle 봇 정합성 측정용 동기화 barrier
    let fickle_count: usize = allocation
        .iter()
        .filter(|(bt, _)| *bt == BotType::Fickle)
        .map(|(_, n)| *n)
        .sum();
    let fickle_barrier = Arc::new(Barrier::new(fickle_count.max(1)));

    for (bt, n) in &allocation {
        for _ in 0..*n {
            let recv_counter = recv_counter.clone();
            let rtt_counter = rtt_counter.clone();
            let fickle_results = fickle_results.clone();
            let fickle_barrier = fickle_barrier.clone();
            let bt = *bt;
            let id = bot_id;
            bot_id += 1;

            let handle = tokio::spawn(async move {
                let result = match bt {
                    BotType::Normal => {
                        normal::run(id, msg_per_bot, recv_counter, rtt_counter).await
                    }
                    BotType::Fickle => {
                        match fickle::run(id, msg_per_bot, fickle_barrier).await {
                            Ok(fickle_result) => {
                                fickle_results.lock().await.push(fickle_result);
                                Ok(())
                            }
                            Err(e) => Err(e),
                        }
                    }
                    BotType::Spammer => {
                        spammer::run(id, msg_per_bot, recv_counter, rtt_counter).await
                    }
                    BotType::Ghost => ghost::run(id).await,
                    BotType::Quitter => quitter::run(id).await,
                };
                if let Err(ref e) = result {
                    tracing::warn!("봇 {id} ({}) 오류: {e}", bt.as_str());
                }
                result
            });
            handles.push(handle);
        }
    }

    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(())) => success_count += 1,
            Ok(Err(_)) => failure_count += 1,
            Err(_join_err) => failure_count += 1,
        }
    }

    // 투표 정합성 검증
    let fickle_results = fickle_results.lock().await;
    let vote_integrity = if fickle_results.is_empty() {
        None
    } else {
        let last_votes: Vec<Option<usize>> = fickle_results.iter().map(|r| r.last_vote).collect();
        let expected = tally_votes(&last_votes);
        let actual = fickle_results
            .iter()
            .rev()
            .find_map(|r| r.last_snapshot)
            .unwrap_or([0; N_OPTIONS]);
        Some(check_vote_integrity(expected, actual, fickle_results.len()))
    };

    let elapsed = start.elapsed().as_secs_f64();
    let report = ScenarioReport {
        mode: "mixed".to_string(),
        total_bots: count,
        success_count,
        failure_count,
        elapsed_secs: elapsed,
        avg_rtt_ms: rtt_counter.average(),
        vote_integrity,
    };
    info!("\n{report}");
    Ok(())
}

/// 기존 단일 모드 시나리오
async fn run_single_scenario(mode: &str, count: usize, msg_per_bot: usize) {
    let start = Instant::now();

    let recv_counter = Arc::new(AtomicU64::new(0));
    let rtt_counter = RttCounter::new();
    let fickle_results: Arc<Mutex<Vec<FickleResult>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::with_capacity(count);

    // fickle 모드일 때만 barrier 크기를 봇 수로, 그 외에는 1로 설정
    let barrier_size = if mode == "fickle" { count.max(1) } else { 1 };
    let fickle_barrier = Arc::new(Barrier::new(barrier_size));

    for i in 0..count {
        let recv_counter = recv_counter.clone();
        let rtt_counter = rtt_counter.clone();
        let fickle_results = fickle_results.clone();
        let fickle_barrier = fickle_barrier.clone();
        let mode = mode.to_string();

        let handle = tokio::spawn(async move {
            let result = match mode.as_str() {
                "normal" => {
                    normal::run(i as u64, msg_per_bot, recv_counter, rtt_counter).await
                }
                "fickle" => {
                    match fickle::run(i as u64, msg_per_bot, fickle_barrier).await {
                        Ok(fickle_result) => {
                            fickle_results.lock().await.push(fickle_result);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                "spammer" => {
                    spammer::run(i as u64, msg_per_bot, recv_counter, rtt_counter).await
                }
                "ghost" => ghost::run(i as u64).await,
                "quitter" => quitter::run(i as u64).await,
                other => {
                    tracing::warn!("알 수 없는 봇 모드: {other}");
                    Ok(())
                }
            };
            if let Err(ref e) = result {
                tracing::warn!("봇 {i} 오류: {e}");
            }
            result
        });
        handles.push(handle);
    }

    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(())) => success_count += 1,
            Ok(Err(_)) => failure_count += 1,
            Err(_join_err) => failure_count += 1,
        }
    }

    // 기존 normal 모드 누락 검증 유지
    if mode == "normal" {
        let expected = (count * msg_per_bot) as u64;
        let received = recv_counter.load(Ordering::SeqCst);
        info!(expected, received, "누락 검증");
        assert_eq!(
            expected, received,
            "메시지 누락 발생! expected={expected} received={received}"
        );
        info!("누락 없음 확인");
    }

    // 투표 정합성 검증 (fickle 모드에서만)
    let fickle_results = fickle_results.lock().await;
    let vote_integrity = if mode == "fickle" && !fickle_results.is_empty() {
        let last_votes: Vec<Option<usize>> = fickle_results.iter().map(|r| r.last_vote).collect();
        let expected = tally_votes(&last_votes);
        let actual = fickle_results
            .iter()
            .rev()
            .find_map(|r| r.last_snapshot)
            .unwrap_or([0; N_OPTIONS]);
        Some(check_vote_integrity(expected, actual, fickle_results.len()))
    } else {
        None
    };

    let elapsed = start.elapsed().as_secs_f64();
    let report = ScenarioReport {
        mode: mode.to_string(),
        total_bots: count,
        success_count,
        failure_count,
        elapsed_secs: elapsed,
        avg_rtt_ms: rtt_counter.average(),
        vote_integrity,
    };
    info!("\n{report}");
}


// ── Task 5 & 6: 단위 테스트 + 속성 기반 테스트 ─────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── 5.1 RatioSpec::parse() 정상 케이스 ──────────────────────────

    #[test]
    fn parse_default_ratio() {
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        assert_eq!(spec.entries.len(), 5);
        assert_eq!(spec.entries[0], (BotType::Normal, 40));
        assert_eq!(spec.entries[1], (BotType::Spammer, 20));
        assert_eq!(spec.entries[2], (BotType::Fickle, 20));
        assert_eq!(spec.entries[3], (BotType::Ghost, 10));
        assert_eq!(spec.entries[4], (BotType::Quitter, 10));
    }

    #[test]
    fn parse_single_entry() {
        let spec = RatioSpec::parse("normal:100").unwrap();
        assert_eq!(spec.entries, vec![(BotType::Normal, 100)]);
    }

    #[test]
    fn parse_with_whitespace() {
        let spec = RatioSpec::parse("  normal:40 , spammer:20  ").unwrap();
        assert_eq!(spec.entries.len(), 2);
        assert_eq!(spec.entries[0], (BotType::Normal, 40));
        assert_eq!(spec.entries[1], (BotType::Spammer, 20));
    }

    // ── 5.2 RatioSpec::parse() 오류 케이스 ──────────────────────────

    #[test]
    fn parse_empty_string() {
        assert!(RatioSpec::parse("").is_err());
    }

    #[test]
    fn parse_whitespace_only() {
        assert!(RatioSpec::parse("   ").is_err());
    }

    #[test]
    fn parse_bad_format_no_colon() {
        assert!(RatioSpec::parse("normal40").is_err());
    }

    #[test]
    fn parse_zero_ratio() {
        assert!(RatioSpec::parse("normal:0").is_err());
    }

    #[test]
    fn parse_invalid_bot_type() {
        assert!(RatioSpec::parse("unknown:10").is_err());
    }

    #[test]
    fn parse_non_numeric_ratio() {
        assert!(RatioSpec::parse("normal:abc").is_err());
    }

    // ── 5.3 allocate() 기본 비율 + 500봇 ───────────────────────────

    #[test]
    fn allocate_default_500() {
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        let result = allocate(500, &spec);

        // 합계 검증
        let total: usize = result.iter().map(|(_, n)| *n).sum();
        assert_eq!(total, 500);

        // 비율 40:20:20:10:10 → 200:100:100:50:50
        assert_eq!(result[0], (BotType::Normal, 200));
        assert_eq!(result[1], (BotType::Spammer, 100));
        assert_eq!(result[2], (BotType::Fickle, 100));
        assert_eq!(result[3], (BotType::Ghost, 50));
        assert_eq!(result[4], (BotType::Quitter, 50));
    }

    #[test]
    fn allocate_default_501() {
        // 501봇: 나머지 1개는 비율이 가장 높은 normal에 배분
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        let result = allocate(501, &spec);

        let total: usize = result.iter().map(|(_, n)| *n).sum();
        assert_eq!(total, 501);

        // normal이 나머지 1개를 받아야 함
        assert_eq!(result[0], (BotType::Normal, 201));
    }

    // ── 5.4 allocate() edge case: 봇 수 < 타입 수 ──────────────────

    #[test]
    fn allocate_fewer_bots_than_types() {
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        let result = allocate(3, &spec);

        let total: usize = result.iter().map(|(_, n)| *n).sum();
        assert_eq!(total, 3);

        // 비율 높은 순서대로 배분: normal(40), spammer(20), fickle(20)
        // floor 배분은 모두 0이므로 나머지 3개를 비율 높은 순서대로 배분
        let normal_count = result.iter().find(|(bt, _)| *bt == BotType::Normal).unwrap().1;
        assert!(normal_count >= 1, "비율이 가장 높은 normal은 최소 1개 배분");
    }

    #[test]
    fn allocate_zero_bots() {
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        let result = allocate(0, &spec);

        let total: usize = result.iter().map(|(_, n)| *n).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn allocate_one_bot() {
        let spec = RatioSpec::parse(RatioSpec::DEFAULT).unwrap();
        let result = allocate(1, &spec);

        let total: usize = result.iter().map(|(_, n)| *n).sum();
        assert_eq!(total, 1);
    }

    // ── BotType 변환 테스트 ─────────────────────────────────────────

    #[test]
    fn bot_type_roundtrip() {
        let types = [BotType::Normal, BotType::Fickle, BotType::Spammer, BotType::Ghost, BotType::Quitter];
        for bt in &types {
            assert_eq!(BotType::from_str(bt.as_str()).unwrap(), *bt);
        }
    }

    #[test]
    fn bot_type_invalid() {
        assert!(BotType::from_str("invalid").is_err());
        assert!(BotType::from_str("").is_err());
    }

    // ── Task 6: 속성 기반 테스트 (proptest) ─────────────────────────

    use proptest::prelude::*;

    /// 유효한 BotType을 생성하는 전략
    fn arb_bot_type() -> impl Strategy<Value = BotType> {
        prop_oneof![
            Just(BotType::Normal),
            Just(BotType::Fickle),
            Just(BotType::Spammer),
            Just(BotType::Ghost),
            Just(BotType::Quitter),
        ]
    }

    /// 유효한 RatioSpec entries를 생성하는 전략 (1~5개 항목, 비율 1~100)
    fn arb_ratio_entries() -> impl Strategy<Value = Vec<(BotType, u32)>> {
        // 중복 없는 BotType 선택을 위해 서브셋 방식 사용
        (1u32..=100, 1u32..=100, 1u32..=100, 1u32..=100, 1u32..=100, 1usize..=5)
            .prop_map(|(r1, r2, r3, r4, r5, count)| {
                let all = vec![
                    (BotType::Normal, r1),
                    (BotType::Spammer, r2),
                    (BotType::Fickle, r3),
                    (BotType::Ghost, r4),
                    (BotType::Quitter, r5),
                ];
                all.into_iter().take(count).collect::<Vec<_>>()
            })
    }

    /// 유효한 RatioSpec을 생성하는 전략
    fn arb_ratio_spec() -> impl Strategy<Value = RatioSpec> {
        arb_ratio_entries().prop_map(|entries| RatioSpec { entries })
    }

    // ── 6.2 Property 1: RatioSpec 라운드트립 ────────────────────────
    // **Validates: Requirements 3.4**
    proptest! {
        #[test]
        fn prop_ratio_spec_roundtrip(spec in arb_ratio_spec()) {
            // Property 1: parse(to_string(spec)) == spec
            let serialized = spec.to_string();
            let parsed = RatioSpec::parse(&serialized)
                .expect("라운드트립 파싱 실패");
            prop_assert_eq!(parsed.entries, spec.entries);
        }
    }

    // ── 6.3 Property 2: 유효하지 않은 입력 거부 ─────────────────────
    // **Validates: Requirements 2.4, 3.2, 3.3**
    proptest! {
        #[test]
        fn prop_invalid_bot_type_rejected(
            invalid_name in "[a-z]{1,10}"
                .prop_filter("유효한 봇 타입 제외",
                    |s| !["normal","fickle","spammer","ghost","quitter"].contains(&s.as_str())),
            ratio in 1u32..=100
        ) {
            // (a) 유효하지 않은 봇 타입명
            let input = format!("{}:{}", invalid_name, ratio);
            prop_assert!(RatioSpec::parse(&input).is_err(),
                "유효하지 않은 봇 타입 '{}'이 허용됨", invalid_name);
        }

        #[test]
        fn prop_bad_format_rejected(
            word in "[a-z]{1,10}"
        ) {
            // (b) 콜론 없는 형식
            prop_assert!(RatioSpec::parse(&word).is_err(),
                "콜론 없는 형식 '{}'이 허용됨", word);
        }

        #[test]
        fn prop_zero_ratio_rejected(
            bot_type in arb_bot_type()
        ) {
            // (c) 비율 값이 0
            let input = format!("{}:0", bot_type.as_str());
            prop_assert!(RatioSpec::parse(&input).is_err(),
                "비율 0이 허용됨: '{}'", input);
        }
    }

    // ── 6.4 Property 3: 배분 합계 불변량 ────────────────────────────
    // **Validates: Requirements 4.2**
    proptest! {
        #[test]
        fn prop_allocation_sum_equals_total(
            total in 0usize..=10000,
            spec in arb_ratio_spec()
        ) {
            let result = allocate(total, &spec);
            let sum: usize = result.iter().map(|(_, n)| *n).sum();
            prop_assert_eq!(sum, total,
                "배분 합계({})가 total({})과 불일치. spec={}, result={:?}",
                sum, total, spec, result);
        }
    }

    // ── 6.5 Property 4: 비율 비례 배분 ──────────────────────────────
    // **Validates: Requirements 2.1, 4.1**
    proptest! {
        #[test]
        fn prop_proportional_allocation(
            total in 0usize..=10000,
            spec in arb_ratio_spec()
        ) {
            let result = allocate(total, &spec);
            let ratio_sum: u64 = spec.entries.iter().map(|(_, r)| *r as u64).sum();

            for (i, (bt, count)) in result.iter().enumerate() {
                let ratio = spec.entries[i].1 as u64;
                let floor_val = (total as u64 * ratio / ratio_sum) as usize;
                prop_assert!(*count >= floor_val,
                    "{:?} 배분({})이 floor({})보다 작음. total={}, spec={}",
                    bt, count, floor_val, total, spec);
            }
        }
    }

    // ── 6.6 Property 5: 나머지 배분 순서 ────────────────────────────
    // **Validates: Requirements 4.3, 4.4**
    proptest! {
        #[test]
        fn prop_remainder_allocation_order(
            total in 0usize..=10000,
            spec in arb_ratio_spec()
        ) {
            let result = allocate(total, &spec);
            let ratio_sum: u64 = spec.entries.iter().map(|(_, r)| *r as u64).sum();

            // 각 타입의 나머지(실제 배분 - floor 배분) 계산
            let remainders: Vec<(BotType, usize, u32)> = result.iter().enumerate().map(|(i, (bt, count))| {
                let ratio = spec.entries[i].1;
                let floor_val = (total as u64 * ratio as u64 / ratio_sum) as usize;
                let extra = count - floor_val;
                (*bt, extra, ratio)
            }).collect();

            // 나머지를 받은 타입의 비율은 나머지를 받지 못한 타입의 비율 이상이어야 함
            for got in remainders.iter().filter(|(_, extra, _)| *extra > 0) {
                for not_got in remainders.iter().filter(|(_, extra, _)| *extra == 0) {
                    prop_assert!(got.2 >= not_got.2,
                        "나머지 배분 순서 위반: {:?}(비율={})이 나머지를 받았지만 {:?}(비율={})은 받지 못함",
                        got.0, got.2, not_got.0, not_got.2);
                }
            }
        }
    }

    // ── Task 4: ScenarioReport / RttCounter 단위 테스트 ─────────────

    // ── 4.1 ScenarioReport Display 출력 형식 테스트 (모든 필드 포함) ─
    #[test]
    fn scenario_report_display_all_fields() {
        let report = ScenarioReport {
            mode: "normal".to_string(),
            total_bots: 100,
            success_count: 95,
            failure_count: 5,
            elapsed_secs: 12.345,
            avg_rtt_ms: Some(42),
            vote_integrity: None,
        };
        let output = format!("{report}");

        assert!(output.contains("=== Scenario Report ==="));
        assert!(output.contains("mode: normal"));
        assert!(output.contains("total_bots: 100"));
        assert!(output.contains("success: 95"));
        assert!(output.contains("failure: 5"));
        assert!(output.contains("elapsed: 12.35s"));
        assert!(output.contains("avg_rtt: 42ms"));
    }

    // ── 4.2 ScenarioReport Display에서 avg_rtt_ms가 None일 때 "N/A" ─
    #[test]
    fn scenario_report_display_rtt_none_shows_na() {
        let report = ScenarioReport {
            mode: "ghost".to_string(),
            total_bots: 10,
            success_count: 10,
            failure_count: 0,
            elapsed_secs: 1.0,
            avg_rtt_ms: None,
            vote_integrity: None,
        };
        let output = format!("{report}");

        assert!(output.contains("avg_rtt: N/A"));
        // "ms"가 avg_rtt 라인에 나타나지 않아야 함
        for line in output.lines() {
            if line.starts_with("avg_rtt:") {
                assert!(!line.contains("ms"), "None일 때 'ms'가 포함되면 안 됨");
            }
        }
    }

    // ── 4.3 RttCounter record/average 기본 동작 테스트 ──────────────
    #[test]
    fn rtt_counter_record_and_average() {
        let counter = RttCounter::new();
        counter.record(10);
        counter.record(20);
        counter.record(30);

        // 평균: (10 + 20 + 30) / 3 = 20
        assert_eq!(counter.average(), Some(20));
    }

    // ── 4.4 RttCounter 빈 상태에서 average() → None ────────────────
    #[test]
    fn rtt_counter_empty_average_is_none() {
        let counter = RttCounter::new();
        assert_eq!(counter.average(), None);
    }

    // ── Task 5: ScenarioReport / RttCounter 속성 기반 테스트 ────────

    /// 유효한 ScenarioReport를 생성하는 전략
    fn arb_scenario_report() -> impl Strategy<Value = ScenarioReport> {
        (
            prop_oneof![
                Just("normal".to_string()),
                Just("fickle".to_string()),
                Just("spammer".to_string()),
                Just("ghost".to_string()),
                Just("quitter".to_string()),
                Just("mixed".to_string()),
            ],
            0usize..=10000,
            prop::option::of(0u64..=100_000),
        )
            .prop_flat_map(|(mode, total, avg_rtt_ms)| {
                // success_count는 0..=total 범위에서 생성
                (Just(mode), Just(total), 0..=total, Just(avg_rtt_ms))
            })
            .prop_flat_map(|(mode, total, success, avg_rtt_ms)| {
                let failure = total - success;
                // elapsed_secs: 0.0 ~ 3600.0
                (Just(mode), Just(total), Just(success), Just(failure), 0.0f64..3600.0, Just(avg_rtt_ms))
            })
            .prop_map(|(mode, total_bots, success_count, failure_count, elapsed_secs, avg_rtt_ms)| {
                ScenarioReport {
                    mode,
                    total_bots,
                    success_count,
                    failure_count,
                    elapsed_secs,
                    avg_rtt_ms,
                    vote_integrity: None,
                }
            })
    }

    /// Display 출력 문자열에서 ScenarioReport 필드를 파싱하는 헬퍼
    fn parse_report_display(s: &str) -> Option<(String, usize, usize, usize, String, String)> {
        let mut mode = None;
        let mut total_bots = None;
        let mut success = None;
        let mut failure = None;
        let mut elapsed = None;
        let mut avg_rtt = None;

        for line in s.lines() {
            if let Some(v) = line.strip_prefix("mode: ") {
                mode = Some(v.to_string());
            } else if let Some(v) = line.strip_prefix("total_bots: ") {
                total_bots = Some(v.parse::<usize>().ok()?);
            } else if let Some(v) = line.strip_prefix("success: ") {
                success = Some(v.parse::<usize>().ok()?);
            } else if let Some(v) = line.strip_prefix("failure: ") {
                failure = Some(v.parse::<usize>().ok()?);
            } else if let Some(v) = line.strip_prefix("elapsed: ") {
                elapsed = Some(v.to_string());
            } else if let Some(v) = line.strip_prefix("avg_rtt: ") {
                avg_rtt = Some(v.to_string());
            }
        }

        Some((mode?, total_bots?, success?, failure?, elapsed?, avg_rtt?))
    }

    // ── 5.1 Property 1: 성공/실패 합계 불변량 ──────────────────────
    // **Validates: Requirements 2.4**
    proptest! {
        #[test]
        fn prop_scenario_report_success_failure_sum(
            report in arb_scenario_report()
        ) {
            // Property 1: success_count + failure_count == total_bots
            prop_assert_eq!(
                report.success_count + report.failure_count,
                report.total_bots,
                "성공({}) + 실패({}) != 전체({})",
                report.success_count, report.failure_count, report.total_bots
            );
        }
    }

    // ── 5.2 Property 2: Display 포맷 라운드트립 ─────────────────────
    // **Validates: Requirements 5.3**
    proptest! {
        #[test]
        fn prop_scenario_report_display_roundtrip(
            report in arb_scenario_report()
        ) {
            let output = format!("{report}");
            let parsed = parse_report_display(&output);
            prop_assert!(parsed.is_some(), "Display 출력 파싱 실패: {}", output);

            let (mode, total_bots, success, failure, elapsed_str, rtt_str) = parsed.unwrap();

            prop_assert_eq!(&mode, &report.mode);
            prop_assert_eq!(total_bots, report.total_bots);
            prop_assert_eq!(success, report.success_count);
            prop_assert_eq!(failure, report.failure_count);

            // elapsed: "{:.2}s" 형식 검증
            let expected_elapsed = format!("{:.2}s", report.elapsed_secs);
            prop_assert_eq!(&elapsed_str, &expected_elapsed);

            // avg_rtt 검증
            let expected_rtt = match report.avg_rtt_ms {
                Some(ms) => format!("{ms}ms"),
                None => "N/A".to_string(),
            };
            prop_assert_eq!(&rtt_str, &expected_rtt);
        }
    }

    // ── 5.3 Property 3: 평균 RTT 계산 정확성 ───────────────────────
    // **Validates: Requirements 3.2, 3.3**
    proptest! {
        #[test]
        fn prop_rtt_counter_average_correctness(
            values in prop::collection::vec(1u64..=10_000, 0..100)
        ) {
            let counter = RttCounter::new();
            for &v in &values {
                counter.record(v);
            }

            if values.is_empty() {
                prop_assert_eq!(counter.average(), None,
                    "빈 RttCounter의 average()는 None이어야 함");
            } else {
                let expected_sum: u64 = values.iter().sum();
                let expected_avg = expected_sum / values.len() as u64;
                prop_assert_eq!(counter.average(), Some(expected_avg),
                    "average() 불일치: values={:?}, sum={}, count={}",
                    values, expected_sum, values.len());
            }
        }
    }

    // ── 5.4 Property 4: Display 출력 필드 완전성 ────────────────────
    // **Validates: Requirements 4.2, 5.2**
    proptest! {
        #[test]
        fn prop_scenario_report_display_field_completeness(
            report in arb_scenario_report()
        ) {
            let output = format!("{report}");

            let required_keys = ["mode:", "total_bots:", "success:", "failure:", "elapsed:", "avg_rtt:", "vote_integrity:"];
            for key in &required_keys {
                prop_assert!(output.contains(key),
                    "Display 출력에 '{}' 키가 누락됨. 출력:\n{}", key, output);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Task 5: 투표 정합성 단위 테스트
    // ══════════════════════════════════════════════════════════════════

    // ── 5.1 tally_votes() 기본 동작 테스트 ──────────────────────────

    #[test]
    fn tally_votes_basic() {
        // 4명이 각각 옵션 0, 1, 2, 3에 투표
        let votes = vec![Some(0), Some(1), Some(2), Some(3)];
        let result = tally_votes(&votes);
        assert_eq!(result, [1, 1, 1, 1]);
    }

    #[test]
    fn tally_votes_multiple_same_option() {
        // 여러 명이 같은 옵션에 투표
        let votes = vec![Some(0), Some(0), Some(1), Some(2), Some(2), Some(2)];
        let result = tally_votes(&votes);
        assert_eq!(result, [2, 1, 3, 0]);
    }

    // ── 5.2 tally_votes() 빈 입력 테스트 ───────────────────────────

    #[test]
    fn tally_votes_empty_input() {
        let votes: Vec<Option<usize>> = vec![];
        let result = tally_votes(&votes);
        assert_eq!(result, [0, 0, 0, 0]);
    }

    // ── 5.3 tally_votes() None 및 범위 초과 입력 테스트 ─────────────

    #[test]
    fn tally_votes_none_excluded() {
        let votes = vec![Some(0), None, Some(1), None];
        let result = tally_votes(&votes);
        assert_eq!(result, [1, 1, 0, 0]);
    }

    #[test]
    fn tally_votes_out_of_range_excluded() {
        // N_OPTIONS == 4이므로 4 이상은 제외
        let votes = vec![Some(0), Some(4), Some(100), Some(3)];
        let result = tally_votes(&votes);
        assert_eq!(result, [1, 0, 0, 1]);
    }

    #[test]
    fn tally_votes_all_none() {
        let votes = vec![None, None, None];
        let result = tally_votes(&votes);
        assert_eq!(result, [0, 0, 0, 0]);
    }

    // ── 5.4 check_vote_integrity() PASS 케이스 테스트 ───────────────

    #[test]
    fn check_vote_integrity_pass() {
        let expected = [3, 2, 1, 4];
        let actual = [3, 2, 1, 4]; // 옵션별 분포까지 정확히 일치
        let result = check_vote_integrity(expected, actual, 10);
        assert!(result.passed);
        assert_eq!(result.expected, expected);
        assert_eq!(result.actual, actual);
        assert_eq!(result.fickle_count, 10);
    }

    // ── 5.5 check_vote_integrity() FAIL 케이스 테스트 ───────────────

    #[test]
    fn check_vote_integrity_fail() {
        let expected = [3, 2, 1, 4]; // 총합 10
        let actual = [2, 2, 1, 4];   // 총합 9
        let result = check_vote_integrity(expected, actual, 10);
        assert!(!result.passed);
        assert_eq!(result.expected, expected);
        assert_eq!(result.actual, actual);
        assert_eq!(result.fickle_count, 10);
    }

    // ── 5.6 ScenarioReport Display에 vote_integrity PASS/FAIL/N/A 출력 테스트 ─

    #[test]
    fn scenario_report_display_vote_integrity_pass() {
        let report = ScenarioReport {
            mode: "fickle".to_string(),
            total_bots: 10,
            success_count: 10,
            failure_count: 0,
            elapsed_secs: 1.0,
            avg_rtt_ms: None,
            vote_integrity: Some(VoteIntegrityResult {
                passed: true,
                expected: [3, 2, 3, 2],
                actual: [3, 2, 3, 2],
                fickle_count: 10,
            }),
        };
        let output = format!("{report}");
        assert!(output.contains("vote_integrity: PASS"));
    }

    #[test]
    fn scenario_report_display_vote_integrity_fail() {
        let report = ScenarioReport {
            mode: "fickle".to_string(),
            total_bots: 10,
            success_count: 10,
            failure_count: 0,
            elapsed_secs: 1.0,
            avg_rtt_ms: None,
            vote_integrity: Some(VoteIntegrityResult {
                passed: false,
                expected: [3, 2, 3, 2],
                actual: [2, 2, 3, 2],
                fickle_count: 10,
            }),
        };
        let output = format!("{report}");
        assert!(output.contains("vote_integrity: FAIL"));
    }

    #[test]
    fn scenario_report_display_vote_integrity_na() {
        let report = ScenarioReport {
            mode: "normal".to_string(),
            total_bots: 10,
            success_count: 10,
            failure_count: 0,
            elapsed_secs: 1.0,
            avg_rtt_ms: None,
            vote_integrity: None,
        };
        let output = format!("{report}");
        assert!(output.contains("vote_integrity: N/A"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Task 6: 투표 정합성 속성 기반 테스트 (Property-Based Tests)
    // ══════════════════════════════════════════════════════════════════

    /// 유효한 Option<usize> 투표 목록을 생성하는 전략
    /// Some(0..N_OPTIONS), Some(범위 초과), None을 혼합
    fn arb_vote_list() -> impl Strategy<Value = Vec<Option<usize>>> {
        prop::collection::vec(
            prop_oneof![
                // 유효한 투표 (0..N_OPTIONS)
                (0..N_OPTIONS).prop_map(Some),
                // 범위 초과 투표
                (N_OPTIONS..N_OPTIONS + 100).prop_map(Some),
                // None (투표 없음)
                Just(None),
            ],
            0..200,
        )
    }

    // ── 6.1 Property 1: 투표 집계 합계 불변량 ───────────────────────
    // **Validates: Requirements 2.2**
    proptest! {
        #[test]
        fn prop_tally_votes_sum_invariant(votes in arb_vote_list()) {
            let result = tally_votes(&votes);
            let result_sum: u64 = result.iter().sum();

            // 유효 투표 수: Some(v)이고 v < N_OPTIONS인 항목의 수
            let valid_count = votes.iter()
                .filter(|v| matches!(v, Some(opt) if *opt < N_OPTIONS))
                .count() as u64;

            prop_assert_eq!(result_sum, valid_count,
                "tally_votes 결과 총합({})이 유효 투표 수({})와 불일치. votes={:?}",
                result_sum, valid_count, votes);
        }
    }

    // ── 6.2 Property 2: 정합성 판정 일관성 ──────────────────────────
    // **Validates: Requirements 4.1, 4.2, 4.3**
    proptest! {
        #[test]
        fn prop_check_vote_integrity_consistency(
            expected in prop::array::uniform4(0u64..1000),
            actual in prop::array::uniform4(0u64..1000),
            fickle_count in 1usize..100
        ) {
            let result = check_vote_integrity(expected, actual, fickle_count);

            let should_pass = expected == actual;

            prop_assert_eq!(result.passed, should_pass,
                "passed({})가 element-wise 비교와 불일치. expected={:?}, actual={:?}",
                result.passed, expected, actual);
        }
    }

    // ── 6.3 Property 3: Display 라운드트립 (vote_integrity 상태) ────
    // **Validates: Requirements 5.4**
    proptest! {
        #[test]
        fn prop_vote_integrity_display_roundtrip(
            passed in proptest::bool::ANY,
            expected in prop::array::uniform4(0u64..100),
            actual in prop::array::uniform4(0u64..100),
            fickle_count in 1usize..50
        ) {
            // vote_integrity가 Some인 경우
            let report = ScenarioReport {
                mode: "fickle".to_string(),
                total_bots: 10,
                success_count: 10,
                failure_count: 0,
                elapsed_secs: 1.0,
                avg_rtt_ms: None,
                vote_integrity: Some(VoteIntegrityResult {
                    passed,
                    expected,
                    actual,
                    fickle_count,
                }),
            };
            let output = format!("{report}");

            // vote_integrity 라인에서 상태 복원
            let vi_line = output.lines()
                .find(|l| l.starts_with("vote_integrity:"))
                .expect("vote_integrity 라인 없음");

            if passed {
                prop_assert!(vi_line.contains("PASS"),
                    "passed=true인데 PASS가 없음: {}", vi_line);
                prop_assert!(!vi_line.contains("FAIL"),
                    "passed=true인데 FAIL이 있음: {}", vi_line);
            } else {
                prop_assert!(vi_line.contains("FAIL"),
                    "passed=false인데 FAIL이 없음: {}", vi_line);
            }
        }

        #[test]
        fn prop_vote_integrity_na_display_roundtrip(
            mode in prop_oneof![
                Just("normal".to_string()),
                Just("ghost".to_string()),
                Just("quitter".to_string()),
            ]
        ) {
            // vote_integrity가 None인 경우
            let report = ScenarioReport {
                mode,
                total_bots: 10,
                success_count: 10,
                failure_count: 0,
                elapsed_secs: 1.0,
                avg_rtt_ms: None,
                vote_integrity: None,
            };
            let output = format!("{report}");

            let vi_line = output.lines()
                .find(|l| l.starts_with("vote_integrity:"))
                .expect("vote_integrity 라인 없음");

            prop_assert!(vi_line.contains("N/A"),
                "vote_integrity=None인데 N/A가 없음: {}", vi_line);
        }
    }

    // ── 6.4 Property 4: tally_votes 멱등성 ──────────────────────────
    // **Validates: Requirements 2.2**
    proptest! {
        #[test]
        fn prop_tally_votes_idempotent(votes in arb_vote_list()) {
            let result1 = tally_votes(&votes);
            let result2 = tally_votes(&votes);
            prop_assert_eq!(result1, result2,
                "동일 입력에 대해 tally_votes 결과가 다름: {:?} vs {:?}",
                result1, result2);
        }
    }
}
