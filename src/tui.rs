use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::Mutex,
};

use crate::protocol::{ClientMsg, N_OPTIONS, ServerMsg};

const MAX_MESSAGES: usize = 200;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct AppState {
    messages: Vec<String>,
    vote_counts: [u64; N_OPTIONS],
    client_count: usize,
    input: String,
    my_vote: Option<usize>,
}

impl AppState {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            vote_counts: [0; N_OPTIONS],
            client_count: 0,
            input: String::new(),
            my_vote: None,
        }
    }

    fn push_msg(&mut self, msg: String) {
        if self.messages.len() >= MAX_MESSAGES {
            self.messages.remove(0);
        }
        self.messages.push(msg);
    }
}

pub async fn run(addr: &str) -> Result<()> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let writer = Arc::new(Mutex::new(BufWriter::new(writer)));

    let state = Arc::new(Mutex::new(AppState::new()));

    // 네트워크 수신 task
    let state_net = state.clone();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(msg) = serde_json::from_str::<ServerMsg>(&line) else {
                continue;
            };
            let mut s = state_net.lock().await;
            match msg {
                ServerMsg::Chat { from, text, .. } => {
                    s.push_msg(format!("[{from}] {text}"));
                }
                ServerMsg::VoteSnapshot { counts } => {
                    s.vote_counts = counts;
                }
                ServerMsg::Presence { id, joined } => {
                    if joined {
                        s.client_count += 1;
                        s.push_msg(format!("▶ {id} 입장"));
                    } else {
                        s.client_count = s.client_count.saturating_sub(1);
                        s.push_msg(format!("◀ {id} 퇴장"));
                    }
                }
                ServerMsg::Error { msg } => {
                    s.push_msg(format!("⚠ {msg}"));
                }
            }
        }
    });

    // 터미널 초기화
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(&mut terminal, state.clone(), writer.clone()).await;

    // 터미널 복원
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    state: Arc<Mutex<AppState>>,
    writer: Arc<Mutex<BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
) -> Result<()> {
    loop {
        // 렌더링
        {
            let s = state.lock().await;
            terminal.draw(|f| render(f, &s))?;
        }

        // 키 이벤트 (16ms 타임아웃 → ~60fps)
        if event::poll(Duration::from_millis(16))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                // Ctrl+C / Esc → 종료
                if key.code == KeyCode::Esc
                    || (key.modifiers.contains(KeyModifiers::CONTROL)
                        && key.code == KeyCode::Char('c'))
                {
                    break;
                }

                match key.code {
                    KeyCode::Enter => {
                        let input = {
                            let mut s = state.lock().await;
                            let input = s.input.trim().to_string();
                            s.input.clear();
                            input
                        };

                        if !input.is_empty() {
                            let msg = parse_input(&input, &state).await;
                            if let Some(msg) = msg {
                                send(&writer, &msg).await?;
                            }
                        }
                    }
                    KeyCode::Backspace => {
                        state.lock().await.input.pop();
                    }
                    KeyCode::Char(c) => {
                        state.lock().await.input.push(c);
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn parse_input(input: &str, state: &Arc<Mutex<AppState>>) -> Option<ClientMsg> {
    if let Some(rest) = input.strip_prefix("/vote ") {
        if let Ok(n) = rest.trim().parse::<usize>() {
            let option = n.saturating_sub(1); // 1-based → 0-based
            if option < N_OPTIONS {
                state.lock().await.my_vote = Some(option);
                return Some(ClientMsg::Vote { option });
            }
        }
        return None;
    }

    if input == "/unvote" {
        state.lock().await.my_vote = None;
        return Some(ClientMsg::Unvote);
    }

    // 이슈 7: 클라이언트 송신 시각 포함
    Some(ClientMsg::Chat {
        text: input.to_string(),
        client_ts: now_ms(),
    })
}

async fn send(
    writer: &Arc<Mutex<BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    msg: &ClientMsg,
) -> Result<()> {
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    let mut w = writer.lock().await;
    w.write_all(line.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

fn render(f: &mut ratatui::Frame, state: &AppState) {
    // 전체 영역을 상단(본문) / 하단(입력창)으로 분할
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(f.area());

    // 상단을 채팅(좌) / 투표(우)로 분할
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(20), Constraint::Length(22)])
        .split(outer[0]);

    render_chat(f, state, top[0]);
    render_vote(f, state, top[1]);
    render_input(f, state, outer[1]);
}

fn render_chat(f: &mut ratatui::Frame, state: &AppState, area: ratatui::layout::Rect) {
    let height = area.height.saturating_sub(2) as usize;
    let items: Vec<ListItem> = state
        .messages
        .iter()
        .rev()
        .take(height)
        .rev()
        .map(|m| ListItem::new(Line::from(m.as_str())))
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Chat  참여자: {} ", state.client_count)),
    );
    f.render_widget(list, area);
}

fn render_vote(f: &mut ratatui::Frame, state: &AppState, area: ratatui::layout::Rect) {
    let block = Block::default().borders(Borders::ALL).title(" 투표 ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let total: u64 = state.vote_counts.iter().sum();
    let row_h = 2u16;

    for (i, &count) in state.vote_counts.iter().enumerate() {
        let y = inner.y + (i as u16) * row_h;
        if y + row_h > inner.y + inner.height {
            break;
        }

        let ratio = if total > 0 {
            count as f64 / total as f64
        } else {
            0.0
        };
        let is_mine = state.my_vote == Some(i);
        let label = if is_mine {
            format!("[{}]▶ {}/{}", i + 1, count, total)
        } else {
            format!("[{}]  {}/{}", i + 1, count, total)
        };

        let gauge = Gauge::default()
            .block(Block::default())
            .gauge_style(
                Style::default()
                    .fg(if is_mine { Color::Yellow } else { Color::Cyan })
                    .add_modifier(if is_mine {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
            )
            .label(label)
            .ratio(ratio);

        let gauge_area = ratatui::layout::Rect {
            x: inner.x,
            y,
            width: inner.width,
            height: row_h,
        };
        f.render_widget(gauge, gauge_area);
    }
}

fn render_input(f: &mut ratatui::Frame, state: &AppState, area: ratatui::layout::Rect) {
    let hint = Span::styled(
        " /vote 1~4  /unvote  Esc:종료",
        Style::default().fg(Color::DarkGray),
    );
    let input_line = Line::from(vec![
        Span::raw("> "),
        Span::styled(&state.input, Style::default().fg(Color::White)),
        hint,
    ]);
    let p =
        Paragraph::new(input_line).block(Block::default().borders(Borders::ALL).title(" 입력 "));
    f.render_widget(p, area);

    // border(1) + "> "(2) + 입력 문자 너비
    let input_width: u16 = state
        .input
        .chars()
        .map(|c| if c.len_utf8() > 1 { 2 } else { 1 })
        .sum();
    f.set_cursor_position((area.x + 1 + 2 + input_width, area.y + 1));
}
