use std::time::Duration;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use rust_projects::{bot, metrics, room, server, tui, vote};

#[derive(Parser)]
#[command(name = "chat-server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// TCP 채팅 서버 실행
    Server {
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
    },
    /// TUI 채팅 클라이언트
    Client {
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
    },
    /// 봇 시나리오 테스트 실행 (서버가 먼저 실행 중이어야 함)
    BotTest {
        /// 봇 수
        #[arg(long, default_value_t = 500)]
        count: usize,
        /// 봇 모드: normal | fickle | spammer | ghost | quitter | mixed
        #[arg(long, default_value = "normal")]
        mode: String,
        /// 봇 1개당 메시지(또는 투표) 수
        #[arg(long, default_value_t = 100)]
        msg: usize,
        /// 봇 타입별 비율 (mixed 모드 전용). 예: "normal:40,spammer:20,fickle:20,ghost:10,quitter:10"
        #[arg(long)]
        ratio: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Server { addr } => {
            let room = room::Room::new();
            let vote = vote::VoteBoard::new();
            let metrics = metrics::Metrics::new();
            metrics::start_reporter(metrics.clone(), Duration::from_secs(5));
            server::run(&addr, room, vote, metrics).await?;
        }
        Command::Client { addr } => {
            tui::run(&addr).await?;
        }
        Command::BotTest { count, mode, msg, ratio } => {
            bot::run_scenario(&mode, count, msg, ratio.as_deref()).await;
        }
    }

    Ok(())
}
