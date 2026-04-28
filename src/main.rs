mod bot;
mod client;
mod metrics;
mod protocol;
mod room;
mod server;
mod tui;
mod vote;

use std::time::Duration;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

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
        /// 봇 모드: normal | fickle | spammer | ghost | quitter
        #[arg(long, default_value = "normal")]
        mode: String,
        /// 봇 1개당 메시지(또는 투표) 수
        #[arg(long, default_value_t = 100)]
        msg: usize,
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
        Command::BotTest { count, mode, msg } => {
            bot::run_scenario(&mode, count, msg).await;
        }
    }

    Ok(())
}
