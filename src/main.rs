use ahash::AHashMap;
use anyhow::{Context, Ok};
use chrono::NaiveDateTime;
use clap::Parser;

use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use clickhouse::Client;

mod utils;
use utils::api::RateLimitedApi;
use utils::downloader::TxnDownloader;
use utils::parser::{TokenMetadata, TxnParser};

const JUPITER_TOKENS_URL: &str = "https://api.jup.ag/tokens/v1/all";
const SOLANA_RPC_URL: &str = "https://api.mainnet-beta.solana.com";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_parser = parse_naive_datetime)]
    start_ts: NaiveDateTime,
    #[arg(long, value_parser = parse_naive_datetime)]
    end_ts: NaiveDateTime,
    #[arg(long, default_value = "5")]
    num_downloaders: usize,
    #[arg(long, default_value = "1")]
    num_parsers: usize,
    #[arg(long, default_value = SOLANA_RPC_URL)]
    rpc_url: String,
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,
    #[arg(long, default_value = "user")]
    clickhouse_user: String,
    #[arg(long, default_value = "password")]
    clickhouse_password: String,
    #[arg(long, default_value = "arbitrage_details")]
    clickhouse_table: String,
    #[arg(long, default_value = "1000")]
    clickhouse_batch_size: usize,
}

fn parse_naive_datetime(s: &str) -> anyhow::Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("Failed to parse NaiveDateTime from '{}'", s))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Fetch token metadata from Jupiter REST endpoint. This is helpful in mapping token addresses to token symbols
    // The decimal precision of tokens is also used when calculating token PnL
    let token_metadata: AHashMap<String, TokenMetadata> = reqwest::get(JUPITER_TOKENS_URL)
        .await
        .context("Failed to fetch token data")?
        .json::<Vec<TokenMetadata>>()
        .await
        .context("Failed to parse token data")?
        .into_iter()
        .map(|x| (x.address.clone(), x))
        .collect();

    let api = Arc::new(RateLimitedApi::new(
        30,
        Duration::from_secs(10),
        args.num_downloaders,
        args.rpc_url,
    ));

    let start_block = api.get_block_for_ts(args.start_ts).await?;
    let end_block = api.get_block_for_ts(args.end_ts).await?;

    // Create a channel for downloader and parser tasks to communicate
    let (downloader_tx, parser_rx) =
        mpsc::channel::<EncodedConfirmedTransactionWithStatusMeta>(100);
    let mut join_set = JoinSet::new();

    // Spawn downloader tasks
    let interval_size = (end_block - start_block) / args.num_downloaders as u64;
    for i in 0..args.num_downloaders {
        let downloader_start = start_block + i as u64 * interval_size;
        let downloader_end = std::cmp::min(downloader_start + interval_size, end_block);
        let downloader = TxnDownloader::new(
            api.clone(),
            downloader_tx.clone(),
            downloader_start,
            downloader_end,
        );

        join_set.spawn(async move {
            downloader.download().await.unwrap();
        });
    }

    // Spawn parser tasks
    let async_parser_rx = Arc::new(tokio::sync::Mutex::new(parser_rx));
    let token_metadata = Arc::new(token_metadata);
    let ch_client = Client::default()
        .with_url(args.clickhouse_url)
        .with_user(args.clickhouse_user)
        .with_password(args.clickhouse_password);

    for _ in 0..args.num_parsers {
        let mut parser = TxnParser::new(
            async_parser_rx.clone(),
            token_metadata.clone(),
            ch_client.clone(),
            args.clickhouse_table.clone(),
            args.clickhouse_batch_size,
        );

        join_set.spawn(async move {
            parser.parse().await.unwrap();
        });
    }

    drop(downloader_tx);
    join_set.join_all().await;

    Ok(())
}
