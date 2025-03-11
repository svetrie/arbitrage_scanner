use anyhow::{anyhow, Context, Ok};
use chrono::NaiveDateTime;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_commitment_config::CommitmentConfig;
use solana_transaction_status_client_types::{
    TransactionDetails, UiConfirmedBlock, UiTransactionEncoding,
};
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};

pub struct RateLimitedApi {
    max_requests: usize,
    interval: Duration,
    rpc_clients: Mutex<Vec<RpcClient>>,
    semaphore: Semaphore,
    request_times: Mutex<VecDeque<NaiveDateTime>>,
    rpc_block_config: RpcBlockConfig,
}

/// RateLimitedApi is a wrapper around solana RPC client that limits the number of concurrent requests
/// and the number of requests per a sliding window interval
impl RateLimitedApi {
    pub fn new(
        max_requests: usize,
        interval: Duration,
        max_connections: usize,
        api_url: String,
    ) -> Self {
        let rpc_clients = (0..max_connections)
            .map(|_| RpcClient::new(&api_url))
            .collect();
        let rpc_block_config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        Self {
            max_requests,
            interval,
            rpc_clients: Mutex::new(rpc_clients),
            semaphore: Semaphore::new(max_connections),
            request_times: Mutex::new(VecDeque::new()),
            rpc_block_config,
        }
    }

    pub async fn get_api_client(&self) -> RpcClient {
        let permit = self.semaphore.acquire().await.unwrap();
        permit.forget();

        let mut request_times = self.request_times.lock().await;
        let now = chrono::Utc::now().naive_utc();

        // Remove timestamps that are outside the rate limit window
        while let Some(&time) = request_times.front() {
            if now.signed_duration_since(time).num_seconds() > self.interval.as_secs() as i64 {
                request_times.pop_front();
            } else {
                break;
            }
        }

        // Wait if max request limit has been reached
        if request_times.len() >= self.max_requests {
            let earliest = *request_times.front().unwrap();
            let wait_time = (earliest + chrono::Duration::from_std(self.interval).unwrap())
                .signed_duration_since(now)
                .to_std()
                .unwrap();
            sleep(wait_time).await;
        }

        request_times.push_back(chrono::Utc::now().naive_utc());

        self.rpc_clients.lock().await.pop().unwrap()
    }

    pub async fn release_api_client(&self, rpc_client: RpcClient) {
        self.rpc_clients.lock().await.push(rpc_client);
        self.semaphore.add_permits(1);
    }

    pub async fn get_block_time(&self, block: u64) -> anyhow::Result<i64> {
        let rpc_client = self.get_api_client().await;
        let block_time = rpc_client
            .get_block_time(block)
            .context("Failed to fetch block time")?;
        self.release_api_client(rpc_client).await;
        Ok(block_time)
    }

    pub async fn get_first_block_slot(&self) -> anyhow::Result<u64> {
        let rpc_client = self.get_api_client().await;
        let first_available_block = rpc_client
            .get_first_available_block()
            .context("Failed to fetch first available block")?;
        self.release_api_client(rpc_client).await;
        Ok(first_available_block)
    }

    pub async fn get_latest_block_slot(&self) -> anyhow::Result<u64> {
        let rpc_client = self.get_api_client().await;
        let latest_block = rpc_client
            .get_slot()
            .context("Failed to fetch latest block")?;
        self.release_api_client(rpc_client).await;
        Ok(latest_block)
    }

    pub async fn get_block_slots(&self, start: u64, end: u64) -> anyhow::Result<Vec<u64>> {
        let mut block_slots = vec![];
        // Fetch block slots in batches to avoid requesting too much data at once
        for i in (start..=end).step_by(5000) {
            let rpc_client = self.get_api_client().await;
            let batch_block_slots = rpc_client
                .get_blocks(i, Some(std::cmp::min(i + 5000, end)))
                .context("Failed to fetch blocks")?;
            block_slots.extend(batch_block_slots);
            self.release_api_client(rpc_client).await;
        }
        Ok(block_slots)
    }

    pub async fn get_block(&self, block: u64) -> anyhow::Result<UiConfirmedBlock> {
        let rpc_client = self.get_api_client().await;
        let block = rpc_client
            .get_block_with_config(block, self.rpc_block_config)
            .context("Failed to fetch block")?;
        self.release_api_client(rpc_client).await;
        Ok(block)
    }

    /// Binary search to find block closest to a given timestamp
    pub async fn get_block_for_ts(&self, ts: NaiveDateTime) -> anyhow::Result<u64> {
        let ts = ts.and_utc().timestamp();
        let mut left = self.get_first_block_slot().await?;
        let mut right = self.get_latest_block_slot().await?;

        while left < right {
            let mid = (left + right) / 2;
            // Not all slots have blocks, so fetch a range of blocks to find the closest one
            let blocks = self
                .get_block_slots(mid, std::cmp::min((mid + 50).into(), right.into()))
                .await?;

            let mid = blocks.get(0).ok_or(anyhow!("No blocks found"))?;
            let block_time = self.get_block_time(*mid).await?;

            if block_time == ts {
                return Ok(*mid);
            } else if block_time > ts {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return Ok(left);
    }
}
