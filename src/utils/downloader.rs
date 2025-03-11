use super::api::RateLimitedApi;
use anyhow::Context;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct TxnDownloader {
    pub api: Arc<RateLimitedApi>,
    pub tx: mpsc::Sender<EncodedConfirmedTransactionWithStatusMeta>,
    pub start_block: u64,
    pub end_block: u64,
}

/// Worker that downloads blocks from the Solana blockchain and sends transactions to parsers through tokio channels
impl TxnDownloader {
    pub fn new(
        api: Arc<RateLimitedApi>,
        tx: mpsc::Sender<EncodedConfirmedTransactionWithStatusMeta>,
        start_block: u64,
        end_block: u64,
    ) -> Self {
        Self {
            api,
            tx,
            start_block,
            end_block,
        }
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        let block_slots = self
            .api
            .get_block_slots(self.start_block, self.end_block)
            .await?;

        for cur_slot in block_slots {
            let block_data = self
                .api
                .get_block(cur_slot)
                .await
                .context(format!("Failed to fetch block {}", cur_slot))?;

            if block_data.transactions.is_none() {
                continue;
            }
            for txn in block_data.transactions.unwrap() {
                // Reformat block transactions to have same structure as independent transactions queried directly
                // This allows for consistent handling in the parser and support for transactions from blocks and API directly
                let reformatted_txn = EncodedConfirmedTransactionWithStatusMeta {
                    slot: cur_slot,
                    transaction: txn,
                    block_time: block_data.block_time,
                };
                self.tx
                    .send(reformatted_txn)
                    .await
                    .context("Downloader failed to send transaction")?;
            }
        }
        Ok(())
    }
}
