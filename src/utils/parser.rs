use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use ahash::AHashMap;
use anyhow::{anyhow, Context, Ok};
use clickhouse::Row;
use serde::Deserialize;

use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiInstruction, UiMessage,
    UiParsedInstruction, UiParsedMessage, UiTransactionStatusMeta, UiTransactionTokenBalance,
};

const WSOL_MINT_ADDR: &str = "So11111111111111111111111111111111111111112";
const LAMPORTS_PER_SOL: f64 = 1000000000.0;
const MAX_DECIMALS: u8 = 18;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct ArbitrageDetails {
    txn_hash: String,
    signer: String,
    token_mint_addr: String,
    token_amount: f64,
    token_symbol: Option<String>,
    was_successful: bool,
    txn_fee: f64,
    txn_timestamp: i64,
    txn_block: u64,
}

#[derive(Debug, Deserialize)]
pub struct TokenMetadata {
    pub address: String,
    pub symbol: String,
    pub decimals: u8,
}

/// TxnParser extracts arbitrage data from transactions and ingests it into Clickhouse.
/// Recieves transactions from Downloaders via a tokio mpsc channel.
pub struct TxnParser {
    // Using mutex to distribute work amongst multiple parsers through shared reciever
    rx: Arc<Mutex<mpsc::Receiver<EncodedConfirmedTransactionWithStatusMeta>>>,
    token_metadata: Arc<AHashMap<String, TokenMetadata>>,
    ch_client: clickhouse::Client,
    ch_table: String,
    batch_insert_size: usize,
}

impl TxnParser {
    pub fn new(
        rx: Arc<Mutex<mpsc::Receiver<EncodedConfirmedTransactionWithStatusMeta>>>,
        token_metadata: Arc<AHashMap<String, TokenMetadata>>,
        ch_client: clickhouse::Client,
        ch_table: String,
        batch_insert_size: usize,
    ) -> Self {
        Self {
            rx,
            token_metadata,
            ch_client,
            ch_table,
            batch_insert_size,
        }
    }

    pub async fn parse(&mut self) -> anyhow::Result<()> {
        let mut arb_data: Vec<ArbitrageDetails> = Vec::with_capacity(self.batch_insert_size);

        while let Some(txn) = self.rx.lock().await.recv().await {
            let txn_arb_details = extract_arb_data_from_txn(txn, &self.token_metadata)?;
            arb_data.extend(txn_arb_details);

            // Write to clickhouse in batches to reduce network overhead
            if arb_data.len() >= self.batch_insert_size {
                self.ingest(&arb_data).await?;
                arb_data.clear();
            }
        }

        if !arb_data.is_empty() {
            self.ingest(&arb_data).await?;
        }

        Ok(())
    }

    pub async fn ingest(&self, arb_data: &[ArbitrageDetails]) -> anyhow::Result<()> {
        let mut ch_insert = self.ch_client.insert(&self.ch_table)?;
        for arb_txn_detail in arb_data.iter() {
            ch_insert
                .write(arb_txn_detail)
                .await
                .context("Failed to write arb txn to Clickhouse buffer")?;
        }
        ch_insert
            .end()
            .await
            .context("Failed to insert arb txns into Clickhouse")?;
        Ok(())
    }
}

// Extract user token balances from pre token balances or post token balances sections
fn extract_user_token_balances(
    token_balances: &[UiTransactionTokenBalance],
    user: &str,
) -> anyhow::Result<AHashMap<String, f64>> {
    let mut token_amts: AHashMap<String, f64> = AHashMap::new();

    for token_balance in token_balances
        .iter()
        .filter(|x| x.owner.clone().unwrap_or_else(|| "".to_string()) == user)
    {
        let mint = token_balance.mint.clone();
        let amount = token_balance
            .ui_token_amount
            .ui_amount_string
            .parse::<f64>()
            .context("Failed to parse token amount")?;
        token_amts.insert(mint.clone(), amount);
    }
    Ok(token_amts)
}

/// Parse transactions instructions to identify all accounts closed by transaction signer
fn extract_closed_accounts<'a>(
    parsed_msg: &'a UiParsedMessage,
    txn_signer: &str,
) -> anyhow::Result<Vec<&'a str>> {
    let mut closed_accounts = vec![];

    for instr in &parsed_msg.instructions {
        if let UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instr)) = instr {
            let instr_type = parsed_instr
                .parsed
                .get("type")
                .and_then(|x| x.as_str())
                .unwrap_or_default();

            if instr_type == "closeAccount" {
                let destination = parsed_instr
                    .parsed
                    .get("info")
                    .and_then(|x| x.get("destination"))
                    .and_then(|x| x.as_str())
                    .context(format!(
                        "Missing or invalid 'destination' field in closeAccount instruction"
                    ))?;

                if destination == txn_signer {
                    let closed_account = parsed_instr
                        .parsed
                        .get("info")
                        .and_then(|x| x.get("account"))
                        .and_then(|x| x.as_str())
                        .context(format!(
                            "Missing or invalid 'account' field in closeAccount instruction"
                        ))?;

                    closed_accounts.push(closed_account);
                }
            }
        }
    }

    Ok(closed_accounts)
}

/// Compute the PnL for the transaction signer in SOL.
/// SOL account balances are represented as lamports and stored in pre_balances and post_balances sections
/// as opposed to token balances sections
pub fn compute_sol_pnl(
    meta: &UiTransactionStatusMeta,
    txn_signer: &str,
    acct_idx_map: &AHashMap<&str, usize>,
    closed_accounts: Vec<&str>,
) -> anyhow::Result<f64> {
    // Identify any SOL that was recieved from closeAccount instructions
    // This amount should not be considered as profit when determining arbitrage
    let mut closed_acct_sol: i64 = 0;

    for acct in closed_accounts {
        if let Some(idx) = acct_idx_map.get(acct) {
            closed_acct_sol += meta.pre_balances[*idx] as i64;
        }
    }

    let signer_idx = acct_idx_map
        .get(txn_signer)
        .context("Signer not found in account map")?;
    let sol_pnl: i64 = meta.post_balances[*signer_idx] as i64
        - meta.pre_balances[*signer_idx] as i64
        - closed_acct_sol;
    let sol_pnl = sol_pnl as f64 / LAMPORTS_PER_SOL;
    Ok(sol_pnl)
}

/// Compute PnL for transaction signer by comparing pre and post token balances
pub fn compute_token_balances_pnl(
    meta: &UiTransactionStatusMeta,
    txn_signer: &str,
    token_metadata: &AHashMap<String, TokenMetadata>,
    sol_pnl: f64,
) -> anyhow::Result<AHashMap<String, f64>> {
    let prev_amounts = meta
        .pre_token_balances
        .as_ref()
        .map(|x| extract_user_token_balances(x, txn_signer))
        .unwrap_or_else(|| Err(anyhow!("Missing pre token balances")))?;
    let mut post_amounts = meta
        .post_token_balances
        .as_ref()
        .map(|x| extract_user_token_balances(x, txn_signer))
        .unwrap_or_else(|| Err(anyhow!("Missing post token balances")))?;

    // Account for any token balances that were fully depleted in the transaction
    for token_mint in prev_amounts.keys() {
        post_amounts.entry(token_mint.to_string()).or_insert(0.0);
    }

    // Treat SOL and WSOL as fungible for PnL calculation
    post_amounts
        .entry(WSOL_MINT_ADDR.to_string())
        .or_insert(0.0);
    *post_amounts.get_mut(WSOL_MINT_ADDR).unwrap() += sol_pnl;

    let mut profits: AHashMap<String, f64> = AHashMap::new();
    let txn_fee = (meta.fee as f64) / LAMPORTS_PER_SOL;
    // Compare floats with a tolerance to account for floating point precision errors
    let compare_floats = |a: f64, b: f64, tol: f64| -> bool { (a - b).abs() < tol };

    for (mint, &post_amt) in &post_amounts {
        // Precision refers to number of decimal places for token
        // Can be extracted from token balances in txns but is also available in token metadata from Jupiter
        let precision = token_metadata
            .get(mint)
            .map(|x| x.decimals)
            .unwrap_or(MAX_DECIMALS);
        let tolerance = 10.0_f64.powi(-(precision as i32));

        let prev_amt = prev_amounts.get(mint).unwrap_or(&0.0);
        let pnl = post_amt - prev_amt;

        // When determining profit for SOL/WSOL, consider txn fee cost.
        // If arbitraged token is SOL/WSOL, txn fee should be deducted from profit since units are the same.
        // For other tokens, not sure how to take txn fee into account
        if mint == WSOL_MINT_ADDR && compare_floats(pnl, -(txn_fee), tolerance) {
            continue;
        } else if compare_floats(pnl, 0.0, tolerance) {
            continue;
        } else if pnl < 0.0 {
            return Ok(AHashMap::new());
        } else if pnl > 0.0 {
            profits.insert(mint.to_string(), pnl);
        }
    }

    Ok(profits)
}

pub fn extract_arb_data_from_txn(
    txn: EncodedConfirmedTransactionWithStatusMeta,
    token_metadata: &AHashMap<String, TokenMetadata>,
) -> anyhow::Result<Vec<ArbitrageDetails>> {
    let ui_txn = match txn.transaction.transaction {
        EncodedTransaction::Json(ui_txn) => ui_txn,
        _ => {
            println!("Unable to parse transaction. Not in JSON format");
            return Ok(vec![]);
        }
    };
    let txn_signature = ui_txn
        .signatures
        .get(0)
        .ok_or_else(|| anyhow!("No txn signature found"))?;
    let parsed_msg = match ui_txn.message {
        UiMessage::Parsed(parsed_msg) => parsed_msg,
        _ => {
            println!(
                "Unable to parse transaction {}. Message section is decoded",
                txn_signature
            );
            return Ok(vec![]);
        }
    };
    if txn.transaction.meta.is_none() {
        println!(
            "Unable to parse transaction {}. Meta section is missing",
            txn_signature
        );
        return Ok(vec![]);
    }
    let meta = txn.transaction.meta.unwrap();
    if meta.pre_token_balances.is_none() || meta.post_token_balances.is_none() {
        println!(
            "Unable to parse transaction {}. Token balances are missing",
            txn_signature
        );
        return Ok(vec![]);
    }

    let mut txn_signer: Option<&str> = None;
    let mut acct_idx_map: AHashMap<&str, usize> = AHashMap::new();
    let mut is_multisig: bool = false;

    // Build a map of account addresses to their index in the account_keys array
    // The index is used to match accounts to their pre and post SOL balances
    for (idx, acct) in parsed_msg.account_keys.iter().enumerate() {
        if acct.signer && txn_signer.is_some() {
            is_multisig = true;
            break;
        } else if acct.signer {
            txn_signer = Some(acct.pubkey.as_str());
        }
        acct_idx_map.insert(acct.pubkey.as_str(), idx);
    }

    if txn_signer.is_none() {
        return Err(anyhow!("No signer found for transaction {}", txn_signature));
    } else if is_multisig {
        // Not sure how to determine profit when multiple signers are involved
        println!("Skipping multisig transaction {}", txn_signature);
        return Ok(vec![]);
    }

    let txn_signer = txn_signer.unwrap();

    let closed_accounts = extract_closed_accounts(&parsed_msg, txn_signer)
        .context(format!("Failed to parse txn {}", txn_signature))?;

    let sol_pnl = compute_sol_pnl(&meta, txn_signer, &acct_idx_map, closed_accounts)?;

    let profits = compute_token_balances_pnl(&meta, txn_signer, token_metadata, sol_pnl)?;

    // Although unlikely, it may be possible for a transaction to arbitrage multiple tokens
    let mut arb_data: Vec<ArbitrageDetails> = vec![];
    for (mint, profit_amt) in profits.iter() {
        arb_data.push(ArbitrageDetails {
            txn_hash: txn_signature.to_string(),
            signer: txn_signer.to_string(),
            token_mint_addr: mint.clone(),
            token_amount: *profit_amt,
            token_symbol: token_metadata.get(mint).map(|x| x.symbol.clone()),
            was_successful: meta.err.is_none(),
            txn_fee: meta.fee as f64 / LAMPORTS_PER_SOL,
            txn_timestamp: txn.block_time.unwrap(),
            txn_block: txn.slot,
        });
    }

    return Ok(arb_data);
}
