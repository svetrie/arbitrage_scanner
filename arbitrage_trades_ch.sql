CREATE TABLE arbitrage_details
(
    `txn_hash` String,
    `signer` String,
    `token_mint_addr` String,
    `token_amount` Float64,
    `token_symbol` Nullable(String),
    `was_successful` UInt8,
    `txn_fee` Float64,
    `txn_timestamp` Int64,
    `txn_block` UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime64(txn_timestamp, 0))
PRIMARY KEY (txn_timestamp, txn_hash, token_mint_addr)
ORDER BY (txn_timestamp, txn_hash, token_mint_addr)
SETTINGS index_granularity = 8192;