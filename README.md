
# Arbitrage Scanner #

## Scanning Logic Overview ##

At a high level this project can be broken down into two steps
- Query Solana transactions for a given time range using the RPC API
- Parse transactions and ingest arbitrages to clickhouse

### Querying Data from Solana ###

The RPC API doesn't seem to have built in support for querying data by timestamps. Instead, we have to represent the start and end points of our time interval as block slots.

To identify the block slots corresponding to our start and end timestamps, we perform binary search on available slots. The RPC API has `getFirstAvailableBlock` and `getSlot` endpoints that will return the first and last slots. We perform binary search on this range, comparing the block time of our mid point block to the timestamp we're searching for at each iteration.

After converting our time interval to a pair of start and end block slots, we can then retrieve all slots in this range with assigned blocks using `getBlocks` endpoint. This step ensures we don't pass in invalid slots to `getBlock` endpoint when a block is not present. Finally for each valid slot, we request block data. Each block can contain several transactions for us to parse 

### Parsing Transactions ###

Here's an outline for how we identify arbitrage for each individual transaction.

First, the parser expects the transaction to be structured in a specific format and will skip the transaction if
- The txn is not in json format
- The message section of the txn is decoded
- The meta section of the txn is missing
- Token balances within the meta section are missing

We identify the signer of the txn by scanning the account keys section. If there are multiple signers, we skip the transaction as its not obvious how to define arbitrage in this situation (i.e. do all signers need to profit or just one?). I also hypothesize that multsig transactions are rarely used for arbitrage. Since arbitrage opportunities are usually latency sensitive, the additional overhead of requiring more signatures seems less effective.

Next we determine the amount of SOL recieved by the signer due to closeAccount instructions. We parse the instructions section and track the closed accounts that transferred SOL to the signer. We sum the lamport pre balances for those accounts. 

We compute PnL in SOL for the signer by finding the difference between their pre balance lamports and post balance lamports. The closedAccount SOL from the prior step should be deducted from the PnL as well

We apply similar logic to the signer's pre token balances and post token balances to compute PnL for other tokens. A couple things to note here
- It's possible for tokens to be in pre token balances but not post token balances. In this case, we assume the token's balance was fully depleted during the transaction. This will be considered a deficit
- Similarly, it's possible for a token to not exist in the pre token balances but appear in the post token balances. This will be considered a profit
- WSOL and SOL are fungible for PnL computation. So the SOL PnL from the prior step will be merged with the WSOL PnL
- If the arbitraged token is SOL, the txn fee will be reflected in the overall profit

A transaction is considered arbitrage if all post token amounts are >= pre token amounts and at least one post token amount is > pre token amount. If the arbitraged token is SOL, the profit must be greater than the txn fee for it to be considered arbitrage. For other tokens, we ignore the txn fee.

### Improvements/Issues ###

The ideal solution would probably involve processing each individual instruction, identifying swaps, and applying the impact on the user's token balances at each step. Any contiguous subsequence of swaps within the txn where the pre and post token balances adhere to the criteria mentioned above could be tagged as arbitrage. 

The current approach is not as robust since I didn't know how to handle instructions that weren't decoded. This made it difficult to reliably identify swap instructions and impacted tokens. So as a workaround, I relied on comparing pre and post token balances for the entire transaction rather than at the instruction level. However this can lead to both false positives and false negatives. Here are some potential scenarios
- Consider a sequence of instructions A -> B -> C where A -> B is a swap sequence resulting in arbitrage and C transfers the profit to a different account than the signer. This would lead to a false negative in the current approach - no arbitrage would be detected since the profit would not be reflected in the signer's post balances
- The signer recieves some reward in the form of tokens for interacting with a defi protocol (staking, lending, etc). This could lead to a false positive in the current approach - the reward would impact the signer's post token balances and indicate arbitrage profit despite no swaps occurring

Another potential issue to consider are scenarios where the arbitrage profit is less than the txn fee based on some conversion rate. The current approach doesn't attempt to standardize non-SOL token profits and txn fees to the same units for comparison.

## Architecture Overview ##

### System Design ###

The system consists of three main components:
- RateLimitedApi
- TxnDownloader
- TxnParser

The RateLimitedApi is a wrapper around the Solana RPC API that manages concurrent connections and applies a sliding window approach to rate limiting requests per a given interval. I frequently encountered http 429 errors when using the RPC API directly and wanted to simplify usage.

TxnDownloaders query blocks using the RateLimitedApi and send txns from these blocks to TxnParsers through a tokio mpsc channel.

TxnParsers recieve txns from TxnDownloaders. They parse, detect, and ingest arbitrage txns into Clickhouse.

Multiple downloaders and parsers can be run in parallel. They are decoupled in order to scale separately. This seems sensible as downloaders are expected to be a bigger bottleneck compared to parsers. 


### Database Choice ###

I chose Clickhouse to store arbitrage txns for a few reasons
- Clickhouse is optimized for analytical query patterns. Many of the metrics we're interested in (total profit per token, number of arbitrages per token, etc) involve aggregations that fit these patterns
- There is significant arbitrage txn volume on Solana. As the dataset continues to grow, columnar storage can be more efficient with better compression
- Clickhouse works well with immutable data that is append-only such as finalized blockchain transactions

Within Clickhouse, the data is partitioned based on txn timestamps for better performance when computing metrics over time ranges