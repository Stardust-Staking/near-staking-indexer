use crate::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::Write;
use std::str::FromStr;
use fastnear_primitives::near_indexer_primitives::{
    IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome,
};
use fastnear_primitives::near_primitives::borsh::BorshDeserialize;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_primitives::views::{
    ActionView, ReceiptEnumView, SignedTransactionView,
};
use fastnear_primitives::near_primitives::{borsh, views};

use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::common::Row;

const LAST_BLOCK_HEIGHT_KEY: &str = "last_block_height";

const BLOCK_HEADERS_KEY: &str = "block_headers";
const RECEIPT_TO_TX_KEY: &str = "receipt_to_tx";
const DATA_RECEIPTS_KEY: &str = "data_receipts";
const TRANSACTIONS_KEY: &str = "transactions";

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

const BLOCK_HEADER_CLEANUP: u64 = 2000;

const POTENTIAL_ACCOUNT_ARGS: [&str; 21] = [
    "receiver_id",
    "account_id",
    "sender_id",
    "new_account_id",
    "predecessor_account_id",
    "contract_id",
    "owner_id",
    "token_owner_id",
    "nft_contract_id",
    "token_account_id",
    "creator_id",
    "referral_id",
    "previous_owner_id",
    "seller_id",
    "buyer_id",
    "user_id",
    "beneficiary_id",
    "staking_pool_account_id",
    "owner_account_id",
    "claimer",
    "bounty_owner",
];

const POTENTIAL_EVENTS_ARGS: [&str; 10] = [
    "account_id",
    "owner_id",
    "old_owner_id",
    "new_owner_id",
    "payer_id",
    "farmer_id",
    "validator_id",
    "liquidation_account_id",
    "contract_id",
    "nft_contract_id",
];

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Vec<Value>,
}

#[derive(Serialize, Clone)]
pub struct TransactionRow {
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_hash: String,
    pub tx_block_timestamp: u64,
    pub transaction: Value,
    pub last_block_height: u64,
}

impl From<TransactionRow> for Row {
    fn from(value: TransactionRow) -> Self {
        Row::TransactionRow(value)
    }
}

#[derive(Serialize, Clone)]
pub struct AccountTxRow {
    pub account_id: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
}

impl From<AccountTxRow> for Row {
    fn from(value: AccountTxRow) -> Self {
        Row::AccountTxRow(value)
    }
}

#[derive(Serialize, Clone)]
pub struct BlockTxRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
}

impl From<BlockTxRow> for Row {
    fn from(value: BlockTxRow) -> Self {
        Row::BlockTxRow(value)
    }
}

#[derive(Serialize, Clone)]
pub struct ReceiptTxRow {
    pub receipt_id: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub tx_block_height: u64,
    pub tx_block_timestamp: u64,
}

impl From<ReceiptTxRow> for Row {
    fn from(value: ReceiptTxRow) -> Self {
        Row::ReceiptTxRow(value)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionView {
    pub transaction: SignedTransactionView,
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipts: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub data_receipts: Vec<views::ReceiptView>,
}

fn trim_execution_outcome(execution_outcome: &mut views::ExecutionOutcomeWithIdView) {
    execution_outcome.proof.clear();
    execution_outcome.outcome.metadata.gas_profile = None;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PendingTransaction {
    pub tx_block_height: BlockHeight,
    pub tx_block_hash: CryptoHash,
    pub tx_block_timestamp: u64,
    pub blocks: Vec<BlockHeight>,
    pub transaction: TransactionView,
    pub pending_receipt_ids: Vec<CryptoHash>,
}

#[derive(Default)]
pub struct TxRows {
    pub transactions: Vec<TransactionRow>,
    pub account_txs: Vec<AccountTxRow>,
    pub block_txs: Vec<BlockTxRow>,
    pub receipt_txs: Vec<ReceiptTxRow>,
}

impl PendingTransaction {
    pub fn transaction_hash(&self) -> CryptoHash {
        self.transaction.transaction.hash
    }
}

#[derive(Clone)]
pub struct WatchListEntry {
    pub account_id: String,
    pub is_regex: bool,
}

impl From<(String, bool)> for WatchListEntry {
    fn from(value: (String, bool)) -> Self {
        WatchListEntry {
            account_id: value.0,
            is_regex: value.1,
        }
    }
}

pub struct TransactionsData {
    pub commit_every_block: bool,
    pub tx_cache: TxCache,
    pub rows: TxRows,
    pub watch_list: Vec<WatchListEntry>,
}

impl TransactionsData {
    pub fn new() -> Self {
        let commit_every_block = env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        let sled_db_path = env::var("SLED_DB_PATH").expect("Missing SLED_DB_PATH env var");
        if std::path::Path::new(&sled_db_path).exists() {
            std::fs::remove_dir_all(&sled_db_path)
              .expect(format!("Failed to remove {}", sled_db_path).as_str());
        }
        std::fs::create_dir_all(&sled_db_path)
          .expect(format!("Failed to create {}", sled_db_path).as_str());
        let sled_db = sled::open(&sled_db_path).expect("Failed to open sled_db_path");
        let tx_cache = TxCache::new(sled_db);

        Self {
            commit_every_block,
            tx_cache,
            rows: TxRows::default(),
            watch_list: vec![],
        }
    }

    pub async fn process_block(
        &mut self,
        db: &PostgresDB,
        block: BlockWithTxHashes,
        last_db_block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let block_timestamp = block.block.header.timestamp;

        let skip_missing_receipts = block_height <= last_db_block_height;

        self.tx_cache.insert_block_header(block.block.header);

        let mut complete_transactions = vec![];

        let mut shards = block.shards;
        for shard in &mut shards {
            if let Some(chunk) = shard.chunk.take() {
                for IndexerTransactionWithOutcome {
                    transaction,
                    mut outcome,
                } in chunk.transactions
                {
                    let pending_receipt_ids = outcome.execution_outcome.outcome.receipt_ids.clone();
                    trim_execution_outcome(&mut outcome.execution_outcome);
                    let pending_transaction = PendingTransaction {
                        tx_block_height: block_height,
                        tx_block_hash: block_hash,
                        tx_block_timestamp: block_timestamp,
                        blocks: vec![block_height],
                        transaction: TransactionView {
                            transaction,
                            execution_outcome: outcome.execution_outcome,
                            receipts: vec![],
                            data_receipts: vec![],
                        },
                        pending_receipt_ids,
                    };
                    let pending_receipt_ids = pending_transaction.pending_receipt_ids.clone();
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
                for receipt in chunk.receipts {
                    match receipt.receipt {
                        ReceiptEnumView::Action { .. } => {
                            // skipping here, since we'll get one with execution
                        }
                        ReceiptEnumView::Data { data_id, .. } => {
                            self.tx_cache.insert_data_receipt(&data_id, receipt);
                        }
                    }
                }
            }
        }

        for shard in shards {
            for outcome in shard.receipt_execution_outcomes {
                let receipt = outcome.receipt;
                let mut execution_outcome = outcome.execution_outcome;
                trim_execution_outcome(&mut execution_outcome);
                let receipt_id = receipt.receipt_id;
                let tx_hash = match self.tx_cache.get_and_remove_receipt_to_tx(&receipt_id) {
                    Some(tx_hash) => tx_hash,
                    None => {
                        if skip_missing_receipts {
                            tracing::log::warn!(target: PROJECT_ID, "Missing tx_hash for action receipt_id: {}", receipt_id);
                            continue;
                        }
                        panic!("Missing tx_hash for receipt_id");
                    }
                };
                let mut pending_transaction = self
                    .tx_cache
                    .get_and_remove_transaction(&tx_hash)
                    .expect("Missing transaction for receipt");
                pending_transaction
                    .pending_receipt_ids
                    .retain(|r| r != &receipt_id);
                if pending_transaction.blocks.last() != Some(&block_height) {
                    pending_transaction.blocks.push(block_height);
                }

                // Extracting matching data receipts
                match &receipt.receipt {
                    ReceiptEnumView::Action { input_data_ids, .. } => {
                        let mut ok = true;
                        for data_id in input_data_ids {
                            let data_receipt = match self
                                .tx_cache
                                .get_and_remove_data_receipt(data_id)
                            {
                                Some(data_receipt) => data_receipt,
                                None => {
                                    if skip_missing_receipts {
                                        tracing::log::warn!(target: PROJECT_ID, "Missing data receipt for data_id: {}", data_id);
                                        ok = false;
                                        break;
                                    }
                                    panic!("Missing data receipt for data_id");
                                }
                            };

                            pending_transaction
                                .transaction
                                .data_receipts
                                .push(data_receipt);
                        }
                        if !ok {
                            for receipt_id in &pending_transaction.pending_receipt_ids {
                                self.tx_cache.remove_receipt_to_tx(receipt_id);
                            }
                            continue;
                        }
                    }
                    ReceiptEnumView::Data { .. } => {
                        unreachable!("Data receipt should be processed before")
                    }
                };

                let pending_receipt_ids = execution_outcome.outcome.receipt_ids.clone();
                pending_transaction
                    .transaction
                    .receipts
                    .push(IndexerExecutionOutcomeWithReceipt {
                        execution_outcome,
                        receipt,
                    });
                pending_transaction
                    .pending_receipt_ids
                    .extend(pending_receipt_ids.clone());
                if pending_transaction.pending_receipt_ids.is_empty() {
                    // Received the final receipt.
                    if self.some_account_in_watch_list(&pending_transaction) {
                        complete_transactions.push(pending_transaction);
                    }
                } else {
                    self.tx_cache
                        .insert_transaction(pending_transaction, &pending_receipt_ids);
                }
            }
        }

        self.tx_cache.trim_headers();

        self.tx_cache.set_u64(LAST_BLOCK_HEIGHT_KEY, block_height);
        // self.tx_cache.flush();

        tracing::log::info!(target: PROJECT_ID, "#{}: Complete {} transactions. Pending {}", block_height, complete_transactions.len(), self.tx_cache.stats());

        if block_height > last_db_block_height {
            for transaction in complete_transactions {
                self.process_transaction(transaction).await?;
            }
        }

        self.maybe_commit(db, block_height).await?;

        Ok(())
    }

    async fn process_transaction(&mut self, transaction: PendingTransaction) -> anyhow::Result<()> {
        let tx_hash = transaction.transaction_hash().to_string();
        let last_block_height = *transaction.blocks.last().unwrap();
        let signer_id = transaction
            .transaction
            .transaction
            .signer_id
            .clone()
            .to_string();

        for block_height in transaction.blocks.clone() {
            let block_header = self.tx_cache.get_and_remove_block_header(block_height);
            if let Some(block_header) = block_header {
                self.rows.block_txs.push(BlockTxRow {
                    block_height,
                    block_hash: block_header.hash.to_string(),
                    block_timestamp: block_header.timestamp,
                    transaction_hash: tx_hash.clone(),
                    signer_id: signer_id.clone(),
                    tx_block_height: transaction.tx_block_height,
                });
                self.tx_cache.insert_block_header(block_header);
            } else {
                tracing::log::warn!(target: PROJECT_ID, "Missing block header #{} for a transaction {}", block_height, tx_hash.clone());
                // Append to a file a record about a missing
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open("missing_block_headers.txt")
                    .expect("Failed to open missing_block_headers.txt");
                writeln!(
                    file,
                    "{} {} {} {}",
                    block_height, tx_hash, signer_id, transaction.tx_block_height
                )
                .expect("Failed to write to missing_block_headers.txt");
            }
        }

        for receipt in &transaction.transaction.receipts {
            let receipt_id = receipt.receipt.receipt_id.to_string();
            self.rows.receipt_txs.push(ReceiptTxRow {
                receipt_id,
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
        }
        for data_receipt in &transaction.transaction.data_receipts {
            let receipt_id = data_receipt.receipt_id.to_string();
            self.rows.receipt_txs.push(ReceiptTxRow {
                receipt_id,
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
        }

        let accounts = Self::get_accounts_from_transaction(&transaction);
        for account_id in accounts {
            self.rows.account_txs.push(AccountTxRow {
                account_id: account_id.to_string(),
                transaction_hash: tx_hash.clone(),
                signer_id: signer_id.clone(),
                tx_block_height: transaction.tx_block_height,
                tx_block_timestamp: transaction.tx_block_timestamp,
            });
        }

        self.rows.transactions.push(TransactionRow {
            transaction_hash: tx_hash.clone(),
            signer_id: signer_id.clone(),
            tx_block_height: transaction.tx_block_height,
            tx_block_hash: transaction.tx_block_hash.to_string(),
            tx_block_timestamp: transaction.tx_block_timestamp,
            transaction: serde_json::to_value(&transaction.transaction).unwrap(),
            last_block_height,
        });

        // TODO: Save TX to redis

        Ok(())
    }

    pub async fn maybe_commit(
        &mut self,
        db: &PostgresDB,
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(
                target: POSTGRES_TARGET,
                "#{}: Having {} transactions, {} account_txs, {} block_txs, {} receipts_txs",
                block_height,
                self.rows.transactions.len(),
                self.rows.account_txs.len(),
                self.rows.block_txs.len(),
                self.rows.receipt_txs.len(),
            );
        }
        if self.rows.transactions.len() >= db.min_batch || is_round_block || self.commit_every_block
        {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &PostgresDB) -> anyhow::Result<()> {
        let mut rows = TxRows::default();
        std::mem::swap(&mut rows, &mut self.rows);

        if !rows.transactions.is_empty() {
            db.insert_rows_with_retry(
                &rows.transactions.clone().into_iter().map(|r| r.into()).collect(),
                "transactions"
            ).await?;
        }
        if !rows.account_txs.is_empty() {
            db.insert_rows_with_retry(
                &rows.account_txs.clone().into_iter().map(|r| r.into()).collect(),
                "account_txs"
            ).await?;
        }
        if !rows.block_txs.is_empty() {
            db.insert_rows_with_retry(
                &rows.block_txs.clone().into_iter().map(|r| r.into()).collect(),
                "block_txs"
            ).await?;
        }
        if !rows.receipt_txs.is_empty() {
            db.insert_rows_with_retry(
                &rows.receipt_txs.clone().into_iter().map(|r| r.into()).collect(),
                "receipt_txs"
            ).await?;
        }
        tracing::log::info!(
                target: POSTGRES_TARGET,
                "Committed {} transactions, {} account_txs, {} block_txs, {} receipts_txs",
                rows.transactions.len(),
                rows.account_txs.len(),
                rows.block_txs.len(),
                rows.receipt_txs.len(),
            );
        rows.transactions.clear();
        rows.account_txs.clear();
        rows.block_txs.clear();
        rows.receipt_txs.clear();

        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &PostgresDB) -> BlockHeight {
        let db_block = db.max("block_height", "block_txs").await.unwrap_or(0);
        let cache_block = self.tx_cache.get_u64(LAST_BLOCK_HEIGHT_KEY).unwrap_or(0);
        db_block.max(cache_block)
    }

    pub fn is_cache_ready(&self, last_block_height: BlockHeight) -> bool {
        let cache_block = self.tx_cache.get_u64(LAST_BLOCK_HEIGHT_KEY).unwrap_or(0);
        cache_block == last_block_height
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.tx_cache.flush();
        Ok(())
    }

    fn get_accounts_from_transaction(transaction: &PendingTransaction) -> HashSet<AccountId> {
        let mut accounts = HashSet::new();
        accounts.insert(transaction.transaction.transaction.signer_id.clone());

        for receipt in &transaction.transaction.receipts {
            add_accounts_from_receipt(&mut accounts, &receipt.receipt);
            add_accounts_from_logs(&mut accounts, &receipt.execution_outcome.outcome.logs);
        }

        accounts
    }

    fn some_account_in_watch_list(&self, transaction: &PendingTransaction) -> bool {
        let accounts = Self::get_accounts_from_transaction(transaction);

        self.watch_list
          .clone()
          .into_iter()
          .find(
              |e|
                  accounts
                    .clone()
                    .into_iter()
                    .find(|a| if e.is_regex {
                        let re = Regex::new(e.account_id.as_str()).unwrap();
                        re.is_match(a.as_str())
                    } else {
                        a.to_string() == e.clone().account_id
                    })
                    .is_some()
          )
          .is_some()
    }

    pub fn set_watch_list(&mut self, watch_list: Vec<WatchListEntry>) {
        self.watch_list.extend(watch_list);
    }
}

fn extract_accounts(accounts: &mut HashSet<AccountId>, value: &Value, keys: &[&str]) {
    for arg in keys {
        if let Some(account_id) = value.get(arg) {
            if let Some(account_id) = account_id.as_str() {
                if let Ok(account_id) = AccountId::from_str(account_id) {
                    accounts.insert(account_id);
                }
            }
        }
    }
}

fn add_accounts_from_logs(accounts: &mut HashSet<AccountId>, logs: &[String]) {
    for log in logs {
        if log.starts_with(EVENT_JSON_PREFIX) {
            let event_json = &log[EVENT_JSON_PREFIX.len()..];
            if let Ok(event) = serde_json::from_str::<EventJson>(event_json) {
                for data in &event.data {
                    extract_accounts(accounts, data, &POTENTIAL_EVENTS_ARGS);
                }
            }
        }
    }
}

fn add_accounts_from_receipt(accounts: &mut HashSet<AccountId>, receipt: &views::ReceiptView) {
    accounts.insert(receipt.receiver_id.clone());
    match &receipt.receipt {
        ReceiptEnumView::Action { actions, .. } => {
            for action in actions {
                match action {
                    ActionView::FunctionCall { args, .. } => {
                        if let Ok(args) = serde_json::from_slice::<Value>(&args) {
                            extract_accounts(accounts, &args, &POTENTIAL_ACCOUNT_ARGS);
                        }
                    }
                    _ => {}
                }
            }
        }
        ReceiptEnumView::Data { .. } => {}
    }
}

pub struct TxCache {
    pub sled_db: sled::Db,

    pub block_headers: BTreeMap<BlockHeight, views::BlockHeaderView>,
    pub receipt_to_tx: HashMap<CryptoHash, CryptoHash>,
    pub data_receipts: HashMap<CryptoHash, views::ReceiptView>,
    pub transactions: HashMap<CryptoHash, PendingTransaction>,
    pub last_block_height: BlockHeight,
}

impl TxCache {
    pub fn new(sled: sled::Db) -> Self {
        let mut this = Self {
            sled_db: sled,
            block_headers: Default::default(),
            receipt_to_tx: Default::default(),
            data_receipts: Default::default(),
            transactions: Default::default(),
            last_block_height: 0,
        };
        this.last_block_height = this.get_u64(LAST_BLOCK_HEIGHT_KEY).unwrap_or(0);

        this.block_headers = this.get_json(BLOCK_HEADERS_KEY).unwrap_or_default();
        this.receipt_to_tx = this.get_json(RECEIPT_TO_TX_KEY).unwrap_or_default();
        this.data_receipts = this.get_json(DATA_RECEIPTS_KEY).unwrap_or_default();
        this.transactions = this.get_json(TRANSACTIONS_KEY).unwrap_or_default();

        this
    }

    pub fn stats(&self) -> String {
        format!(
            "mem: {} tx, {} r, {} dr, {} h",
            self.transactions.len(),
            self.receipt_to_tx.len(),
            self.data_receipts.len(),
            self.block_headers.len(),
        )
    }

    pub fn flush(&self) {
        self.set_json(BLOCK_HEADERS_KEY, &self.block_headers);
        self.set_json(RECEIPT_TO_TX_KEY, &self.receipt_to_tx);
        self.set_json(DATA_RECEIPTS_KEY, &self.data_receipts);
        self.set_json(TRANSACTIONS_KEY, &self.transactions);

        self.sled_db.flush().expect("Failed to flush");
    }

    pub fn trim_headers(&mut self) {
        while self.block_headers.len() > BLOCK_HEADER_CLEANUP as usize {
            let block_height = self.block_headers.keys().next().unwrap().clone();
            self.block_headers.remove(&block_height);
        }
    }

    fn get_json<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.sled_db
            .get(key)
            .expect("Failed to get")
            .map(|v| serde_json::from_slice(&v).expect("Failed to deserialize"))
    }

    fn set_json<T>(&self, key: &str, value: T) -> bool
    where
        T: Serialize,
    {
        self.sled_db
            .insert(key, serde_json::to_vec(&value).unwrap())
            .expect("Failed to set")
            .is_some()
    }

    pub fn insert_block_header(&mut self, block_header: views::BlockHeaderView) {
        // In-memory insert.
        let block_height = block_header.height;
        let hash = block_header.hash;
        let old_header = self.block_headers.insert(block_height, block_header);
        if let Some(old_header) = old_header {
            assert_eq!(
                old_header.hash, hash,
                "Header mismatch at {}!",
                old_header.height
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate header: {}", old_header.height);
        }
    }

    pub fn get_and_remove_block_header(
        &mut self,
        block_height: BlockHeight,
    ) -> Option<views::BlockHeaderView> {
        self.block_headers.remove(&block_height)
    }

    pub fn get_and_remove_receipt_to_tx(&mut self, receipt_id: &CryptoHash) -> Option<CryptoHash> {
        self.receipt_to_tx.remove(receipt_id)
    }

    pub fn insert_receipt_to_tx(&mut self, receipt_id: &CryptoHash, tx_hash: CryptoHash) {
        // In-memory insert.
        let old_tx_hash = self.receipt_to_tx.insert(*receipt_id, tx_hash);
        if let Some(old_tx_hash) = old_tx_hash {
            assert_eq!(
                old_tx_hash, tx_hash,
                "Duplicate receipt_id: {} with different TX HASHES!",
                receipt_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate receipt_id: {} old_tx_hash: {} new_tx_hash: {}", receipt_id, old_tx_hash, tx_hash);
        }
    }

    fn remove_receipt_to_tx(&mut self, receipt_id: &CryptoHash) {
        self.receipt_to_tx.remove(receipt_id);
    }

    fn insert_data_receipt(&mut self, data_id: &CryptoHash, receipt: views::ReceiptView) {
        let receipt_id = receipt.receipt_id;
        let old_receipt = self.data_receipts.insert(*data_id, receipt);
        // In-memory insert.
        if let Some(old_receipt) = old_receipt {
            assert_eq!(
                old_receipt.receipt_id, receipt_id,
                "Duplicate data_id: {} with different receipt_id!",
                data_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate data_id: {}", data_id);
        }
    }

    fn get_and_remove_data_receipt(&mut self, data_id: &CryptoHash) -> Option<views::ReceiptView> {
        self.data_receipts.remove(data_id)
    }

    fn insert_transaction(
        &mut self,
        pending_transaction: PendingTransaction,
        pending_receipt_ids: &[CryptoHash],
    ) {
        let tx_hash = pending_transaction.transaction_hash();
        for receipt_id in pending_receipt_ids {
            self.insert_receipt_to_tx(receipt_id, tx_hash);
        }

        self.transactions.insert(tx_hash, pending_transaction);
    }

    fn get_and_remove_transaction(&mut self, tx_hash: &CryptoHash) -> Option<PendingTransaction> {
        self.transactions.remove(tx_hash)
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.sled_db
            .get(key)
            .expect("Failed to get")
            .map(|v| u64::try_from_slice(&v).expect("Failed to deserialize"))
    }

    pub fn set_u64(&self, key: &str, value: u64) -> bool {
        self.sled_db
            .insert(key, borsh::to_vec(&value).unwrap())
            .expect("Failed to set")
            .is_some()
    }
}
