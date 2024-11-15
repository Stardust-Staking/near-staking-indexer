mod actions;
mod model;
pub mod common;

mod transactions;

use crate::actions::ActionsData;
use crate::model::*;
use crate::transactions::TransactionsData;
use std::sync::Arc;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
use fastnear_primitives::types::ChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "provider";

const SAFE_CATCH_UP_OFFSET: u64 = 1000;

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        println!("Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    common::setup_tracing("postgres=info,provider=info,neardata-fetcher=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Postgres Provider");

    let db = PostgresDB::new(10000).await;

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");
    let num_threads = std::env::var("NUM_FETCHING_THREADS")
        .expect("NUM_FETCHING_THREADS is not set")
        .parse::<u64>()
        .expect("Invalid NUM_FETCHING_THREADS");

    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let args: Vec<String> = std::env::args().collect();
    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command");

    match command {
        "actions" => {
            let mut actions_data = ActionsData::new();
            actions_data.fetch_last_block_heights(&db).await;
            let min_block_height = actions_data.min_restart_block();
            tracing::log::info!(target: PROJECT_ID, "Min block height: {}", min_block_height);

            let start_block_height = first_block_height.max(min_block_height + 1);
            let (sender, receiver) = mpsc::channel(100);
            let config = fetcher::FetcherConfig {
                num_threads,
                start_block_height,
                chain_id,
            };
            tokio::spawn(fetcher::start_fetcher(
                Some(client),
                config,
                sender,
                is_running,
            ));
            listen_blocks_for_actions(receiver, db, actions_data).await;
        }
        "transactions" => {
            let mut transactions_data = TransactionsData::new();
            let last_block_height = transactions_data.last_block_height(&db).await;
            let is_cache_ready = transactions_data.is_cache_ready(last_block_height);
            tracing::log::info!(target: PROJECT_ID, "Last block height: {}. Cache is ready: {}", last_block_height, is_cache_ready);

            let start_block_height = if is_cache_ready {
                last_block_height + 1
            } else {
                last_block_height.saturating_sub(SAFE_CATCH_UP_OFFSET)
            };

            let start_block_height = first_block_height.max(start_block_height);
            let (sender, receiver) = mpsc::channel(100);
            let config = fetcher::FetcherConfig {
                num_threads,
                start_block_height,
                chain_id,
            };
            tokio::spawn(fetcher::start_fetcher(
                Some(client),
                config,
                sender,
                is_running,
            ));
            listen_blocks_for_transactions(receiver, db, transactions_data, last_block_height)
                .await;
        }
        _ => {
            panic!("Unknown command");
        }
    };

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}

async fn listen_blocks_for_actions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    db: PostgresDB,
    mut actions_data: ActionsData,
) {
    while let Some(block) = stream.recv().await {
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block.block.header.height);
        actions_data.process_block(&db, block).await.unwrap();
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    actions_data.commit(&db).await.unwrap();
}

async fn listen_blocks_for_transactions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    db: PostgresDB,
    mut transactions_data: TransactionsData,
    last_block_height: u64,
) {
    transactions_data.set_watch_list(
        db
          .get_watch_list()
          .await
          .unwrap_or(vec![])
          .into_iter()
          .map(|e| e.into())
          .collect()
    );

    while let Some(block) = stream.recv().await {
        let block_height = block.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        transactions_data
            .process_block(&db, block, last_block_height)
            .await
            .unwrap();
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transactions_data.commit(&db).await.unwrap();
    transactions_data.flush().await.unwrap();
}
