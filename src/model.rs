use tokio_postgres::{Client, connect, Error, NoTls};
use std::env;
use fastnear_primitives::near_primitives::types::BlockHeight;
use std::time::Duration;
use crate::common::Row;
use crate::transactions::{AccountTxRow, BlockTxRow, ReceiptTxRow, TransactionRow};

pub const POSTGRES_TARGET: &str = "postgres";
pub const SAVE_STEP: u64 = 1000;

pub struct PostgresDB {
  pub client: Client,
  pub min_batch: usize,
}

impl PostgresDB {
  pub async fn new(min_batch: usize) -> Self {
    Self {
      client: Self::establish_connection().await.expect("Failed to connect to Postgres"),
      min_batch,
    }
  }

  pub async fn max(&self, column: &str, table: &str) -> Result<BlockHeight, Error> {
    let row = self
      .client
      .query_one(&format!("SELECT max({}) as max FROM {}", column, table), &[])
      .await?;
    let block_height: i64 = row.get("max");
    Ok(block_height as u64)
  }

  pub async fn get_watch_list(&self) -> Result<Vec<(String, bool)>, Error> {
    let result = self
      .client
      .query("SELECT account_id, is_regex FROM watch_list", &[])
      .await?
      .into_iter()
      .map(|r| (r.get("account_id"), r.get("is_regex")))
      .collect();
    Ok(result)
  }

  async fn establish_connection() -> Result<Client, Error> {
    let (client, connection) = connect(env::var("DATABASE_URL").unwrap().as_str(), NoTls).await?;

    tokio::spawn(async move {
      if let Err(e) = connection.await {
        eprintln!("Connection error: {}", e);
      }
    });

    Ok(client)
  }

  async fn insert_transaction(&self, row: &TransactionRow) -> Result<(), Error> {
    self.client.execute(
      "insert into transactions (\
      transaction_hash, signer_id, tx_block_height, tx_block_hash,\
      tx_block_timestamp, transaction, last_block_height\
    ) values ($1, $2, $3, $4, $5, $6, $7)",
      &[
        &row.transaction_hash,
        &row.signer_id,
        &(row.tx_block_height as i64),
        &row.tx_block_hash,
        &(row.tx_block_timestamp as i64),
        &row.transaction,
        &(row.last_block_height as i64)
      ]
    ).await?;
    Ok(())
  }

  async fn insert_account(&self, row: &AccountTxRow) -> Result<(), Error> {
    self.client.execute(
      "insert into account_txs (\
      account_id, transaction_hash, signer_id, tx_block_height, tx_block_timestamp\
    ) values ($1, $2, $3, $4, $5)",
      &[
        &row.account_id,
        &row.transaction_hash,
        &row.signer_id,
        &(row.tx_block_height as i64),
        &(row.tx_block_timestamp as i64)
      ]
    ).await?;
    Ok(())
  }

  async fn insert_block(&self, row: &BlockTxRow) -> Result<(), Error> {
    self.client.execute(
      "insert into block_txs (\
      block_height, block_hash, block_timestamp, transaction_hash, signer_id, tx_block_height\
    ) values ($1, $2, $3, $4, $5, $6)",
      &[
        &(row.block_height as i64),
        &row.block_hash,
        &(row.block_timestamp as i64),
        &row.transaction_hash,
        &row.signer_id,
        &(row.tx_block_height as i64)
      ]
    ).await?;
    Ok(())
  }

  async fn insert_receipt(&self, row: &ReceiptTxRow) -> Result<(), Error> {
    self.client.execute(
      "insert into receipt_txs (\
      receipt_id, transaction_hash, signer_id, tx_block_height, tx_block_timestamp\
    ) values ($1, $2, $3, $4, $5)",
      &[
        &row.receipt_id,
        &row.transaction_hash,
        &row.signer_id,
        &(row.tx_block_height as i64),
        &(row.tx_block_timestamp as i64)
      ]
    ).await?;
    Ok(())
  }

  pub async fn insert_rows_with_retry(
    &self,
    rows: &Vec<Row>,
    table: &str,
  ) -> Result<(), Error>
  {
    let mut delay = Duration::from_millis(100);
    let max_retries = 10;
    let mut i = 0;

    loop {
      let res = || async {
        if env::var("POSTGRES_SKIP_COMMIT") != Ok("true".to_string()) {
          for row in rows {
            match row {
              Row::TransactionRow(row) => self.insert_transaction(row).await?,
              Row::AccountTxRow(row) => self.insert_account(row).await?,
              Row::BlockTxRow(row) => self.insert_block(row).await?,
              Row::ReceiptTxRow(row) => self.insert_receipt(row).await?,
              _ => ()
            }
          }
        }
        Ok(())
      };
      match res().await {
        Ok(v) => break Ok(v),
        Err(err) => {
          let db_error = Error::as_db_error(&err);
          let constraint_violated = db_error.is_some() && db_error
            .unwrap()
            .to_string()
            .contains("duplicate key value violates unique constraint");

          if constraint_violated {
            tracing::log::warn!(target: POSTGRES_TARGET, "This entry already exists: {}", err);
            break Ok(());
          }

          tracing::log::error!(target: POSTGRES_TARGET, "Attempt #{}: Error inserting rows into \"{}\": {}", i, table, err);
          tokio::time::sleep(delay).await;
          delay *= 2;
          if i == max_retries - 1 {
            break Err(err);
          }
        }
      };
      i += 1;
    }
  }
}
