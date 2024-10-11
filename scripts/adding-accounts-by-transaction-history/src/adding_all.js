// This script adds all other accounts from transactions (except Heroes transactions) to the account_txs table

require('dotenv').config();
const { createClient } = require('@clickhouse/client');
const axios = require('axios');
const { Decimal } = require('decimal.js');

const CLICK_HOUSE_URL = process.env.CLICK_HOUSE_URL || "http://localhost:8123";
const CLICK_HOUSE_DB = process.env.CLICK_HOUSE_DB || "database";
const CLICK_HOUSE_USER = process.env.CLICK_HOUSE_USER || "username";
const CLICK_HOUSE_PASSWORD = process.env.CLICK_HOUSE_PASSWORD || "secret";
const BLOCK_HEIGHT_FROM = process.env.BLOCK_HEIGHT_FROM || 0;
const BLOCK_HEIGHT_TO = process.env.BLOCK_HEIGHT_TO || 0;
const HEROES_API_URL = process.env.HEROES_API_URL || "http://localhost:8080/api";
const HEROES_BOUNTIES_CONTRACT_ID = process.env.HEROES_BOUNTIES_CONTRACT_ID || "bounties.heroes.near";

const POTENTIAL_ACCOUNT_ARGS = ["receiver_id", "account_id", "sender_id", "new_account_id",
  "predecessor_account_id", "contract_id", "owner_id", "token_owner_id", "nft_contract_id",
  "token_account_id", "creator_id", "referral_id", "previous_owner_id", "seller_id", "buyer_id",
  "user_id", "beneficiary_id", "staking_pool_account_id", "owner_account_id", "voting_account_id"];

function initClickHouse() {
  return createClient({
    url: CLICK_HOUSE_URL,
    username: CLICK_HOUSE_USER,
    password: CLICK_HOUSE_PASSWORD,
    database: CLICK_HOUSE_DB,
  });
}

async function getTokens() {
  const { data } = await axios(`${HEROES_API_URL}/conf/tokens`);
  return (data || []).map(t => t.tokenId);
}

function parseTransaction(resultRow) {
  let transaction = null;
  try {
    transaction = JSON.parse(resultRow.transaction);
  } catch (error) {
    console.error(`Transaction parsing error: ${error.message}`);
    return null;
  }

  if (transaction?.transaction?.hash) {
    const accounts = [];

    const mainActions = transaction.transaction.actions?.filter(
      a => !!a.FunctionCall?.method_name
    ) || [];

    if (mainActions.length > 0) {
      transaction.receipts?.filter(
        r => r.receipt?.predecessor_id !== 'system' &&
          // eslint-disable-next-line no-prototype-builtins
          (r.execution_outcome?.outcome?.status?.hasOwnProperty("SuccessValue") ||
            // eslint-disable-next-line no-prototype-builtins
            r.execution_outcome?.outcome?.status?.hasOwnProperty("SuccessReceiptId"))
      ).forEach(r => {
        r.receipt?.receipt?.Action?.actions?.forEach(action => {
          const methodName = action?.FunctionCall?.method_name;

          let args;
          if (/^\[.+]$/.test(action?.FunctionCall?.args)) {
            args = String.fromCharCode.apply(null, JSON.parse(action.FunctionCall.args));
          } else {
            args = action?.FunctionCall?.args ?
              Buffer.from(action.FunctionCall.args, "base64").toString("utf-8")
              : "{}";
          }

          if (methodName) {
            accounts.push(r.receipt.predecessor_id);
            accounts.push(r.receipt.receiver_id);
            const argsObj = JSON.parse(args);

            for (const key in argsObj) {
              if (!argsObj.hasOwnProperty(key)) {
                continue;
              }
              const value = argsObj[key];
              if (POTENTIAL_ACCOUNT_ARGS.includes(key) && value && typeof value === "string") {
                accounts.push(value);
              }
            }
          }
        });
      });
    }

    return accounts;
  }

  return null;
}

async function createTempAccountsTable(client) {
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS account_txs2
      (
          account_id         String COMMENT 'The account ID',
          transaction_hash   String COMMENT 'The transaction hash',
          signer_id          String COMMENT 'The account ID of the transaction signer',
          tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
          tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
          INDEX              tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1
      ) ENGINE = ReplacingMergeTree
      PRIMARY KEY (account_id, tx_block_height)
      ORDER BY (account_id, tx_block_height, transaction_hash)
    `,
  });
}

async function getBlockHeightOfLatestData(client) {
  const resultSet = await client.query({
    query: `
    select max(tx_block_height) as lastBlockHeight
    from account_txs2`,
    format: 'JSONEachRow',
  });

  const lastBlockHeight = await resultSet.json();
  return Number(lastBlockHeight?.[0]?.["lastBlockHeight"]) || null;
}

async function getTimestampOfLatestData(client) {
  const resultSet = await client.query({
    query: `
    select max(toUInt64(toFloat64(tx_block_timestamp) * 1000000000)) as last_block_timestamp
    from account_txs2`,
    format: 'JSONEachRow',
  });

  const lastBlockTimestamp = await resultSet.json();
  return new Decimal(lastBlockTimestamp?.[0]?.["last_block_timestamp"] || 0);
}

async function clearIncompleteData(client, blockHeight) {
  await client.command({
    query: "delete from account_txs2 where tx_block_height = {blockHeight: UInt64}",
    query_params: { blockHeight: Number(blockHeight) },
  });
}

async function getTransactions(client, blockHeightFrom, blockHeightTo, limit, offset) {
  const query =
    `select
       t.transaction_hash,
       t.tx_block_timestamp as block_date,
       toUInt64(toFloat64(t.tx_block_timestamp) * 1000000000) as block_timestamp,
       t.tx_block_height as block_height,
       t.transaction,
       JSON_VALUE(t.transaction, '$.transaction.receiver_id') as receiver_id,
       JSON_VALUE(t.transaction, '$.transaction.signer_id') as signer_id
     from transactions as t
     left join account_txs as at on at.transaction_hash = t.transaction_hash
     where tx_block_height >= {startBlock: UInt64}
       and tx_block_height <= {endBlock: UInt64}
       and at.transaction_hash = ''
     order by t.tx_block_timestamp
     limit {limit: UInt16}
     offset {offset: UInt16}`;

  const resultSet = await client.query({
    query,
    format: 'JSONEachRow',
    query_params: {
      startBlock: Number(blockHeightFrom),
      endBlock: Number(blockHeightTo),
      limit,
      offset,
    },
  });

  return resultSet.json();
}

async function appendRecordsToMainAccountsTable(client) {
  await client.command({
    query: `
      insert into account_txs (account_id, transaction_hash, signer_id, tx_block_height, tx_block_timestamp)
      select account_id, transaction_hash, signer_id, tx_block_height, tx_block_timestamp
      from account_txs2
      left join account_txs a1 on a1.transaction_hash = transaction_hash and a1.account_id = account_id
      where a1.transaction_hash = ''
      order by tx_block_height;
    `,
  });
}

async function removeTempAccountsTable(client) {
  await client.command({ query: `drop table account_txs2` });
}

async function buildAccountsTable(client, blockHeightFrom, blockHeightTo) {
  let lastBlockHeight = await getBlockHeightOfLatestData(client);
  console.log(`lastBlockHeight: ${lastBlockHeight}`);

  if (lastBlockHeight) {
    await clearIncompleteData(client, lastBlockHeight);
  } else {
    lastBlockHeight = blockHeightFrom;
  }

  let doNotIncludeAccounts = [HEROES_BOUNTIES_CONTRACT_ID, ...(await getTokens())];
  const limit = 500;
  let offset = 0;
  let i = 1;

  let dataset = await getTransactions(client, lastBlockHeight, blockHeightTo, limit, offset);
  let lastBlockTimestamp = await getTimestampOfLatestData(client);

  while (dataset.length > 0) {
    console.log(`Fetched: ${dataset.length}, offset: ${offset}`);

    if (
      lastBlockTimestamp.greaterThan(
        new Decimal(dataset[dataset.length - 1].block_timestamp)
      )
    ) {
      throw new Error("Violation of the sequence of timestamps");
    }

    for (const tx of dataset) {
      const accounts = [tx.receiver_id, tx.signer_id];
      const additionalAccounts = parseTransaction(tx);

      if (additionalAccounts?.length > 0) {
        Array.prototype.push.apply(accounts, additionalAccounts);
      }

      const txAccounts = accounts.filter(
        (value, index, array) => array.indexOf(value) === index
      );
      if (txAccounts.length > 0 && !txAccounts.find(a => doNotIncludeAccounts.includes(a))) {
        for (const account of txAccounts) {
          await client.insert({
            table: 'account_txs2',
            values: [{
              account_id: account,
              transaction_hash: tx.transaction_hash,
              signer_id: tx.signer_id,
              tx_block_height: tx.block_height,
              tx_block_timestamp: tx.block_date,
            }],
            format: 'JSONEachRow',
          });
        }

        // console.log(`Record ${i} of ${dataset.length}`);
      }

      i++;
    }

    offset += dataset.length;
    lastBlockTimestamp = new Decimal(dataset[dataset.length - 1].block_timestamp);
    lastBlockHeight = dataset[dataset.length - 1].block_height;
    console.log(`lastBlockHeight: ${lastBlockHeight}`);

    dataset = await getTransactions(client, lastBlockHeight, blockHeightTo, limit, offset);
  }
}

async function main() {
  let keepRunning = true

  while (keepRunning) {
    try {
      const client = initClickHouse();
      await createTempAccountsTable(client);
      await buildAccountsTable(client, BLOCK_HEIGHT_FROM, BLOCK_HEIGHT_TO);
      await appendRecordsToMainAccountsTable(client);
      await removeTempAccountsTable(client);

      keepRunning = false;

    } catch (err) {
      console.error(err);
      console.log(`Restarting the process after an error...`);
    }
  }
}

main()
  .then(console.log)
  .catch(console.error);
