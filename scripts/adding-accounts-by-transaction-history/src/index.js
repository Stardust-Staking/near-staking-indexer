require('dotenv').config();
const { createClient } = require('@clickhouse/client');
const axios = require('axios');

const CLICK_HOUSE_URL = process.env.CLICK_HOUSE_URL || "http://localhost:8123";
const CLICK_HOUSE_DB = process.env.CLICK_HOUSE_DB || "database";
const CLICK_HOUSE_USER = process.env.CLICK_HOUSE_USER || "username";
const CLICK_HOUSE_PASSWORD = process.env.CLICK_HOUSE_PASSWORD || "secret";
const BLOCK_HEIGHT_FROM = process.env.BLOCK_HEIGHT_FROM || 0;
const HEROES_API_URL = process.env.HEROES_API_URL || "http://localhost:8080/api";
const HEROES_BOUNTIES_CONTRACT_ID = process.env.HEROES_BOUNTIES_CONTRACT_ID || "bounties.heroes.near";

const BOUNTIES_METHODS = ["bounty_claim", "bounty_give_up", "bounty_done", "open_dispute",
  "accept_claimant", "decline_claimant", "ft_transfer_call", "bounty_cancel", "bounty_update",
  "bounty_approve", "bounty_reject", "bounty_approve_of_several", "bounty_finalize",
  "extend_claim_deadline", "bounty_create", "mark_as_paid", "confirm_payment", "start_competition",
  "withdraw", "bounty_action", "decision_on_claim"];
const POTENTIAL_ACCOUNT_ARGS = ["receiver_id", "account_id", "sender_id", "new_account_id",
  "predecessor_account_id", "contract_id", "owner_id", "token_owner_id", "nft_contract_id",
  "token_account_id", "creator_id", "referral_id", "previous_owner_id", "seller_id", "buyer_id",
  "user_id", "beneficiary_id", "staking_pool_account_id", "owner_account_id", "claimer",
  "bounty_owner"];

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
      a => BOUNTIES_METHODS.includes(a.FunctionCall?.method_name)
    ) || [];

    if (mainActions.length > 0) {
      transaction.receipts?.filter(
        r => r.receipt?.predecessor_id !== 'system'
      ).forEach(r => {
        const action = r.receipt?.receipt?.Action?.actions?.[0];
        const methodName = action?.FunctionCall?.method_name;

        let args;
        if (/^\[.+]$/.test(action?.FunctionCall?.args)) {
          args = String.fromCharCode.apply(null, JSON.parse(action.FunctionCall.args));
        } else {
          args = action?.FunctionCall?.args ?
            Buffer.from(action.FunctionCall.args, "base64").toString("utf-8")
            : "{}";
        }

        if (
          (r.execution_outcome?.outcome?.status?.hasOwnProperty("SuccessValue") ||
            r.execution_outcome?.outcome?.status?.hasOwnProperty("SuccessReceiptId")) &&
          methodName
        ) {
          accounts.push(r.receipt.predecessor_id);
          accounts.push(r.receipt.receiver_id);
          const argsObj = JSON.parse(args);
          for (const key in argsObj) {
            if (!argsObj.hasOwnProperty(key)) continue;

            const value = argsObj[key];
            if (POTENTIAL_ACCOUNT_ARGS.includes(key) && value && typeof value === "string") {
              accounts.push(value);
            } else if (methodName === "bounty_action" && key === "action") {
              if (value?.ClaimApproved?.receiver_id) {
                accounts.push(value.ClaimApproved.receiver_id);
              } else if (value?.ClaimRejected?.receiver_id) {
                accounts.push(value.ClaimRejected.receiver_id);
              } else if (value?.Finalize?.receiver_id) {
                accounts.push(value.Finalize.receiver_id);
              }
            } else if (
              methodName === "bounty_finalize" && key === "claimant" && value?.length === 2
            ) {
              accounts.push(value[0]);
            }
          }
        }
      });
    }

    return accounts;
  }

  return null;
}

async function getLastBlockHeight(client) {
  const query = `select max(tx_block_height) as max_block_height from transactions`;
  const resultSet = await client.query({ query, format: 'JSONEachRow' });
  const dataset = await resultSet.json();
  return dataset.length === 1 ? Number(dataset[0].max_block_height) : 0;
}

async function createTempAccountsTable(client) {
  await client.command({
    query: `
      CREATE TABLE account_txs2
      (
          account_id         String COMMENT 'The account ID',
          transaction_hash   String COMMENT 'The transaction hash',
          signer_id          String COMMENT 'The account ID of the transaction signer',
          tx_block_height    UInt64 COMMENT 'The block height when the transaction was included',
          tx_block_timestamp DateTime64(9, 'UTC') COMMENT 'The block timestamp in UTC when the transaction was included',
          INDEX              tx_block_timestamp_minmax_idx tx_block_timestamp TYPE minmax GRANULARITY 1,
      ) ENGINE = ReplacingMergeTree
      PRIMARY KEY (account_id, tx_block_height)
      ORDER BY (account_id, tx_block_height, transaction_hash)
    `,
  });
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

async function buildAccountsTable(client, blockHeightTo) {
  let contracts = [HEROES_BOUNTIES_CONTRACT_ID, ...(await getTokens())];

  const query =
    `select
       transaction_hash,
       tx_block_timestamp as block_date,
       tx_block_height as block_height,
       transaction,
       JSON_VALUE(transaction, '$.transaction.receiver_id') as receiver_id,
       JSON_VALUE(transaction, '$.transaction.signer_id') as signer_id
     from transactions
     where tx_block_height >= {startBlock: UInt64}
       and tx_block_height < {endBlock: UInt64}
       and JSON_VALUE(transaction, '$.transaction.receiver_id') in
           ('${ contracts.join("','") }')`;

  const resultSet = await client.query({
    query,
    format: 'JSONEachRow',
    query_params: {
      startBlock: Number(BLOCK_HEIGHT_FROM),
      endBlock: blockHeightTo,
    },
  });
  const dataset = await resultSet.json();

  let i = 1;

  for (const tx of dataset) {
    const accounts = [tx.receiver_id, tx.signer_id];
    const additionalAccounts = parseTransaction(tx);
    if (additionalAccounts?.length > 0) {
      Array.prototype.push.apply(accounts, additionalAccounts);
    }

    const txAccounts = accounts.filter(
      (value, index, array) => array.indexOf(value) === index
    );
    if (txAccounts.length > 0) {
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

      console.log(`Record ${i} of ${dataset.length}`);
    }

    i++;
  }
}

async function main() {
  try {
    const client = initClickHouse();

    const blockHeightTo = await getLastBlockHeight(client);
    await createTempAccountsTable(client);
    await buildAccountsTable(client, blockHeightTo);
    await appendRecordsToMainAccountsTable(client);
    await removeTempAccountsTable(client);

  } catch (err) {
    console.error(err);
  }
}

main()
  .then(console.log)
  .catch(console.error);
