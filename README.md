## Clickhouse Provider based on FASTNEAR's indexed neardata xyz

### Create clickhouse table

For generic action view:

```sql
-- This is a PostgresSQL table.

-- TODO ...

```

### Clickhouse explorer tables

The explorer is transaction focused. Everything is bundled around transactions.

```sql
-- This is a PostgresSQL table.
create table if not exists transactions
(
  transaction_hash   text not null primary key ,
  signer_id          text not null,
  tx_block_height    bigint not null,
  tx_block_hash      text not null,
  tx_block_timestamp bigint not null,
  transaction        jsonb not null ,
  last_block_height  bigint not null
);

create index transactions_signer_id_idx on transactions (signer_id);
create index transactions_block_height_idx on transactions (tx_block_height);
create index transactions_block_timestamp_idx on transactions (tx_block_timestamp);

create table if not exists account_txs
(
  account_id         text not null,
  transaction_hash   text not null,
  signer_id          text not null,
  tx_block_height    bigint not null,
  tx_block_timestamp bigint not null,
  PRIMARY KEY (account_id, transaction_hash)
);

create index account_txs_block_height_idx on account_txs (tx_block_height);
create index account_txs_block_timestamp_idx on account_txs (tx_block_timestamp);
create index account_txs_transaction_hash_idx on account_txs (transaction_hash);

create table if not exists block_txs
(
  block_height     bigint not null,
  block_hash       text null,
  block_timestamp  bigint null,
  transaction_hash text null,
  signer_id        text null,
  tx_block_height  bigint null,
  PRIMARY KEY (block_height, transaction_hash)
);

create index block_txs_block_hash_idx on block_txs (block_hash);
create index block_txs_block_timestamp_idx on block_txs (block_timestamp);
create index block_txs_transaction_hash_idx on block_txs (transaction_hash);

create table if not exists receipt_txs
(
  receipt_id         text not null primary key,
  transaction_hash   text not null,
  signer_id          text not null,
  tx_block_height    bigint not null,
  tx_block_timestamp bigint not null
);

create index receipt_txs_block_height_idx on receipt_txs (tx_block_height);
create index receipt_txs_block_timestamp_idx on receipt_txs (tx_block_timestamp);
create index receipt_txs_transaction_hash_idx on receipt_txs (transaction_hash);

CREATE TABLE if not exists public.watch_list
(
  account_id         text not null primary key,
  is_regex           boolean not null
);
```
