CREATE TABLE eth_main.uni_trades (
  blockchain       text        NOT NULL,
  token_address    bytea       NOT NULL,
  tx_hash          bytea       NOT NULL,
  log_index        int         NOT NULL,
  block_number     bigint      NOT NULL,
  block_time       timestamptz NOT NULL,
  from_address     bytea       NOT NULL,
  to_address       bytea       NOT NULL,
  amount_raw       numeric     NOT NULL,
  amount_decimals  int         NOT NULL,
  PRIMARY KEY (tx_hash, log_index)
);