CREATE TABLE IF NOT EXISTS eth_main.uniswap_v3_swaps (
  pool_address   bytea       NOT NULL,
  tx_hash        bytea       NOT NULL,
  log_index      int         NOT NULL,
  block_number   bigint      NOT NULL,
  block_time     timestamptz NOT NULL,
  sender         bytea       NOT NULL,
  recipient      bytea       NOT NULL,
  amount0_raw    numeric     NOT NULL,  -- signed
  amount1_raw    numeric     NOT NULL,  -- signed
  sqrtPriceX96   numeric     NOT NULL,
  liquidity      numeric     NOT NULL,
  tick           int         NOT NULL,
  PRIMARY KEY (tx_hash, log_index)
);