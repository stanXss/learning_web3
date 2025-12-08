
--volume of USDC transfers
SELECT
  date_trunc('day', block_time) AS day,
  SUM(amount_raw) AS volume_usdc -- adjust decimals
FROM tokens_ethereum.transfers
WHERE contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
and block_month >= 	date '2025-10-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

--top senders of USDC
SELECT
  "from" AS sender,
  COUNT(*) AS tx_count,
  SUM(amount_raw) AS total_sent
FROM tokens_ethereum.transfers
WHERE contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
  AND block_time >= date '2025-11-24'
  and block_month >= 	date '2025-11-01'
GROUP BY 1
ORDER BY total_sent DESC
LIMIT 20;

--top senders of USDC with labels
SELECT
  tr."from" AS sender,
  tr.blockchain,
  l.account_owner,
  l.contract_name,
  COUNT(*) AS tx_count,
  SUM(tr.amount_raw) AS total_sent
FROM tokens_ethereum.transfers tr
join labels.owner_addresses l
on tr."from" = l.address
WHERE tr.contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
  AND tr.block_time >= date '2025-11-25'
  and tr.block_month >= 	date '2025-11-01'
  and l.blockchain = 'ethereum'
GROUP BY 1, 2, 3, 4
ORDER BY total_sent DESC
LIMIT 30;

--number of addresses sending USDC
SELECT
  date_trunc('day', block_time) AS day,
  COUNT(DISTINCT "from") AS daily_active_senders
FROM tokens.transfers
WHERE contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
and block_month >= 	date '2025-10-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

--new addresses sending USDC
WITH first_seen AS (
  SELECT
    "from" AS addr,
    MIN(date_trunc('day', block_time)) AS first_day
  FROM tokens_ethereum.transfers
  WHERE contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
  GROUP BY 1
)
SELECT
  fs.first_day,
  COUNT(*) AS new_senders_that_day
FROM first_seen fs
WHERE fs.first_day >= 	date '2025-11-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

--uniswap trades USDC-WETH for last 30 days
SELECT
  date_trunc('day', block_time) AS day,
  SUM(amount_usd) AS volume_usd
FROM dex.trades dt
WHERE 1=1 --pool_address = '<POOL_ADDRESS>' -- or token pair filter
and blockchain = 'ethereum'
and project = 'uniswap'
  AND block_time >= current_date - interval '7' day
  and block_month >= current_date - interval '2' month
and token_pair in ('USDC-WETH', 'WETH-USDC')
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

--uniswap top traders by day
SELECT
  tx_from,
  l.name,
  COUNT(*) AS tx_count,
  SUM(amount_usd) AS volume_usd
FROM dex.trades dt
join labels.ens l
on dt.tx_from = l.address
WHERE token_pair in ('USDC-WETH', 'WETH-USDC')
  AND block_time >= current_date - interval '7' day
  and block_month >= current_date - interval '2' month
GROUP BY 1,2
ORDER BY volume_usd DESC
LIMIT 20;