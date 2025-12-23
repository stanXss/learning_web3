--test queries to compare the indexer result with Dune

--checking the USDC transfers for a given block
select * from tokens_ethereum.transfers
where block_number = 24060957
and contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
and blockchain = 'ethereum'