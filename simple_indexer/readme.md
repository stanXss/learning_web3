Web3 ERC-20 Indexer (Ethereum)

A minimal, production-style ERC-20 Transfer indexer for Ethereum mainnet.

This project connects directly to an Ethereum JSON-RPC node, fetches on-chain Transfer events, decodes them using ABI, and stores normalized data in PostgreSQL for analytics and downstream use.

Architecture overview

Ingestion flow:
- Fetch logs via eth_getLogs (address + topic filtered)
- Decode events using ERC-20 ABI
- Collect unique block numbers
- Batch-fetch block timestamps
- Normalize and bulk-insert into PostgreSQL

Data model highlights:
- One row per (tx_hash, log_index)
- No assumptions about “one transfer per transaction”
- Addresses and hashes stored as raw bytes (20 / 32 bytes)

Run file: main.py
Database entities: in folder sql/ddl/

Requirements
- Python 3.09
- PostgreSQL 14+
- Ethereum JSON-RPC endpoint

Python dependencies:
- web3
- psycopg2
- python-dotenv
- requests