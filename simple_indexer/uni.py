from web3 import Web3
import json
import psycopg2
from psycopg2.extras import execute_values
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from shared_funcs import hex_to_bytes20, hex_to_bytes32, parse_block_number


load_dotenv()
RPC_URL = os.environ["RPC_URL"]
CONN_LINE = os.environ["CONN_LINE"]

UNISWAP_V3_SWAP_ABI = [
  {
    "anonymous": False,
    "inputs": [
      {"indexed": True,  "name": "sender",       "type": "address"},
      {"indexed": True,  "name": "recipient",    "type": "address"},
      {"indexed": False, "name": "amount0",      "type": "int256"},
      {"indexed": False, "name": "amount1",      "type": "int256"},
      {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
      {"indexed": False, "name": "liquidity",    "type": "uint128"},
      {"indexed": False, "name": "tick",         "type": "int24"}
    ],
    "name": "Swap",
    "type": "event"
  }
]

# POOL_ADDRESS = Web3.to_checksum_address('0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36')  # ETH/USDT v3 0.3% pool
POOL_ADDRESS = Web3.to_checksum_address('0x8ad599c3a055b0d53d8045f47c405cc1d17674f9')  # ETH/USDC v3 0.3% pool
SWAP_TOPIC = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
w3 = Web3(Web3.HTTPProvider(RPC_URL))  # web3 RPC service
conn = psycopg2.connect(CONN_LINE)  # init db connection

pool_contract = w3.eth.contract(address=POOL_ADDRESS, abi=UNISWAP_V3_SWAP_ABI)


def proc_main():

    last_block = get_max_loaded_block()

    current_block = w3.eth.block_number

    if last_block is None:  # only take several blocks because of rpc limit
        last_block = current_block - 1000
    else:
        if current_block - last_block > 9:
            last_block = current_block - 9

    print(last_block)
    print(current_block)

    logs = w3.eth.get_logs({
        "fromBlock": last_block,
        "toBlock":   current_block,
        "address":   POOL_ADDRESS,
        "topics":    [SWAP_TOPIC]
    })

    block_numbers = list(set([parse_block_number(log["blockNumber"]) for log in logs]))
    blk_times = get_block_timestamps_bulk(sorted(block_numbers))

    logs_out = []
    for log in logs:
        log_out = decode_swap_log(log, blk_times)

        logs_out.append(make_row(log_out))

    print(logs_out)

    print(blk_times)

    bulk_insert_transfers(logs_out)

    load_block_numbers(blk_times)


def load_block_numbers(blk_times): # load block data into db

    with conn.cursor() as cur:

        for key, value in blk_times.items():
            cur.execute(
                """
                INSERT INTO eth_main.block_cache (
                    block_number, block_time
                )
                VALUES (%s,to_timestamp(%s))
                ON CONFLICT (block_number) DO NOTHING;
                """,
                (
                    key,
                    value,
                )
            )

    conn.commit()


def bulk_insert_transfers(rows, page_size=5000):  # load txs into db
    cur = conn.cursor()

    """
    rows: list of tuples in the exact column order above.
    Use page_size 1k–10k. Start with 5k; tune later.
    """

    SQL_INSERT = """
    INSERT INTO eth_main.uni_trades (
    pool_address, tx_hash, log_index, block_number,
    block_time, sender, recipient, amount0_raw, amount1_raw, sqrtPriceX96, liquidity, tick
    ) VALUES %s
    ON CONFLICT (tx_hash, log_index) DO NOTHING;
    """

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, SQL_INSERT, rows, page_size=page_size)
    conn.commit()
    return len(rows)


def get_block_timestamps_bulk(block_numbers, batch_size=100):  # get block timestamps for blocks
    result = {}

    for i in range(0, len(block_numbers), batch_size):
        batch = block_numbers[i:i+batch_size]

        payload = [{
            "jsonrpc": "2.0",
            "id": bn,
            "method": "eth_getBlockByNumber",
            "params": [hex(bn), False]  # False = header only (no tx objects)
        } for bn in batch]

        r = requests.post(RPC_URL, json=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()

        # Alchemy returns a list of responses
        for item in resp:
            if "result" not in item or item["result"] is None:
                continue
            bn = int(item["result"]["number"], 16)
            ts = int(item["result"]["timestamp"], 16)
            result[bn] = ts

    return result


def get_max_loaded_block():  # get max block number already loaded into db
    SQL_GET_MAX_BLOCK = """
    select
    max(block_number) block
    from eth_main.block_cache bc
    """

    with conn.cursor() as cur:
        cur.execute(SQL_GET_MAX_BLOCK)
        last_block = int(cur.fetchone()[0])
    return last_block


def make_row(decoded) -> tuple:  # prepare row for db insert
    # decoded: dict with from_address, to_address, amount_raw, tx_hash, log_index, block_number, block_time (unix int)

    return (
        hex_to_bytes20(decoded["pool"]),
        hex_to_bytes32(decoded["tx_hash"]),
        int(decoded["log_index"]),
        int(decoded["block_number"]),
        datetime.fromtimestamp(int(decoded["block_time"]), tz=timezone.utc),
        hex_to_bytes20(decoded["sender"]),
        hex_to_bytes20(decoded["recipient"]),
        int(decoded["amount0_raw"]),
        int(decoded["amount1_raw"]),
        int(decoded["sqrtPriceX96"]),
        int(decoded["liquidity"]),
        int(decoded["tick"]),
    )


def decode_swap_log(log, blk_times):  # decode logs with ABI

    decoded = pool_contract.events.Swap().process_log(log)
    a = decoded["args"]
    return {
        "pool": POOL_ADDRESS,
        "tx_hash": decoded["transactionHash"].hex(),
        "log_index": decoded["logIndex"],
        "block_number": decoded["blockNumber"],
        "sender": a["sender"],
        "recipient": a["recipient"],
        "amount0_raw": int(a["amount0"]),  # signed
        "amount1_raw": int(a["amount1"]),  # signed
        "sqrtPriceX96": int(a["sqrtPriceX96"]),
        "liquidity": int(a["liquidity"]),
        "tick": int(a["tick"]),
        "block_time": blk_times[decoded["blockNumber"]],
    }

if __name__ == '__main__':
    proc_main()

