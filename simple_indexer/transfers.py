from web3 import Web3
import json
import psycopg2
from psycopg2.extras import execute_values
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from shared_funcs import hex_to_bytes20, hex_to_bytes32, parse_block_number, throttle

load_dotenv()
RPC_URL = os.environ["RPC_URL"]
CONN_LINE = os.environ["CONN_LINE"]

ERC20_TRANSFER_ABI = json.loads("""
[
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "address", "name": "from", "type": "address" },
      { "indexed": true,  "internalType": "address", "name": "to",   "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "value","type": "uint256" }
    ],
    "name": "Transfer",
    "type": "event"
  }
]
""")

TOKEN = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")  # USDC address
DECIMALS = 6  # decimals for USDC
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()
w3 = Web3(Web3.HTTPProvider(RPC_URL))  # web3 RPC service
conn = psycopg2.connect(CONN_LINE)  # init db connection

token_contract = w3.eth.contract(address=TOKEN, abi=ERC20_TRANSFER_ABI)


def proc_main():

    last_block = get_max_loaded_block()

    current_block = w3.eth.block_number

    if last_block is None:  # only take several blocks because of rpc limit
        last_block = current_block - 100
    else:
        if current_block - last_block > 100:
            last_block = current_block - 100

    print(last_block)
    print(current_block)

    all_logs = []

    run_block = last_block

    LAST_CALL = 0

    while current_block >= run_block:

        try:
            logs = w3.eth.get_logs({
                "fromBlock": run_block,
                "toBlock": run_block + 9,
                "address": TOKEN,
                "topics": [TRANSFER_TOPIC]
            })

            all_logs.extend(logs)

            LAST_CALL = throttle(0.1, LAST_CALL)

        except Exception as e:
            if "429" in str(e):
                LAST_CALL = throttle(2.0, LAST_CALL)  # hard backoff
                continue
            else:
                raise

        run_block = run_block + 9

    block_numbers = list(set([parse_block_number(log["blockNumber"]) for log in all_logs]))
    blk_times = get_block_timestamps_bulk(sorted(block_numbers))

    logs_out = []
    for log in all_logs:
        log_out = decode_transfer_log(log, blk_times)
        logs_out.append(make_row(log_out, decimals=DECIMALS))

    print(blk_times)

    bulk_insert_transfers(logs_out)

    load_block_numbers(blk_times)

    load_block_load(blk_times)


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


def load_block_load(blk_times): # load block data into db

    with conn.cursor() as cur:

        for key, value in blk_times.items():
            cur.execute(
                """
                INSERT INTO eth_main.block_load (
                    block_number, load_table
                )
                VALUES (%s,%s)
                ON CONFLICT (block_number, load_table) DO NOTHING;
                """,
                (
                    key,
                    'erc20_transfers',
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
    INSERT INTO eth_main.erc20_transfers (
      blockchain, token_address, tx_hash, log_index,
      block_number, block_time, from_address, to_address,
      amount_raw, amount_decimals
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
    from eth_main.block_load bc
    where load_table = 'erc20_transfers'
    """

    with conn.cursor() as cur:
        cur.execute(SQL_GET_MAX_BLOCK)
        res = cur.fetchone()
        if res is None:
            return None
        else:
            return res[0]


def make_row(decoded, decimals: int) -> tuple:  # prepare row for db insert
    # decoded: dict with from_address, to_address, amount_raw, tx_hash, log_index, block_number, block_time (unix int)
    return (
        "ethereum",
        hex_to_bytes20(decoded["token_address"]),
        hex_to_bytes32(decoded["tx_hash"]),     # ensure tx_hash includes 0x or not, helper handles both
        int(decoded["log_index"]),
        int(decoded["block_number"]),
        datetime.fromtimestamp(int(decoded["block_time"]), tz=timezone.utc),
        hex_to_bytes20(decoded["from_address"]),
        hex_to_bytes20(decoded["to_address"]),
        int(decoded["amount_raw"]),
        int(decimals),
    )


def decode_transfer_log(log, blk_times):  # decode logs with ABI

    decoded = token_contract.events.Transfer().process_log(log)
    args = decoded["args"]

    return {
        "token_address": TOKEN,
        "from_address": args["from"],
        "to_address": args["to"],
        "amount_raw": int(args["value"]),
        "tx_hash": decoded["transactionHash"].hex(),
        "log_index": decoded["logIndex"],
        "block_number": decoded["blockNumber"],
        "block_time": blk_times[decoded["blockNumber"]]
    }


if __name__ == '__main__':
    proc_main()

