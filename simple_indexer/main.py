from web3 import Web3
import json
import psycopg2
from psycopg2.extras import execute_values
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone


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

TOKEN = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
DECIMALS = 6
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()
w3 = Web3(Web3.HTTPProvider(RPC_URL))


def proc_main():

    current_block = w3.eth.block_number
    print(current_block)

    logs = w3.eth.get_logs({
        "fromBlock": current_block - 1,
        "toBlock":   current_block,
        "address":   TOKEN,
        "topics":    [TRANSFER_TOPIC]
    })

    block_numbers = list(set([parse_block_number(log["blockNumber"]) for log in logs]))
    blk_times = get_block_timestamps_bulk(sorted(block_numbers))

    logs_out = []
    for log in logs:
        log_out = decode_transfer_log(log, blk_times)
        logs_out.append(make_row(log_out, decimals=6))

    print(blk_times)

    conn = psycopg2.connect(CONN_LINE)
    cur = conn.cursor()

    SQL_INSERT = """
    INSERT INTO eth_main.erc20_transfers (
      blockchain, token_address, tx_hash, log_index,
      block_number, block_time, from_address, to_address,
      amount_raw, amount_decimals
    ) VALUES %s
    ON CONFLICT (block_number, tx_hash, log_index) DO NOTHING;
    """

    bulk_insert_transfers(conn, logs_out, SQL_INSERT)

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


def bulk_insert_transfers(conn, rows, sql, page_size=5000):
    """
    rows: list of tuples in the exact column order above.
    Use page_size 1k–10k. Start with 5k; tune later.
    """
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
    conn.commit()
    return len(rows)


def get_block_timestamps_bulk(block_numbers, batch_size=100):
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

        out = {}

        # Alchemy returns a list of responses
        for item in resp:
            if "result" not in item or item["result"] is None:
                continue
            bn = int(item["result"]["number"], 16)
            ts = int(item["result"]["timestamp"], 16)
            out[bn] = ts

        return out


def make_row(decoded, decimals: int) -> tuple:
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


def decode_transfer_log(log, blk_times):

    token_contract = w3.eth.contract(address=TOKEN, abi=ERC20_TRANSFER_ABI)

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


def hex_to_bytes20(addr: str) -> bytes:
    # expects '0x' + 40 hex
    a = addr.lower()
    if a.startswith("0x"):
        a = a[2:]
    if len(a) != 40:
        raise ValueError(f"Bad address length: {addr}")
    return bytes.fromhex(a)


def hex_to_bytes32(h: str) -> bytes:
    # expects '0x' + 64 hex, or sometimes without 0x
    x = h.lower()
    if x.startswith("0x"):
        x = x[2:]
    if len(x) != 64:
        raise ValueError(f"Bad hash length: {h}")
    return bytes.fromhex(x)


def parse_block_number(x):
    if isinstance(x, int):
        return x
    return int(x, 16)

if __name__ == '__main__':
    proc_main()

