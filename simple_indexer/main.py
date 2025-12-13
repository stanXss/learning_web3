from web3 import Web3
import json
import psycopg2
import requests

RPC_URL = 'https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY'
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

    print('Hello world!')  # Press Ctrl+F8 to toggle the breakpoint.

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
        logs_out.append(log_out)

    print(blk_times)

    conn = psycopg2.connect("dbname=your_db user=your_user password=your_pass")
    cur = conn.cursor()
    for r in logs_out:
        cur.execute(
            """
            INSERT INTO eth_main.erc20_transfers (
                blockchain, token_address, tx_hash, log_index,
                block_number, block_time, from_address, to_address,
                amount_raw, amount_decimals
            )
            VALUES (%s,%s,%s,%s,%s,to_timestamp(%s),%s,%s,%s,%s)
            ON CONFLICT (block_number, tx_hash, log_index) DO NOTHING;
            """,
            (
                "ethereum",
                bytes.fromhex(r["token_address"][2:]),
                bytes.fromhex(r["tx_hash"][2:]),
                r["log_index"],
                r["block_number"],
                r["block_time"],
                bytes.fromhex(r["from_address"][2:]),
                bytes.fromhex(r["to_address"][2:]),
                r["amount_raw"],
                6,  # USDC decimals
            )
        )
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

        #print(out)
        return out

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


def parse_block_number(x):
    if isinstance(x, int):
        return x
    return int(x, 16)

if __name__ == '__main__':
    proc_main()

