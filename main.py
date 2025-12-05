from web3 import Web3
import time
import psycopg2

RPC_URL = 'https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY'

def proc_main():

    print('Hello world!')  # Press Ctrl+F8 to toggle the breakpoint.

    conn = psycopg2.connect("dbname=your_db user=your_user password=your_pass")
    cur = conn.cursor()
    cur.execute("select 1")
    x = cur.fetchone()
    print(x)


    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    current_block = w3.eth.block_number
    print(current_block)


    #TOKEN = Web3.to_checksum_address("<USDC_OR_WHATEVER>")
    #DECIMALS = 6
    #TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

if __name__ == '__main__':
    proc_main()

