#!/usr/bin/env python3
import asyncio
import json
import random
import time
import sys

async def send_request(host, port, txn, client_id, req_id):
    reader, writer = await asyncio.open_connection(host, port)
    msg = {
        'type': 11,  # MSG_CLIENT_REQUEST
        'client_id': client_id,
        'req_id': req_id,
        'txn': txn,
        'timestamp': time.time()
    }
    writer.write((json.dumps(msg) + '\n').encode())
    await writer.drain()
    # wait for reply
    try:
        data = await reader.readline()
        reply = json.loads(data.decode())
        print(f"Client {client_id} got reply: {reply}")
    except:
        print("No reply received")
    writer.close()

async def main():
    if len(sys.argv) < 3:
        print("Usage: client.py <primary_host> <primary_port>")
        sys.exit(1)
    host, port = sys.argv[1], int(sys.argv[2])
    client_id = random.randint(1, 1000)
    req_id = 0
    while True:
        txn = f"txn_{int(time.time())}_{req_id}"
        await send_request(host, port, txn, client_id, req_id)
        req_id += 1
        await asyncio.sleep(2)

if __name__ == '__main__':
    asyncio.run(main())