client.py


#!/usr/bin/env python3
"""
client.py
Uso:
  python client.py --host 127.0.0.1 --port 8001
Comandos interativos:
  login <username> <password>
  post <message text>
  read
  exit
"""
import asyncio
import json
import argparse

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.token = None

    async def send_recv(self, payload):
        reader, writer = await asyncio.open_connection(self.host, self.port)
        s = json.dumps(payload) + '\n'
        writer.write(s.encode())
        await writer.drain()
        data = await reader.readline()
        writer.close()
        await writer.wait_closed()
        if not data:
            return None
        return json.loads(data.decode())

    async def login(self, username, password):
        res = await self.send_recv({'type': 'login', 'username': username, 'password': password})
        if res and res.get('status') == 'ok':
            self.token = res.get('token')
            print("Login ok. Token:", self.token)
        else:
            print("Login failed:", res)

    async def post(self, text):
        if not self.token:
            print("You must login first")
            return
        res = await self.send_recv({'type': 'post', 'token': self.token, 'content': text})
        print("Response:", res)

    async def read(self):
        res = await self.send_recv({'type': 'read'})
        if res and res.get('status') == 'ok':
            msgs = res.get('messages', [])
            print(f"--- {len(msgs)} messages ---")
            for m in sorted(msgs, key=lambda x: (x['ts'], x['origin'], x['seq'])):
                ts = m.get('ts')
                print(f"[{m['origin']}-{m['seq']}] {m['user']} @ {ts:.3f}: {m['content']}")
        else:
            print("Read failed:", res)

async def interactive(host, port):
    c = Client(host, port)
    print("Client started. Commands: login <user> <pass> | post <text> | read | exit")
    while True:
        try:
            cmd = input('> ').strip()
        except EOFError:
            break
        if not cmd:
            continue
        parts = cmd.split(' ', 2)
        op = parts[0].lower()
        if op == 'login' and len(parts) >= 3:
            await c.login(parts[1], parts[2])
        elif op == 'post' and len(parts) >= 2:
            await c.post(parts[1] if len(parts)==2 else parts[1] + ('' if len(parts)<3 else ' ' + parts[2]))
        elif op == 'read':
            await c.read()
        elif op == 'exit':
            break
        else:
            print("Unknown/invalid command")
    print("Client exiting.")

if name == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, required=True)
    args = p.parse_args()
    asyncio.run(interactive(args.host, args.port))
