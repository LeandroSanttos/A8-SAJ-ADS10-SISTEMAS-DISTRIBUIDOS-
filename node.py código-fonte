node.py


#!/usr/bin/env python3
"""
node.py
Nó do mural distribuído (asyncio TCP).
Uso:
  python node.py --id NODEA --host 127.0.0.1 --port 8001 --peers 127.0.0.1:8002,127.0.0.1:8003
"""
import asyncio
import json
import argparse
import time
from typing import Dict, List, Tuple

# ---------- Configuráveis ----------
REPLICATION_TIMEOUT = 3  # timeout para tentativas de replicação
# -----------------------------------

class Message:
    def __init__(self, origin: str, seq: int, user: str, content: str, ts: float = None):
        self.origin = origin      # id do nó que criou
        self.seq = seq            # sequência local do nó origin
        self.user = user
        self.content = content
        self.ts = ts if ts is not None else time.time()

    def id(self) -> Tuple[str, int]:
        return (self.origin, self.seq)

    def to_dict(self):
        return {
            'origin': self.origin,
            'seq': self.seq,
            'user': self.user,
            'content': self.content,
            'ts': self.ts
        }

    @staticmethod
    def from_dict(d):
        return Message(d['origin'], d['seq'], d['user'], d['content'], d.get('ts'))

class Node:
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        self.id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # list of "host:port"
        self.messages: List[Message] = []  # store Message
        self.next_seq = 1
        # simple user store: username -> password
        self.users: Dict[str, str] = {
            'alice': 'pass1',
            'bob': 'pass2',
            'carol': 'pass3'
        }
        self.tokens: Dict[str, str] = {}  # token -> username (simple)
        self.server = None
        self._lock = asyncio.Lock()

    async def start(self):
        self.server = await asyncio.start_server(self.handle_conn, self.host, self.port)
        addr = self.server.sockets[0].getsockname()
        print(f"[{self.id}] Listening on {addr}")
        # When starting, try to reconcile with peers
        asyncio.create_task(self.reconcile_on_start())

        async with self.server:
            await self.server.serve_forever()

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        try:
            data = await reader.readline()
            if not data:
                writer.close()
                await writer.wait_closed()
                return
            msg = json.loads(data.decode())
            typ = msg.get('type')
            # client posting -> requires auth token
            if typ == 'login':
                await self.handle_login(msg, writer)
            elif typ == 'post':
                await self.handle_client_post(msg, writer)
            elif typ == 'read':
                await self.handle_read(msg, writer)
            # replication messages between nodes
            elif typ == 'replicate':
                await self.handle_replication(msg, writer)
            elif typ == 'req_sync':
                await self.handle_req_sync(msg, writer)
            elif typ == 'send_missing':
                await self.handle_send_missing(msg, writer)
            else:
                await self.send_error(writer, f"unknown message type {typ}")
        except Exception as e:
            print(f"[{self.id}] Error handling conn {addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

    async def handle_login(self, msg, writer):
        username = msg.get('username')
        password = msg.get('password')
        if username in self.users and self.users[username] == password:
            token = f"token-{username}-{int(time.time())}"
            self.tokens[token] = username
            await self.send(writer, {'status': 'ok', 'token': token})
            print(f"[{self.id}] user {username} logged in (token {token})")
        else:
             await self.
          
          send(writer, {'status': 'error', 'reason': 'invalid credentials'})

    async def handle_client_post(self, msg, writer):
        token = msg.get('token')
        if token not in self.tokens:
            await self.send(writer, {'status': 'error', 'reason': 'not authenticated'})
            return
        user = self.tokens[token]
        content = msg.get('content', '')
        async with self._lock:
            seq = self.next_seq
            self.next_seq += 1
            m = Message(self.id, seq, user, content)
            self.messages.append(m)
        print(f"[{self.id}] New local message {m.id()} by {user}: {content!r}")
        # replicate asynchronously (fire-and-forget)
        asyncio.create_task(self.replicate_to_peers(m))
        await self.send(writer, {'status': 'ok', 'message': m.to_dict()})

    async def handle_read(self, msg, writer):
        # public read: send all messages as list
        msgs = [m.to_dict() for m in sorted(self.messages, key=lambda x: (x.ts, x.origin, x.seq))]
        await self.send(writer, {'status': 'ok', 'messages': msgs})

    async def handle_replication(self, msg, writer):
        # received replicate message from another node
        m = Message.from_dict(msg.get('message'))
        async with self._lock:
            # check if we already have this id
            if not any((mm.origin == m.origin and mm.seq == m.seq) for mm in self.messages):
                self.messages.append(m)
                print(f"[{self.id}] Replicated message received {m.id()} from {m.origin}")
        await self.send(writer, {'status': 'ok'})

    async def replicate_to_peers(self, m: Message):
        payload = {'type': 'replicate', 'message': m.to_dict()}
        for p in self.peers:
            try:
                host, port = p.split(':')
                reader, writer = await asyncio.open_connection(host, int(port))
                await self.send_raw(writer, payload)
                # can optionally wait for ack with timeout
                # but we treat replication as asynchronous fire-and-forget
                # attempt to read ack to tidy up connection
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=REPLICATION_TIMEOUT)
                    if data:
                        res = json.loads(data.decode())
                        if res.get('status') == 'ok':
                            pass
                except asyncio.TimeoutError:
                    print(f"[{self.id}] replicate to {p} timed out (fire-and-forget)")
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                print(f"[{self.id}] replicate to {p} failed: {e}")

    async def reconcile_on_start(self):
        # ask peers what they have and request missing messages
        await asyncio.sleep(1.0)  # brief pause so peers may be up
        print(f"[{self.id}] Starting reconciliation with peers...")
        local_ids = [(m.origin, m.seq) for m in self.messages]
        for p in self.peers:
            try:
                host, port = p.split(':')
                reader, writer = await asyncio.open_connection(host, int(port))
                payload = {
                    'type': 'req_sync',
                    'from': self.id,
                    'have_ids': local_ids  # can be big; ok for demo
                }
                await self.send_raw(writer, payload)
                # wait for response (send_missing will be used by peer)
                data = await asyncio.wait_for(reader.readline(), timeout=REPLICATION_TIMEOUT)
                if data:
                    res = json.loads(data.decode())
                    if res.get('type') == 'send_missing':
                        missing = res.get('messages', [])
                        count = 0
                        async with self._lock:
                            for md in missing:
                                mm = Message.from_dict(md)
                                if not any((x.origin == mm.origin and x.seq == mm.seq) for x in self.


                                           messages):
                                    self.messages.append(mm)
                                    count += 1
                        if count:
                            print(f"[{self.id}] Reconciled {count} messages from {p}")
                writer.close()
                await writer.wait_closed()
            except asyncio.TimeoutError:
                print(f"[{self.id}] reconcile with {p} timed out")
            except Exception as e:
                print(f"[{self.id}] reconcile with {p} failed: {e}")
        print(f"[{self.id}] Reconciliation task finished. Local messages: {len(self.messages)}")

    async def handle_req_sync(self, msg, writer):
        # peer asks what we have; respond with messages it doesn't have
        their_have = msg.get('have_ids', [])
        # convert to set for quick check
        their_set = set((t[0], t[1]) for t in their_have)
        missing = []
        async with self._lock:
            for m in self.messages:
                if (m.origin, m.seq) not in their_set:
                    missing.append(m.to_dict())
        # respond with send_missing
        res = {'type': 'send_missing', 'messages': missing}
        await self.send(writer, res)

    async def handle_send_missing(self, msg, writer):
        # used when we are the initiator and peer replies -- handled in reconcile_on_start
        await self.send(writer, {'status': 'ok'})

    # helpers
    async def send(self, writer: asyncio.StreamWriter, obj):
        await self.send_raw(writer, obj)
        # send newline so reader.readline() can finish

    async def send_raw(self, writer: asyncio.StreamWriter, obj):
        s = json.dumps(obj, default=str) + '\n'
        writer.write(s.encode())
        await writer.drain()

    async def send_error(self, writer, reason):
        await self.send(writer, {'status': 'error', 'reason': reason})

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--id', required=True, help='node id (ex: A, B, C)')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, required=True)
    p.add_argument('--peers', default='', help='comma separated host:port peers')
    return p.parse_args()

if name == '__main__':
    args = parse_args()
    peers = [x.strip() for x in args.peers.split(',') if x.strip()]
    node = Node(args.id, args.host, args.port, peers)
    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print(f"[{node.id}] Shutting down.")


                          
