import asyncio
import json
import argparse
import secrets
import time

class Node:
    def __init__(self, host, port, peers, users):
        self.host = host
        self.port = port
        self.peers = peers
        self.users = users
        self.tokens = {}
        self.messages = []
        self.seen = set()
        self.seq = 0
        self.origin = f"{host}:{port}"

    async def handle_client(self, reader, writer):
        try:
            data = await reader.readline()
            if not data:
                return
            req = json.loads(data.decode())
            resp = await self.handle_request(req)
            writer.write((json.dumps(resp) + "\n").encode())
            await writer.drain()
        except Exception as e:
            print("Error handling client:", e)
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_request(self, req):
        t = req.get("type")
        if t == "login":
            user = req.get("username")
            pw = req.get("password")
            if self.users.get(user) == pw:
                token = secrets.token_hex(8)
                self.tokens[token] = user
                return {"status": "ok", "token": token}
            else:
                return {"status": "error", "msg": "invalid credentials"}

        elif t == "post":
            token = req.get("token")
            if token not in self.tokens:
                return {"status": "error", "msg": "not authenticated"}
            user = self.tokens[token]
            content = req.get("content", "")
            self.seq += 1
            msg = {
                "user": user,
                "content": content,
                "ts": time.time(),
                "origin": self.origin,
                "seq": self.seq,
            }
            if (msg["origin"], msg["seq"]) not in self.seen:
                self.messages.append(msg)
                self.seen.add((msg["origin"], msg["seq"]))
                asyncio.create_task(self.replicate(msg))
            return {"status": "ok"}

        elif t == "read":
            return {"status": "ok", "messages": self.messages}

        elif t == "replicate":
            msg = req.get("msg")
            if msg and (msg["origin"], msg["seq"]) not in self.seen:
                self.messages.append(msg)
                self.seen.add((msg["origin"], msg["seq"]))
            return {"status": "ok"}

        elif t == "sync":
            return {"status": "ok", "messages": self.messages}

        return {"status": "error", "msg": "unknown request"}

    async def replicate(self, msg):
        payload = json.dumps({"type": "replicate", "msg": msg}) + "\n"
        for h, p in self.peers:
            try:
                reader, writer = await asyncio.open_connection(h, p)
                writer.write(payload.encode())
                await writer.drain()
                await reader.readline()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def sync_with_peers(self):
        while True:
            for h, p in self.peers:
                try:
                    reader, writer = await asyncio.open_connection(h, p)
                    req = {"type": "sync"}
                    writer.write((json.dumps(req) + "\n").encode())
                    await writer.drain()
                    data = await reader.readline()
                    writer.close()
                    await writer.wait_closed()
                    if not data:
                        continue
                    res = json.loads(data.decode())
                    if res.get("status") == "ok":
                        for m in res.get("messages", []):
                            key = (m["origin"], m["seq"])
                            if key not in self.seen:
                                self.messages.append(m)
                                self.seen.add(key)
                except Exception:
                    pass
            await asyncio.sleep(5)  # faz sync a cada 5 segundos

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Node running on {addrs}")
        asyncio.create_task(self.sync_with_peers())
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--peers", nargs="*", default=[])
    args = p.parse_args()

    users = {"alice": "123", "bob": "456", "carol": "789"}

    peers = []
    for peer in args.peers:
        h, p = peer.split(":")
        peers.append((h, int(p)))

    node = Node(args.host, args.port, peers, users)
    asyncio.run(node.start())
