"""A tiny Redis clone built with asyncio.

It speaks RESP, the Redis wire protocol, which is the same protocol real
Redis clients use.
"""
import argparse
import asyncio
import fnmatch
import random
import re
import time


# ---------- RESP encoding ----------

def simple(text):
    return b"+" + text.encode() + b"\r\n"


def error(text):
    # Replies are framed by \r\n, so never let one leak into an error message.
    text = text.replace("\r", " ").replace("\n", " ")
    return b"-" + text.encode() + b"\r\n"


def integer(n):
    return b":" + str(n).encode() + b"\r\n"


def bulk(value):
    if value is None:
        return b"$-1\r\n"
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"


def array(encoded_items):
    return b"*" + str(len(encoded_items)).encode() + b"\r\n" + b"".join(encoded_items)


# ---------- RESP parsing ----------

MAX_ARGS = 1_000_000
MAX_BULK = 64 * 1024 * 1024


class ProtocolError(Exception):
    pass


async def read_line(reader):
    """Read one line. Returns None if the client disconnected."""
    try:
        line = await reader.readline()
    except ValueError:
        raise ProtocolError("line too long")
    if not line.endswith(b"\n"):
        return None
    return line.rstrip(b"\r\n")


def parse_length(raw, what):
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError(f"invalid {what} length")


async def read_command(reader):
    """Read one command as a list of byte strings, or None if the client left."""
    line = await read_line(reader)
    if line is None:
        return None
    if not line.startswith(b"*"):
        # Inline command, the kind you type into telnet or nc: PING
        return line.split()

    count = parse_length(line[1:], "multibulk")
    if count > MAX_ARGS:
        raise ProtocolError("invalid multibulk length")
    args = []
    for _ in range(max(count, 0)):
        header = await read_line(reader)
        if header is None:
            return None
        if not header.startswith(b"$"):
            raise ProtocolError("expected '$', got " + repr(header[:1].decode("latin-1")))
        length = parse_length(header[1:], "bulk")
        if length < 0 or length > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        try:
            data = await reader.readexactly(length + 2)
        except asyncio.IncompleteReadError:
            return None
        if data[-2:] != b"\r\n":
            raise ProtocolError("bulk string not terminated by CRLF")
        args.append(data[:-2])
    return args


# ---------- The data store ----------

class Store:
    """Keys and values, plus an optional expiry time for each key."""

    def __init__(self):
        self.data = {}
        self.expires = {}  # key -> unix timestamp

    def purge_if_expired(self, key):
        """Lazy expiration: a key is removed the moment someone touches it too late."""
        deadline = self.expires.get(key)
        if deadline is not None and time.time() >= deadline:
            self.data.pop(key, None)
            del self.expires[key]
            return True
        return False

    def get(self, key):
        self.purge_if_expired(key)
        return self.data.get(key)

    def set(self, key, value, ttl=None):
        self.data[key] = value
        if ttl is None:
            self.expires.pop(key, None)  # a plain SET clears any old TTL
        else:
            self.expires[key] = time.time() + ttl

    def delete(self, key):
        self.purge_if_expired(key)
        self.expires.pop(key, None)
        return self.data.pop(key, None) is not None

    def expire(self, key, seconds):
        self.purge_if_expired(key)
        if key not in self.data:
            return False
        self.expires[key] = time.time() + seconds
        return True

    def ttl(self, key):
        """Seconds left, -1 if the key has no expiry, -2 if it does not exist."""
        self.purge_if_expired(key)
        if key not in self.data:
            return -2
        deadline = self.expires.get(key)
        if deadline is None:
            return -1
        return max(0, round(deadline - time.time()))


# ---------- Commands ----------

class CommandError(Exception):
    pass


def parse_int(raw):
    # int() alone would accept things Redis rejects, like b" 5" or b"1_0".
    if not re.fullmatch(rb"-?[0-9]+", raw):
        raise CommandError("ERR value is not an integer or out of range")
    n = int(raw)
    if not -(2 ** 63) <= n < 2 ** 63:
        raise CommandError("ERR value is not an integer or out of range")
    return n


def cmd_ping(store, args):
    return bulk(args[0]) if args else simple("PONG")


def cmd_echo(store, args):
    return bulk(args[0])


def cmd_set(store, args):
    key, value, *options = args
    ttl = None
    i = 0
    while i < len(options):
        option = options[i].upper()
        if option in (b"EX", b"PX") and i + 1 < len(options):
            amount = parse_int(options[i + 1])
            if amount <= 0:
                raise CommandError("ERR invalid expire time in 'set' command")
            ttl = amount if option == b"EX" else amount / 1000
            i += 2
        else:
            raise CommandError("ERR syntax error")
    store.set(key, value, ttl)
    return simple("OK")


def cmd_get(store, args):
    return bulk(store.get(args[0]))


def cmd_del(store, args):
    return integer(sum(store.delete(key) for key in args))


def cmd_exists(store, args):
    return integer(sum(store.get(key) is not None for key in args))


def add_to_counter(store, key, delta):
    raw = store.get(key)
    result = (0 if raw is None else parse_int(raw)) + delta
    if not -(2 ** 63) <= result < 2 ** 63:
        raise CommandError("ERR increment or decrement would overflow")
    store.data[key] = str(result).encode()  # writing to data directly keeps the TTL
    return integer(result)


def cmd_incr(store, args):
    return add_to_counter(store, args[0], 1)


def cmd_decr(store, args):
    return add_to_counter(store, args[0], -1)


def cmd_expire(store, args):
    seconds = parse_int(args[1])
    if seconds <= 0:
        return integer(int(store.delete(args[0])))
    return integer(int(store.expire(args[0], seconds)))


def cmd_ttl(store, args):
    return integer(store.ttl(args[0]))


def cmd_keys(store, args):
    pattern = args[0].decode("latin-1")
    matches = []
    for key in list(store.data):
        if store.purge_if_expired(key):
            continue
        if fnmatch.fnmatchcase(key.decode("latin-1"), pattern):
            matches.append(bulk(key))
    return array(matches)


def cmd_dbsize(store, args):
    return integer(len(store.data))


def cmd_flushall(store, args):
    store.data.clear()
    store.expires.clear()
    return simple("OK")


def cmd_command(store, args):
    # Some versions of redis-cli send this when they connect. An empty answer is fine.
    return array([])


# name -> (function, minimum arguments, maximum arguments or -1 for no limit)
COMMANDS = {
    "PING": (cmd_ping, 0, 1),
    "ECHO": (cmd_echo, 1, 1),
    "SET": (cmd_set, 2, -1),
    "GET": (cmd_get, 1, 1),
    "DEL": (cmd_del, 1, -1),
    "EXISTS": (cmd_exists, 1, -1),
    "INCR": (cmd_incr, 1, 1),
    "DECR": (cmd_decr, 1, 1),
    "EXPIRE": (cmd_expire, 2, 2),
    "TTL": (cmd_ttl, 1, 1),
    "KEYS": (cmd_keys, 1, 1),
    "DBSIZE": (cmd_dbsize, 0, 0),
    "FLUSHALL": (cmd_flushall, 0, 0),
    "COMMAND": (cmd_command, 0, -1),
}


def execute(store, args):
    name = args[0].decode("latin-1").upper()
    entry = COMMANDS.get(name)
    if entry is None:
        return error(f"ERR unknown command '{name}'")
    handler, min_args, max_args = entry
    count = len(args) - 1
    if count < min_args or (max_args != -1 and count > max_args):
        return error(f"ERR wrong number of arguments for '{name.lower()}' command")
    try:
        return handler(store, args[1:])
    except CommandError as exc:
        return error(str(exc))


# ---------- The server ----------

async def handle_client(reader, writer, store):
    try:
        while True:
            args = await read_command(reader)
            if args is None:
                break
            if args:
                writer.write(execute(store, args))
                await writer.drain()
    except ProtocolError as exc:
        writer.write(error(f"ERR Protocol error: {exc}"))
    except ConnectionError:
        pass
    finally:
        writer.close()


async def expire_sweeper(store, interval=0.1, sample_size=20):
    """Active expiry: on every tick, check a random sample of keys that have a TTL."""
    while True:
        await asyncio.sleep(interval)
        sample = random.sample(list(store.expires), min(sample_size, len(store.expires)))
        for key in sample:
            store.purge_if_expired(key)


async def serve(host="127.0.0.1", port=6380):
    store = Store()
    server = await asyncio.start_server(
        lambda reader, writer: handle_client(reader, writer, store), host, port
    )
    sweeper = asyncio.create_task(expire_sweeper(store))
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Listening on {addresses}", flush=True)
    async with server:
        await server.serve_forever()
    sweeper.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A tiny Redis clone")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    options = parser.parse_args()
    try:
        asyncio.run(serve(options.host, options.port))
    except KeyboardInterrupt:
        pass
