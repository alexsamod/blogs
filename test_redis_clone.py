"""Tests for redis_clone.py. They start the real server and talk to it over TCP."""
import os
import socket
import subprocess
import sys
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


class RedisError(str):
    """An error reply (a line starting with '-')."""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    """A minimal RESP client, written independently of the server's code."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.reader = self.sock.makefile("rb")

    def close(self):
        self.reader.close()
        self.sock.close()

    @staticmethod
    def encode(*args):
        out = b"*%d\r\n" % len(args)
        for arg in args:
            if not isinstance(arg, bytes):
                arg = str(arg).encode()
            out += b"$%d\r\n%s\r\n" % (len(arg), arg)
        return out

    def send_raw(self, data):
        self.sock.sendall(data)

    def call(self, *args):
        self.send_raw(self.encode(*args))
        return self.read_reply()

    def read_reply(self):
        line = self.reader.readline().rstrip(b"\r\n")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return RedisError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            return self.reader.read(n + 2)[:-2]
        if kind == b"*":
            return [self.read_reply() for _ in range(int(rest))]
        raise AssertionError(f"unexpected reply: {line!r}")


class ServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = free_port()
        cls.proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "redis_clone.py"), "--port", str(cls.port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        cls.proc.stdout.readline()  # waits for "Listening on ..."

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(timeout=5)
        cls.proc.stdout.close()
        stderr = cls.proc.stderr.read().decode()
        cls.proc.stderr.close()
        assert "Traceback" not in stderr, f"server crashed or logged an error:\n{stderr}"

    def setUp(self):
        self.c = Client(self.port)
        self.assertEqual(self.c.call("FLUSHALL"), "OK")

    def tearDown(self):
        self.c.close()


class TestBasics(ServerTestCase):
    def test_ping(self):
        self.assertEqual(self.c.call("PING"), "PONG")
        self.assertEqual(self.c.call("ping"), "PONG")  # case-insensitive
        self.assertEqual(self.c.call("PING", "hello"), b"hello")

    def test_echo(self):
        self.assertEqual(self.c.call("ECHO", "hi there"), b"hi there")

    def test_set_and_get(self):
        self.assertEqual(self.c.call("SET", "name", "ada"), "OK")
        self.assertEqual(self.c.call("GET", "name"), b"ada")

    def test_get_missing_key_is_null(self):
        self.assertIsNone(self.c.call("GET", "nope"))

    def test_set_overwrites(self):
        self.c.call("SET", "k", "one")
        self.c.call("SET", "k", "two")
        self.assertEqual(self.c.call("GET", "k"), b"two")

    def test_del_counts_removed_keys(self):
        self.c.call("SET", "a", 1)
        self.c.call("SET", "b", 2)
        self.assertEqual(self.c.call("DEL", "a", "b", "missing"), 2)
        self.assertIsNone(self.c.call("GET", "a"))
        self.assertEqual(self.c.call("DEL", "a"), 0)

    def test_del_with_duplicate_key_counts_once(self):
        self.c.call("SET", "a", 1)
        self.assertEqual(self.c.call("DEL", "a", "a"), 1)

    def test_exists(self):
        self.c.call("SET", "a", 1)
        self.assertEqual(self.c.call("EXISTS", "a"), 1)
        self.assertEqual(self.c.call("EXISTS", "a", "b"), 1)
        self.assertEqual(self.c.call("EXISTS", "a", "a"), 2)  # same as real Redis
        self.assertEqual(self.c.call("EXISTS", "b"), 0)

    def test_dbsize_and_flushall(self):
        self.c.call("SET", "a", 1)
        self.c.call("SET", "b", 2)
        self.assertEqual(self.c.call("DBSIZE"), 2)
        self.assertEqual(self.c.call("FLUSHALL"), "OK")
        self.assertEqual(self.c.call("DBSIZE"), 0)

    def test_keys_pattern(self):
        for key in ("user:1", "user:2", "order:1"):
            self.c.call("SET", key, "x")
        self.assertEqual(sorted(self.c.call("KEYS", "user:*")), [b"user:1", b"user:2"])
        self.assertEqual(sorted(self.c.call("KEYS", "*:1")), [b"order:1", b"user:1"])
        self.assertEqual(len(self.c.call("KEYS", "*")), 3)
        self.assertEqual(self.c.call("KEYS", "nothing*"), [])

    def test_command_returns_empty_array_for_redis_cli(self):
        self.assertEqual(self.c.call("COMMAND", "DOCS"), [])


class TestCounters(ServerTestCase):
    def test_incr_and_decr(self):
        self.assertEqual(self.c.call("INCR", "n"), 1)
        self.assertEqual(self.c.call("INCR", "n"), 2)
        self.assertEqual(self.c.call("DECR", "n"), 1)
        self.assertEqual(self.c.call("DECR", "fresh"), -1)
        self.assertEqual(self.c.call("GET", "n"), b"1")

    def test_incr_on_existing_number_string(self):
        self.c.call("SET", "n", 41)
        self.assertEqual(self.c.call("INCR", "n"), 42)

    def test_incr_on_non_integer_is_an_error(self):
        for bad in ("abc", "1.5", " 5", "1_0", ""):
            self.c.call("SET", "n", bad)
            reply = self.c.call("INCR", "n")
            self.assertIsInstance(reply, RedisError, bad)
            self.assertIn("not an integer", reply)

    def test_incr_overflow(self):
        self.c.call("SET", "n", 2 ** 63 - 1)
        self.assertIsInstance(self.c.call("INCR", "n"), RedisError)

    def test_incr_keeps_ttl(self):
        self.c.call("SET", "n", 1, "EX", 100)
        self.c.call("INCR", "n")
        self.assertGreater(self.c.call("TTL", "n"), 0)

    def test_many_clients_incrementing_never_lose_an_update(self):
        def worker():
            client = Client(self.port)
            for _ in range(100):
                client.call("INCR", "counter")
            client.close()

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.c.call("GET", "counter"), b"2000")


class TestExpiry(ServerTestCase):
    def test_ttl_values(self):
        self.assertEqual(self.c.call("TTL", "missing"), -2)
        self.c.call("SET", "k", "v")
        self.assertEqual(self.c.call("TTL", "k"), -1)
        self.c.call("SET", "k", "v", "EX", 100)
        self.assertIn(self.c.call("TTL", "k"), (99, 100))

    def test_set_px_expires_key(self):
        self.c.call("SET", "k", "v", "PX", 150)
        self.assertEqual(self.c.call("GET", "k"), b"v")
        time.sleep(0.3)
        self.assertIsNone(self.c.call("GET", "k"))
        self.assertEqual(self.c.call("TTL", "k"), -2)
        self.assertEqual(self.c.call("EXISTS", "k"), 0)

    def test_set_ex_option_is_case_insensitive(self):
        self.c.call("set", "k", "v", "ex", 100)
        self.assertGreater(self.c.call("TTL", "k"), 0)

    def test_plain_set_clears_ttl(self):
        self.c.call("SET", "k", "v", "EX", 100)
        self.c.call("SET", "k", "v2")
        self.assertEqual(self.c.call("TTL", "k"), -1)

    def test_expire_command(self):
        self.c.call("SET", "k", "v")
        self.assertEqual(self.c.call("EXPIRE", "k", 50), 1)
        self.assertIn(self.c.call("TTL", "k"), (49, 50))
        self.assertEqual(self.c.call("EXPIRE", "missing", 50), 0)

    def test_expire_with_zero_or_negative_deletes_key(self):
        self.c.call("SET", "k", "v")
        self.assertEqual(self.c.call("EXPIRE", "k", 0), 1)
        self.assertIsNone(self.c.call("GET", "k"))

    def test_invalid_expire_arguments(self):
        self.assertIsInstance(self.c.call("SET", "k", "v", "EX", 0), RedisError)
        self.assertIsInstance(self.c.call("SET", "k", "v", "EX", "abc"), RedisError)
        self.assertIsInstance(self.c.call("SET", "k", "v", "EX"), RedisError)
        self.assertIsInstance(self.c.call("SET", "k", "v", "BOGUS", 1), RedisError)
        self.assertIsNone(self.c.call("GET", "k"))

    def test_background_sweeper_removes_keys_nobody_touches(self):
        for i in range(50):
            self.c.call("SET", f"temp:{i}", "x", "PX", 50)
        self.c.call("SET", "forever", "x")
        self.assertEqual(self.c.call("DBSIZE"), 51)
        time.sleep(1.0)
        # DBSIZE never looks at individual keys, so only the sweeper can have removed them.
        self.assertEqual(self.c.call("DBSIZE"), 1)

    def test_keys_skips_expired(self):
        self.c.call("SET", "gone", "x", "PX", 50)
        self.c.call("SET", "here", "x")
        time.sleep(0.15)
        self.assertEqual(self.c.call("KEYS", "*"), [b"here"])


class TestErrorsAndProtocol(ServerTestCase):
    def test_unknown_command(self):
        reply = self.c.call("FLY", "away")
        self.assertIsInstance(reply, RedisError)
        self.assertIn("unknown command", reply)

    def test_wrong_number_of_arguments(self):
        for args in (("GET",), ("GET", "a", "b"), ("SET", "a"), ("DEL",), ("PING", "a", "b")):
            reply = self.c.call(*args)
            self.assertIsInstance(reply, RedisError, args)
            self.assertIn("wrong number of arguments", reply)

    def test_connection_survives_an_error(self):
        self.c.call("NOPE")
        self.assertEqual(self.c.call("PING"), "PONG")

    def test_values_are_binary_safe(self):
        value = b"line1\r\nline2\x00\xff\xfe end"
        self.assertEqual(self.c.call("SET", "bin", value), "OK")
        self.assertEqual(self.c.call("GET", "bin"), value)

    def test_unicode_values_and_keys(self):
        self.c.call("SET", "ключ", "значение ✓ 日本語")
        self.assertEqual(self.c.call("GET", "ключ").decode(), "значение ✓ 日本語")

    def test_empty_value(self):
        self.c.call("SET", "empty", "")
        self.assertEqual(self.c.call("GET", "empty"), b"")
        self.assertEqual(self.c.call("EXISTS", "empty"), 1)

    def test_large_value(self):
        big = os.urandom(1024 * 1024)
        self.assertEqual(self.c.call("SET", "big", big), "OK")
        self.assertEqual(self.c.call("GET", "big"), big)

    def test_pipelining(self):
        payload = b"".join(
            [self.c.encode("SET", "a", 1), self.c.encode("INCR", "a"), self.c.encode("GET", "a")]
        )
        self.c.send_raw(payload)
        self.assertEqual(self.c.read_reply(), "OK")
        self.assertEqual(self.c.read_reply(), 2)
        self.assertEqual(self.c.read_reply(), b"2")

    def test_command_split_across_packets(self):
        data = self.c.encode("SET", "slow", "value")
        for i in range(0, len(data), 3):
            self.c.send_raw(data[i : i + 3])
            time.sleep(0.005)
        self.assertEqual(self.c.read_reply(), "OK")
        self.assertEqual(self.c.call("GET", "slow"), b"value")

    def test_inline_commands(self):
        self.c.send_raw(b"PING\r\n")
        self.assertEqual(self.c.read_reply(), "PONG")
        self.c.send_raw(b"SET color blue\r\n")
        self.assertEqual(self.c.read_reply(), "OK")
        self.c.send_raw(b"GET color\r\n")
        self.assertEqual(self.c.read_reply(), b"blue")
        self.c.send_raw(b"\r\n")  # a blank line is ignored
        self.c.send_raw(b"PING\r\n")
        self.assertEqual(self.c.read_reply(), "PONG")

    def test_clients_share_the_same_data(self):
        other = Client(self.port)
        self.c.call("SET", "shared", "yes")
        self.assertEqual(other.call("GET", "shared"), b"yes")
        other.close()

    def test_error_text_cannot_break_framing(self):
        reply = self.c.call("BAD\r\n+OK", "x")
        self.assertIsInstance(reply, RedisError)
        self.assertEqual(self.c.call("PING"), "PONG")

    def test_garbage_protocol_gets_an_error_and_a_closed_connection(self):
        self.c.send_raw(b"*2\r\nGARBAGE\r\n")
        reply = self.c.read_reply()
        self.assertIsInstance(reply, RedisError)
        self.assertIn("Protocol error", reply)
        self.assertEqual(self.c.reader.readline(), b"")  # server closed the socket

    def test_bad_lengths_are_rejected(self):
        for bad in (b"*abc\r\n", b"*1\r\n$abc\r\n", b"*1\r\n$-5\r\n", b"*1\r\n$3\r\nabcXY"):
            client = Client(self.port)
            client.send_raw(bad)
            self.assertIsInstance(client.read_reply(), RedisError, bad)
            client.close()

    def test_null_array_is_ignored(self):
        self.c.send_raw(b"*-1\r\n")
        self.c.send_raw(b"*0\r\n")
        self.assertEqual(self.c.call("PING"), "PONG")

    def test_abrupt_disconnects_do_not_hurt_the_server(self):
        partial_commands = [b"*3\r\n$3\r\nSET\r\n$1\r\nk", b"*2\r\n$4\r\nPI", b"*1\r\n$4\r\n", b"PIN"]
        for partial in partial_commands:
            s = socket.create_connection(("127.0.0.1", self.port))
            s.sendall(partial)
            s.close()
        # One client that resets the connection instead of closing it politely.
        s = socket.create_connection(("127.0.0.1", self.port))
        s.sendall(Client.encode("GET", "k"))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
        s.close()
        time.sleep(0.1)
        self.assertEqual(self.c.call("PING"), "PONG")

    def test_many_simultaneous_connections(self):
        clients = [Client(self.port) for _ in range(200)]
        for i, client in enumerate(clients):
            client.call("SET", f"key{i}", i)
        for i, client in enumerate(clients):
            self.assertEqual(client.call("GET", f"key{(i + 1) % 200}"), str((i + 1) % 200).encode())
        for client in clients:
            client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
