"""Real-terminal integration test. Run with uv run --with pyte python3 this-file --kai PATH --zellij PATH."""

import argparse
import codecs
import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import tempfile
import termios
import time
import unittest
from pathlib import Path

import pyte

parser = argparse.ArgumentParser()
parser.add_argument("--kai", type=Path, required=True)
parser.add_argument("--zellij", type=Path, required=True)
ARGS, remaining = parser.parse_known_args()
KAI = ARGS.kai.resolve()
ZELLIJ = ARGS.zellij.resolve()


class Screen(pyte.Screen):
    def write_process_input(self, data):
        os.write(self.fd, data.encode())

    def report_device_status(self, mode, **kwargs):
        if not kwargs.get("private"):
            super().report_device_status(mode)

    def report_device_attributes(self, mode=0, **kwargs):
        if not kwargs.get("private"):
            super().report_device_attributes(mode)


class Client:
    def __init__(self, arguments, environment, cwd):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
            os.chdir(cwd)
            os.execvpe(str(arguments[0]), [str(arg) for arg in arguments], environment)
        self.screen = Screen(120, 30)
        self.screen.fd = self.fd
        self.stream = pyte.Stream(self.screen)
        self.decoder = codecs.getincrementaldecoder("utf8")(errors="replace")
        self.output = bytearray()
        self.status = None
        self.eof = False

    def text(self):
        return "\n".join(self.screen.display)

    def send(self, data):
        os.write(self.fd, data)

    def poll(self):
        if self.status is None:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.status = os.waitstatus_to_exitcode(status)
        return self.status


class PaneAttachmentTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="kai-zellij-test-")
        self.root = Path(self.directory.name)
        self.clients = []
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("ZELLIJ")
        }
        for key, subdir in [
            ("ZELLIJ_SOCKET_DIR", "sockets"),
            ("XDG_CONFIG_HOME", "config"),
            ("XDG_CACHE_HOME", "cache"),
            ("XDG_DATA_HOME", "data"),
            ("XDG_RUNTIME_DIR", "runtime"),
        ]:
            directory = self.root / subdir
            directory.mkdir(mode=0o700)
            self.environment[key] = str(directory)
        binary = self.root / "bin"
        binary.mkdir()
        (binary / "zellij").symlink_to(ZELLIJ)
        (binary / "codex").write_text(r"""#!/usr/bin/env python3
import json, os, pathlib, signal, sys, termios, tty
if sys.argv[1:] == ['--version']:
    print('codex-cli ' + ('0.154.0-k.ac192cd7' if os.environ.get('KAI_E2E_SUPERVISED') else '0.154.0'))
    raise SystemExit(0)
marker = os.environ['KAI_E2E_MARKER']
root = pathlib.Path(os.environ['KAI_E2E_ROOT'])
if os.environ.get('KAI_E2E_SUPERVISED') and sys.argv[1] != 'resume':
    handoff = root / 'handoff.json'
    thread = '123e4567-e89b-12d3-a456-426614174000'
    handoff.write_text(json.dumps({'format': 'codex+k-input-handoff', 'version': 1,
                                  'thread_id': thread, 'resume_args': ['--model', 'gpt-test']}))
    print('codex+k (' + thread + '): supervised exit ' + json.dumps({
        'version': 3, 'outcome': os.environ.get('KAI_E2E_OUTCOME', 'stream-disconnected'),
        'handoff_path': str(handoff),
        'unavailable_until': None}), flush=True)
    raise SystemExit(75)
(root / (marker + '.json')).write_text(json.dumps({'cwd': os.getcwd(), 'marker': marker, 'args': sys.argv[1:]}))
def redraw(*_):
    os.write(1, ('\x1b[2J\x1b[HREADY ' + marker).encode())
    (root / (marker + '.size')).write_text(json.dumps(list(os.get_terminal_size(0))))
signal.signal(signal.SIGWINCH, redraw)
redraw()
os.write(1, b'\x1b[?2004h')
if os.environ.get('KAI_E2E_MOUSE'):
    os.write(1, b'\x1b[?1000h\x1b[?1006h')
if 'KAI_E2E_EXIT' in os.environ:
    raise SystemExit(int(os.environ['KAI_E2E_EXIT']))
saved = termios.tcgetattr(0)
tty.setraw(0)
code = 0
try:
    while True:
        data = os.read(0, 4096)
        if not data:
            break
        with (root / (marker + '.input')).open('ab') as file:
            file.write(data)
        if b'\x03' in data:
            code = 7
            break
        if b'\x04' in data:
            code = 9
            break
finally:
    termios.tcsetattr(0, termios.TCSANOW, saved)
raise SystemExit(code)
""")
        (binary / "codex").chmod(0o700)
        self.environment.update(
            PATH=str(binary) + os.pathsep + self.environment["PATH"],
            TERM="xterm-256color",
            COLORTERM="truecolor",
            SHELL="/bin/sh",
            KAI_E2E_ROOT=str(self.root),
        )

    def tearDown(self):
        subprocess.run(
            [ZELLIJ, "kill-session", "kai"],
            check=False,
            env=self.environment,
            capture_output=True,
            timeout=5,
        )
        for client in self.clients:
            if client.poll() is None:
                try:
                    os.killpg(client.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                deadline = time.monotonic() + 2
                while client.poll() is None and time.monotonic() < deadline:
                    self.pump(0.05)
                if client.poll() is None:
                    os.killpg(client.pid, signal.SIGKILL)
                    os.waitpid(client.pid, 0)
            if client.fd is not None:
                os.close(client.fd)
        self.directory.cleanup()

    def pump(self, duration=0.05):
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            fds = [c.fd for c in self.clients if c.fd is not None and not c.eof]
            ready, _, _ = select.select(fds, [], [], 0.02)
            for client in self.clients:
                if client.fd not in ready:
                    continue
                try:
                    data = os.read(client.fd, 262144)
                except OSError:
                    data = b""
                if not data:
                    client.eof = True
                    continue
                client.output.extend(data)
                client.stream.feed(client.decoder.decode(data))

    def wait(self, condition, message, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.pump()
            if condition():
                return
        self.fail(message + "\n" + "\n".join(c.text() for c in self.clients))

    def spawn(self, arguments, marker=None, extra=None):
        environment = dict(self.environment)
        cwd = self.root
        if marker:
            environment["KAI_E2E_MARKER"] = marker
            cwd = self.root / marker
            cwd.mkdir(exist_ok=True)
        environment.update(extra or {})
        client = Client(arguments, environment, cwd)
        self.clients.append(client)
        return client

    def action(self, *arguments):
        process = subprocess.Popen(
            [ZELLIJ, "-s", "kai", "action", *arguments],
            env=self.environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 10
        while process.poll() is None and time.monotonic() < deadline:
            self.pump()
        if process.poll() is None:
            process.kill()
        stdout, stderr = process.communicate()
        self.assertEqual(process.returncode, 0, stderr.decode())
        return stdout.decode()

    def panes(self):
        return [
            p
            for p in json.loads(self.action("list-panes", "--json"))
            if not p["is_plugin"]
        ]

    def test_supervision_survives_inside_the_pane(self):
        client = self.spawn([KAI, "--fast"], "RECOVERED", {"KAI_E2E_SUPERVISED": "1"})
        self.wait(
            lambda: "READY RECOVERED" in client.text(),
            "supervised recovery did not resume",
        )
        call = json.loads((self.root / "RECOVERED.json").read_text())
        self.assertEqual(call["args"][0], "resume")
        self.assertEqual(len(self.panes()), 1)
        client.send(b"\x04")
        self.wait(
            lambda: client.poll() is not None,
            "supervisor did not return its exit status",
        )
        self.assertEqual(client.status, 9)

    def test_error_stays_visible_inside_the_pane(self):
        client = self.spawn(
            [KAI, "--fast"],
            "ERROR",
            {"KAI_E2E_SUPERVISED": "1", "KAI_E2E_OUTCOME": "credential-invalid"},
        )
        self.wait(
            lambda: "automatic credential rotation requires a configured credential provider"
            in client.text(),
            "Kai error was not shown in the pane",
        )
        self.assertIsNone(client.poll())
        self.assertEqual(len(self.panes()), 1)
        client.send(b"\n")
        self.wait(lambda: client.poll() is not None, "error pane did not close after dismissal")
        self.assertEqual(client.status, 1)

    def test_kai_sessions(self):
        first = self.spawn([KAI, "--fast"], "FIRST")
        second = self.spawn([KAI], "SECOND")
        self.wait(
            lambda: "READY FIRST" in first.text() and "READY SECOND" in second.text(),
            "Kai did not start",
        )
        self.assertEqual(len(self.panes()), 2)
        for client, marker in [(first, "FIRST"), (second, "SECOND")]:
            self.assertNotIn("Zellij", client.text())
            self.assertNotIn(b"Waiting to run:", client.output)
            self.assertTrue(
                client.screen.display[0].startswith("READY " + marker), client.text()
            )
            self.assertEqual(
                json.loads((self.root / (marker + ".json")).read_text())["cwd"],
                str(self.root / marker),
            )
        self.assertEqual(json.loads((self.root / "FIRST.size").read_text()), [120, 30])
        fcntl.ioctl(first.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 35, 100, 0, 0))
        first.screen.resize(lines=35, columns=100)
        os.killpg(first.pid, signal.SIGWINCH)
        self.wait(
            lambda: json.loads((self.root / "FIRST.size").read_text()) == [100, 35],
            "pane-only resize did not use the full terminal",
        )
        first.send(b"\x07\x14" + b"2")
        self.wait(
            lambda: (self.root / "FIRST.input").exists(),
            "application did not receive input",
        )
        self.assertIn(b"\x07\x14" + b"2", (self.root / "FIRST.input").read_bytes())
        first.send(b"\x1b[200~pasted text\x1b[201~")
        self.wait(
            lambda: (
                b"\x1b[200~pasted text\x1b[201~"
                in (self.root / "FIRST.input").read_bytes()
            ),
            "bracketed paste did not reach the application",
        )

        manager = self.spawn([ZELLIJ, "attach", "kai"])
        self.wait(
            lambda: "Zellij" in manager.text(), "management interface did not appear"
        )
        manager.send(b"\x14" + b"2")
        self.wait(
            lambda: "READY FIRST" in first.text() and "READY SECOND" in second.text(),
            "resize did not redraw applications",
        )
        self.assertIn("READY FIRST", first.text())
        self.assertIn("READY SECOND", second.text())
        self.assertNotIn("Zellij", first.text())

        first.send(b"\x03")
        self.wait(lambda: first.poll() is not None, "first attachment did not exit")
        self.assertEqual(first.status, 7)
        self.assertNotIn(b"READY SECOND", first.output)
        self.assertIsNone(second.poll())
        self.assertIsNone(manager.poll())

        pane = self.panes()[0]["id"]
        os.killpg(second.pid, signal.SIGHUP)
        self.wait(lambda: second.poll() is not None, "terminal close did not detach")
        self.assertEqual(len(self.panes()), 1)
        reattached = self.spawn([ZELLIJ, "attach", "kai", "--pane", str(pane)])
        self.wait(
            lambda: "READY SECOND" in reattached.text(),
            "reattachment lost the running application",
        )
        self.assertNotIn("Zellij", reattached.text())

        # Keep one unrelated tab alive while exercising immediate exits and tab removal.
        self.action(
            "new-tab",
            "--name",
            "sentinel",
            "--no-focus",
            "--",
            "/bin/sh",
            "-c",
            "sleep 600",
        )
        fast = self.spawn([KAI], "FAST", {"KAI_E2E_EXIT": "23"})
        self.wait(lambda: fast.poll() is not None, "fast command exit hung")
        self.assertEqual(fast.status, 23)
        self.assertIn(b"READY FAST", fast.output)
        self.assertIsNone(manager.poll())

        self.action("close-pane", "--pane-id", str(pane))
        self.wait(
            lambda: reattached.poll() is not None,
            "closing the pane did not end its attachment",
        )
        self.assertNotEqual(reattached.status, 0)
        self.assertNotIn(b"sentinel", reattached.output)
        self.assertIsNone(manager.poll())

        # No original client remains; creating another Kai still uses the background session.
        os.killpg(manager.pid, signal.SIGHUP)
        self.wait(lambda: manager.poll() is not None, "manager did not detach")
        headless = self.spawn([KAI], "HEADLESS")
        self.wait(
            lambda: "READY HEADLESS" in headless.text(),
            "launch into a headless session failed",
        )
        headless.send(b"\x04")
        self.wait(lambda: headless.poll() is not None, "last command did not exit")
        self.assertEqual(headless.status, 9)
        self.assertFalse(list((self.root / "runtime/kai").glob("run-*")))

    def test_mouse_input_and_tab_closure(self):
        client = self.spawn([KAI], "MOUSE", {"KAI_E2E_MOUSE": "1"})
        self.wait(lambda: "READY MOUSE" in client.text(), "application did not start")
        self.wait(
            lambda: b"\x1b[?1006h" in client.output, "mouse reporting was not enabled"
        )
        client.send(b"\x1b[<0;4;3M\x1b[<0;4;3m")
        self.wait(
            lambda: (self.root / "MOUSE.input").exists(), "mouse input did not arrive"
        )
        self.assertIn(b"\x1b[<0;4;3M", (self.root / "MOUSE.input").read_bytes())
        pane = self.panes()[0]
        invalid = self.spawn([ZELLIJ, "attach", "kai", "--pane", "4294967295"])
        self.wait(lambda: invalid.poll() is not None, "invalid pane attachment hung")
        self.assertNotEqual(invalid.status, 0)
        self.assertNotIn(b"READY MOUSE", invalid.output)
        self.action("close-tab", "--tab-id", str(pane["tab_id"]))
        self.wait(
            lambda: client.poll() is not None,
            "closing the tab did not end its attachment",
        )
        self.assertNotEqual(client.status, 0)


if __name__ == "__main__":
    unittest.main(argv=[__file__, *remaining])
