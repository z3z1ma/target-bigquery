"""Single-writer process diagnostics with nonblocking parent reception."""

from __future__ import annotations

import os
import pickle
import socket
import struct
from multiprocessing.reduction import ForkingPickler
from weakref import WeakSet

_receivers: WeakSet[ErrorReceiver] = WeakSet()


def _close_inherited_receivers() -> None:
    # Child workers own senders only. Weak references avoid retaining retired
    # channels across repeated worker creation/checkpoints.
    for receiver in list(_receivers):
        receiver.close()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_close_inherited_receivers)


class ErrorSender:
    """Worker-owned endpoint; never shared by sibling workers."""

    def __init__(self, endpoint: socket.socket):
        self.endpoint = endpoint

    def send(self, value: tuple[Exception, str]) -> None:
        payload = ForkingPickler.dumps(value)
        self.endpoint.sendall(struct.pack("!Q", len(payload)))
        self.endpoint.sendall(payload)

    def close(self) -> None:
        self.endpoint.close()


class ErrorReceiver:
    """Accumulate complete frames without waiting for a live or dead sender."""

    def __init__(self, endpoint: socket.socket):
        self.endpoint = endpoint
        self.endpoint.setblocking(False)
        self.buffer = bytearray()
        self.closed = False
        _receivers.add(self)

    @classmethod
    def create(cls) -> tuple[ErrorReceiver, ErrorSender]:
        reader, writer = socket.socketpair()
        return cls(reader), ErrorSender(writer)

    def drain(self, *, finished: bool = False) -> list[tuple[Exception, str]]:
        messages: list[tuple[Exception, str]] = []
        # Fairness while workers are live. After exit, only a finite amount of
        # kernel-buffered data remains; consume it before assessing truncation.
        remaining = 16
        while not self.closed and (finished or remaining):
            remaining -= 1
            try:
                chunk = self.endpoint.recv(65536)
            except BlockingIOError:
                break
            except OSError as exc:
                messages.append((exc, f"Worker diagnostic channel failed: {exc}"))
                self.close()
                break
            if not chunk:
                self.close()
                break
            self.buffer.extend(chunk)
            while len(self.buffer) >= 8:
                length = struct.unpack("!Q", self.buffer[:8])[0]
                if len(self.buffer) < length + 8:
                    break
                payload = self.buffer[8 : length + 8]
                del self.buffer[: length + 8]
                try:
                    value = pickle.loads(payload)
                    if not (
                        isinstance(value, tuple)
                        and len(value) == 2
                        and isinstance(value[0], Exception)
                        and isinstance(value[1], str)
                    ):
                        raise ValueError("Invalid worker diagnostic")
                    messages.append(value)
                except Exception as exc:
                    messages.append((exc, f"Could not decode worker diagnostic: {exc}"))
        if finished or self.closed:
            if self.buffer:
                incomplete = RuntimeError("Worker exited with an incomplete diagnostic frame.")
                messages.append((incomplete, str(incomplete)))
                self.buffer.clear()
            self.close()
        return messages

    def close(self) -> None:
        self.closed = True
        self.endpoint.close()
