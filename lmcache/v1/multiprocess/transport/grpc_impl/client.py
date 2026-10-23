# SPDX-License-Identifier: Apache-2.0
"""Method-oriented client built directly from generated gRPC descriptors."""

# Future
from __future__ import annotations

# Standard
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlparse
import itertools
import os
import queue
import threading
import uuid

# Third Party
import grpc

# First Party
from lmcache.v1.multiprocess.futures import MessagingFuture, MessagingStream
from lmcache.v1.multiprocess.transport.base import RequestClient
from lmcache.v1.multiprocess.transport.grpc_impl import mux
from lmcache.v1.multiprocess.transport.grpc_impl.descriptors import (
    client_method_name,
    iter_methods,
)
from lmcache.v1.multiprocess.transport.grpc_impl.method_registry import (
    GrpcMethodCodec,
    get_method_codec_registry,
)

CONNECT_TIMEOUT_MS = 5000
INITIAL_RECONNECT_BACKOFF_MS = 100
MAX_RECONNECT_BACKOFF_MS = 1000

# Same bounds as the ZMQ client (#4671); gRPC defaults are 20 s and 120 s.
# "min_reconnect_backoff_ms" is gRPC's per-attempt connect timeout.
_GRPC_OPTIONS = (
    ("grpc.max_send_message_length", -1),
    ("grpc.max_receive_message_length", -1),
    ("grpc.min_reconnect_backoff_ms", CONNECT_TIMEOUT_MS),
    ("grpc.initial_reconnect_backoff_ms", INITIAL_RECONNECT_BACKOFF_MS),
    ("grpc.max_reconnect_backoff_ms", MAX_RECONNECT_BACKOFF_MS),
)
_CLIENT_ID_METADATA_KEY = "lmcache-client-id-bin"


def parse_grpc_target(server_url: str) -> str:
    """Convert an LMCache multiprocess endpoint into a gRPC target.

    Args:
        server_url: gRPC endpoint.

    Returns:
        A target accepted by ``grpc.insecure_channel``.

    Raises:
        ValueError: If the URL scheme or target is invalid.
    """
    if "://" not in server_url:
        if not server_url:
            raise ValueError("gRPC server target must not be empty")
        return server_url

    parsed = urlparse(server_url)
    if parsed.scheme == "grpc":
        if not parsed.netloc:
            raise ValueError(f"Missing host in gRPC URL: {server_url!r}")
        return parsed.netloc
    if parsed.scheme == "grpc+unix":
        path = f"/{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
        if not path:
            raise ValueError(f"Missing socket path in gRPC URL: {server_url!r}")
        return f"unix://{path}"
    raise ValueError(f"Unsupported gRPC URL scheme: {parsed.scheme!r}")


@dataclass(frozen=True)
class _ClientRpc:
    stub_method: Any
    codec: GrpcMethodCodec
    mux_method: mux.MuxMethod | None = None


class _MuxStream:
    """One bidi stream carrying this client's unary calls as frames."""

    def __init__(self, channel: grpc.Channel, metadata: Any) -> None:
        self._outbox: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
        self._ids = itertools.count()
        self._waiting: dict[int, tuple[MessagingFuture[Any], _ClientRpc]] = {}
        self._lock = threading.Lock()
        call = channel.stream_stream(mux.MUX_METHOD)(
            iter(self._outbox.get, None), metadata=metadata, wait_for_ready=True
        )
        self._call = call
        threading.Thread(target=self._read, daemon=True, name="grpc-mux-read").start()

    def send(self, rpc: _ClientRpc, request: Any) -> MessagingFuture[Any]:
        assert rpc.mux_method is not None
        future: MessagingFuture[Any] = MessagingFuture()
        call_id = next(self._ids)
        with self._lock:
            self._waiting[call_id] = (future, rpc)
        self._outbox.put(
            mux.REQUEST_HEADER.pack(call_id, rpc.mux_method.index)
            + request.SerializeToString()
        )
        return future

    def close(self) -> None:
        self._outbox.put(None)
        self._call.cancel()

    def _read(self) -> None:
        try:
            for frame in self._call:
                call_id, status = mux.RESPONSE_HEADER.unpack_from(frame)
                body = frame[mux.RESPONSE_HEADER.size :]
                with self._lock:
                    future, rpc = self._waiting.pop(call_id)
                assert rpc.mux_method is not None
                if status == mux.STATUS_OK:
                    try:
                        future.set_result(
                            rpc.codec.response_decoder(
                                rpc.mux_method.response_cls.FromString(body)
                            )
                        )
                    except BaseException as exc:
                        future.set_exception(exc)
                else:
                    future.set_exception(RuntimeError(body.decode()))
        except grpc.RpcError as exc:
            with self._lock:
                waiting, self._waiting = self._waiting, {}
            for future, _ in waiting.values():
                future.set_exception(ConnectionError(str(exc)))


ClientRpcCallable = Callable[..., MessagingFuture[Any] | MessagingStream[Any]]


class GrpcMultiprocessClient(RequestClient):
    """Expose generated unary and server-streaming RPCs as client methods."""

    def __init__(self, server_url: str) -> None:
        self._channel = grpc.insecure_channel(
            parse_grpc_target(server_url), options=_GRPC_OPTIONS
        )
        stubs: dict[str, Any] = {}
        self._rpc_methods: dict[str, _ClientRpc] = {}
        codec_registry = get_method_codec_registry()
        mux_by_name = {m.full_name: m for m in mux.mux_methods()}
        for binding, method in iter_methods():
            service_name = binding.descriptor.name
            stub = stubs.get(service_name)
            if stub is None:
                stub_class = getattr(binding.grpc_module, f"{service_name}Stub")
                stub = stub_class(self._channel)
                stubs[service_name] = stub
            name = client_method_name(method.name)
            if name in self._rpc_methods:
                raise RuntimeError(f"Duplicate gRPC client method: {name}")
            self._rpc_methods[name] = _ClientRpc(
                stub_method=getattr(stub, method.name),
                codec=codec_registry.by_full_name[method.full_name],
                mux_method=mux_by_name.get(method.full_name),
            )
        self._metadata = ((_CLIENT_ID_METADATA_KEY, uuid.uuid4().bytes),)
        # Prototype switch: send unary calls as frames on one stream.
        self._mux = (
            _MuxStream(self._channel, self._metadata)
            if os.environ.get("LMCACHE_GRPC_MUX") == "1"
            else None
        )

    def __getattr__(self, name: str) -> ClientRpcCallable:
        """Resolve a generated RPC as a method-oriented client call."""
        rpc = self._rpc_methods.get(name)
        if rpc is None:
            raise AttributeError(
                f"{self.__class__.__name__!r} has no attribute {name!r}"
            )

        def invoke(
            *args: Any, **kwargs: Any
        ) -> MessagingFuture[Any] | MessagingStream[Any]:
            return self._call(rpc, args, kwargs)

        return invoke

    def __dir__(self) -> list[str]:
        """Include descriptor-derived RPC methods in introspection output."""
        return sorted(set(super().__dir__()) | set(self._rpc_methods))

    def cb_register_rope_v3(self, *args: Any, **kwargs: Any) -> MessagingFuture[Any]:
        """Call the compatibility alias for ``CbRegisterRope``."""
        return self.cb_register_rope(*args, **kwargs)

    def cb_unregister_rope_v3(self, *args: Any, **kwargs: Any) -> MessagingFuture[Any]:
        """Call the compatibility alias for ``CbUnregisterRope``."""
        return self.cb_unregister_rope(*args, **kwargs)

    def cb_retrieve_pre_computed_v3(
        self, *args: Any, **kwargs: Any
    ) -> MessagingFuture[Any]:
        """Call the compatibility alias for ``CbRetrievePreComputed``."""
        return self.cb_retrieve_pre_computed(*args, **kwargs)

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        if self._mux is not None:
            self._mux.close()
        self._channel.close()

    def _call(
        self,
        rpc: _ClientRpc,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> MessagingFuture[Any] | MessagingStream[Any]:
        request = rpc.codec.request_encoder(args, kwargs)
        if isinstance(rpc.stub_method, grpc.UnaryStreamMultiCallable):
            call = rpc.stub_method(
                request, metadata=self._metadata, wait_for_ready=True
            )

            def read(closed: threading.Event) -> Any:
                try:
                    return rpc.codec.response_decoder(next(call))
                except grpc.RpcError as exc:
                    if exc.code() in (
                        grpc.StatusCode.UNAVAILABLE,
                        grpc.StatusCode.CANCELLED,
                    ):
                        raise ConnectionError("gRPC event stream disconnected") from exc
                    raise

            return MessagingStream(read, call.cancel)
        if self._mux is not None and rpc.mux_method is not None:
            return self._mux.send(rpc, request)
        future: MessagingFuture[Any] = MessagingFuture()
        call = rpc.stub_method.future(
            request,
            metadata=self._metadata,
            wait_for_ready=True,
        )

        def on_done(grpc_future: grpc.Future[Any]) -> None:
            try:
                result = rpc.codec.response_decoder(grpc_future.result())
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

        call.add_done_callback(on_done)
        return future


def _make_client_rpc_method(name: str) -> ClientRpcCallable:
    def rpc_method(
        self: GrpcMultiprocessClient,
        *args: Any,
        **kwargs: Any,
    ) -> MessagingFuture[Any] | MessagingStream[Any]:
        return self._call(self._rpc_methods[name], args, kwargs)

    rpc_method.__name__ = name
    rpc_method.__qualname__ = f"GrpcMultiprocessClient.{name}"
    return rpc_method


def _install_client_rpc_methods() -> None:
    """Install descriptor-derived RPC methods on the concrete client class."""
    for _, method in iter_methods():
        name = client_method_name(method.name)
        if name in GrpcMultiprocessClient.__dict__:
            raise RuntimeError(f"gRPC client method conflicts with {name!r}")
        setattr(GrpcMultiprocessClient, name, _make_client_rpc_method(name))


_install_client_rpc_methods()
