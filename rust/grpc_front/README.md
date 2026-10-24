# LMCache Rust gRPC Front

Native gRPC front end for the LMCache multiprocess server, used by
`--grpc-server-mode native`.

The grpcio Python server runs its own Python layer for every call, under the
same GIL as the handlers. This crate does the HTTP/2 and gRPC framing on a
small tokio runtime instead and hands Python each call as an event: an id,
the method, the request bytes and who sent it. Python still decodes the
protobuf, runs the handler and encodes the response, so the wire format, the
clients and the handlers are the same as with the `sync` and `aio` servers.

```
grpcio client ──HTTP/2──▶ tokio threads (no GIL)
                            │ path → method id, client id metadata
                            ▼
                      event channel ──▶ Python dispatcher thread
                                          decode → handler pool → encode
                            ◀── respond(call_id, bytes) from any thread
```

## Install

```bash
pip install lmcache-rust-grpc-front   # Linux wheels, released with lmcache
```

## Build

On other platforms, or from a checkout:

```bash
pip install maturin
maturin develop --release -m rust/grpc_front/Cargo.toml
```

Then start the server with `lmcache server --transport grpc --grpc-server-mode native`.

## Python API

`NativeGrpcFront(target, methods, io_threads=2)` binds `host:port` (every
address the host resolves to), `unix:path` or `unix:///path`. `methods` lists
`(path, server_streaming)` pairs; a method's id is its index in the list.

| Member | Purpose |
|---|---|
| `bound_port` | the bound TCP port; 1 for a Unix socket, as grpcio reports it |
| `set_fallback(path)` | forward unknown methods to a grpcio server on a Unix socket |
| `start()` | start accepting connections |
| `recv_batch(max_events, timeout_ms=None)` | wait for events with the GIL released; `None` once closed |
| `respond(call_id, payload)` | answer a unary call |
| `fail(call_id, code, message)` | end any call with a non-OK status, without blocking |
| `stream_send(call_id, payload)` | send one streamed message; blocks while the client is not reading |
| `stream_end(call_id)` | finish a stream with OK; a stream cut short by `close()` ends with UNAVAILABLE |
| `stream_open(call_id)` | whether the client is still reading a stream |
| `close()` | stop without waiting for calls in flight; their clients get UNAVAILABLE |

`recv_batch` returns `(kind, call_id, method_id, payload, client_key)` tuples.
`kind` is `EVENT_CALL` for a new call, whose `client_key` is the
`lmcache-client-id-bin` metadata or else the peer address, and `EVENT_CANCEL`
when the client left a stream before it ended (`payload` and `client_key`
are then `None`).

## Limitations

- Unix only: Linux wheels are published; macOS builds from source.
- Insecure only, like the grpcio servers LMCache starts: no TLS and no
  authentication yet.
- Unary and server-streaming methods, which is all the generated services
  use. Out-of-tree services can use any kind; they are served by grpcio
  behind the front.
- Out-of-tree calls take one extra local hop, and `context.peer()` in their
  handlers is the private socket.
- Clients of a Unix socket server must set `grpc.default_authority`, since
  the socket path grpc-core sends as `:authority` is not valid HTTP/2.
- Like grpc-core, binding a Unix socket path replaces any socket file
  already there, even one another server is still using.
- Per connection: at most 1024 concurrent streams and 16 KiB of request
  metadata; a connection that has not finished the HTTP/2 preface after 120 s
  is dropped.

## Tests

```bash
cargo test --locked --no-default-features
pytest tests/v1/multiprocess/test_grpc_front.py \
    tests/v1/multiprocess/test_grpc_native_server.py
```
