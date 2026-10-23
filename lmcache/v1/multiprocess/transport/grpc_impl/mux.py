# SPDX-License-Identifier: Apache-2.0
"""Unary RPCs multiplexed over one bidi stream per client (prototype).

Every unary call costs gRPC a new HTTP/2 stream, a call object and an asyncio
task on the server. Sending the same protobuf requests as frames on one
long-lived stream pays that once per client instead of once per call.

Request frame:  call id (u64) | method index (u16) | serialized request
Response frame: call id (u64) | status (u8)        | serialized response or error
"""

# Standard
from dataclasses import dataclass
from typing import Any
import struct

# Third Party
from google.protobuf import message_factory

# First Party
from lmcache.v1.multiprocess.transport.grpc_impl.descriptors import iter_methods

MUX_METHOD = "/lmcache.mp.Mux/Pipe"
MUX_SERVICE = "lmcache.mp.Mux"
REQUEST_HEADER = struct.Struct("<QH")
RESPONSE_HEADER = struct.Struct("<QB")
STATUS_OK = 0
STATUS_ERROR = 1


@dataclass(frozen=True)
class MuxMethod:
    index: int
    full_name: str
    request_cls: Any
    response_cls: Any


def mux_methods() -> list[MuxMethod]:
    """Unary methods in the order both sides number them."""
    methods = []
    for _, method in iter_methods():
        if method.server_streaming or method.client_streaming:
            continue
        methods.append(
            MuxMethod(
                index=len(methods),
                full_name=method.full_name,
                request_cls=message_factory.GetMessageClass(method.input_type),
                response_cls=message_factory.GetMessageClass(method.output_type),
            )
        )
    return methods
