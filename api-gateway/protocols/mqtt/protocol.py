import sys
import json
import io
import struct

from . import parser
from . import stream

class Protocol:
    def __init__(self):
        self.port = 1887

    def get_port(self):
        return self.port

    def create_stream(self):
        return stream.MQTTStream()

    def read(self, stream):
        return parser.read(stream)

    def write(self, packet, stream):
        parser.write(packet, stream)


    # def _json_encode(self, obj, encoding):
    #     return json.dumps(obj, ensure_ascii=False).encode(encoding)

    # def _json_decode(self, json_bytes, encoding):
    #     tiow = io.TextIOWrapper(
    #         io.BytesIO(json_bytes), encoding=encoding, newline=""
    #     )
    #     obj = json.load(tiow)
    #     tiow.close()
    #     return obj