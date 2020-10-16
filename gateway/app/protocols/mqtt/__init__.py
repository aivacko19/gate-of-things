from . import parser
from .stream import Stream
from . import const

port = 1887
port_safe = 8887

def new_stream():
    return Stream()

def parse(stream):
    packet = parser.read(stream)
    error = packet.get('error')
    if not error:
        if packet['type'] in [const.CONNACK,
                              const.SUBACK,
                              const.UNSUBACK,
                              const.PINGRESP,]:
            error = const.PROTOCOL_ERROR
    if error:
        new_packet = {'code': error}
        if packet['type'] == const.CONNECT:
            new_packet['type'] = const.CONNACK
        else:
            new_packet['type'] = const.DISCONNECT
        return new_packet, True
    return packet, False

def compose(packet):
    stream = Stream()
    parser.write(packet, stream)
    return stream

def append(stream, data):
    stream.append(data)

def load_packet(stream):
    return stream.load()

def still_loading(stream):
    return stream.still_loading()

def write(stream, data):
    stream.load(data)

def peek(stream, size):
    return stream.output(size)

def position(stream, position):
    stream.update(position)

def busy(stream):
    return stream.is_loading()

def empty(stream):
    return stream.empty()

# def _json_encode(self, obj, encoding):
#     return json.dumps(obj, ensure_ascii=False).encode(encoding)

# def _json_decode(self, json_bytes, encoding):
#     tiow = io.TextIOWrapper(
#         io.BytesIO(json_bytes), encoding=encoding, newline=""
#     )
#     obj = json.load(tiow)
#     tiow.close()
#     return obj