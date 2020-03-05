import parser
from .stream import Stream

port = 1887

def new_stream():
    return Stream()

def parse(stream):
    return parser.read(stream)

def compose(packet):
    stream = Stream()
    parser.write(packet, stream)
    return stream

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