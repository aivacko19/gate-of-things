import struct



decode_map = {
    BYTE: decode_byte,
    TWO_BYTE_INT: decode_two_byte_int,
    FOUR_BYTE_INT: decode_four_byte_int,
    UTF8_ENCODED_STRING: decode_utf8_encoded_string,
    VARIABLE_BYTE_INT: decode_variable_byte_int,
    BINARY_DATA: decode_binary_data,
    UTF8_STRING_PAIR: decode_utf8_string_pair
}

encode_map = {
    BYTE: encode_byte,
    TWO_BYTE_INT: encode_two_byte_int,
    FOUR_BYTE_INT: encode_four_byte_int,
    UTF8_ENCODED_STRING: encode_utf8_encoded_string,
    VARIABLE_BYTE_INT: encode_variable_byte_int,
    BINARY_DATA: encode_binary_data,
    UTF8_STRING_PAIR: encode_utf8_string_pair
}

class StreamLengthException(Exception):
    def __init__(self, message):
        super().__init__(message)

def check_bytes(stream):
    if type(stream) is not bytes:
        raise Exception('Parameter is not a byte string')

def check_number(value):
    if not isinstance(value, (int, long)):
        raise Exception("Parameter is not a number")

def check_string(value):
    if type(value) is not str:
        raise Exception("Parameter is not a string")

def check_tuple(value):
    if type(value) is not tuple:
        raise Exception("Parameter is not a tuple")

def decode(stream, data_type):
    if not decode_map.has_key(data_type):
        raise Exception('Unvalid data type')
    return decode_map[data_type](stream)

def decode_byte(stream):
    check_bytes(stream)
    if len(stream) < 1:
        raise StreamLengthError("Stream can't load byte (1)")
    value = struct.unpack(">B", stream[:1])[0]
    return value, 1

def decode_two_byte_int(stream):
    check_bytes(stream)
    if len(stream) < 2:
        raise StreamLengthError("Stream can't load short int (2)")
    value = struct.unpack(">H", stream[:2])[0]
    return value, 2

def decode_four_byte_int(stream):
    check_bytes(stream)
    if len(stream) < 4:
        raise StreamLengthError("Stream can't load long int (4)")
    value = struct.unpack(">L", stream[:4])[0]
    return value, 4

def decode_variable_byte_int(stream):
    multiplier = 1
    value = 0
    number_of_bytes = 0
    while True:
        byte, index = decode_byte(stream)
        stream = stream[index:]
        value += (byte & 0x7F) * multiplier
        if multiplier > 0x80**3:
            raise Exception('Malformed Variable Byte Integer')
        multiplier *= 0x80
        number_of_bytes += 1
        if byte & 0x80 == 0:
            break
    return value, number_of_bytes

def decode_binary_data(stream):
    length, index = decode_two_byte_int(stream)
    stream = stream[index:]
    if len(stream) < length:
        raise StreamLengthError("Stream can't load data (" + str(length + index) + ")")
    value = stream[:length]
    return value, length + index

def decode_utf8_encoded_string(stream):
    value, index = decode_binary_data(stream)
    return value.decode("utf-8"), index

def decode_utf8_string_pair(stream):
    key, index1 = decode_utf8_string_pair(stream)
    value, index2 = decode_utf8_string_pair(stream[index1:])
    return (key, value), index1 + index2

def encode(value, data_type):
    if not encode_map.has_key(data_type):
        raise Exception('Unknown data type')
    return encode_map[data_type](value)

def encode_byte(value):
    check_number(value)
    return struct.pack('B', value)

def encode_two_byte_int(value):
    check_number(value)
    return struct.pack(">H", value)

def encode_four_byte_int(value):
    check_number(value)
    return struct.pack(">L", value)

def encode_variable_byte_int(value):
    check_number(value)
    stream = b""
    while value > 0:
        byte = value % 0x80
        value = value / 0x80
        if value > 0:
            byte |= 0x80
        stream += encode_byte(byte)
    return stream

def encode_binary_data(value):
    check_bytes(value)
    length = encode_two_byte_int(len(value))
    return length + value

def encode_utf8_encoded_string(value):
    check_string(value)
    return encode_binary_data(value.encode("utf-8"))

def encode_utf8_string_pair(value):
    check_tuple(value)
    if len(value) < 2:
        raise Exception("Parameter tuple is shorter than 2")
    string1 = encode_utf8_string_pair(value[0])
    string2 = encode_utf8_string_pair(value[1])
    return string1 + string2

