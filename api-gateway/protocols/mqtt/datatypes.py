import struct

BYTE = 1
TWO_BYTE_INT = 2
FOUR_BYTE_INT = 3
UTF8_ENCODED_STRING = 4
VARIABLE_BYTE_INT = 5
BINARY_DATA = 6
UTF8_STRING_PAIR = 7

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

def decode(stream, data_type):
    if not decode_map.has_key(data_type):
        raise Exception('Unvalid data type')
    return decode_map[data_type](stream)

def decode_byte(stream):
    if len(stream) < 1:
        raise Exception('Malformed Variable Byte Integer')
    value = struct.unpack(">B", stream[:1])[0]
    return value, 1

def decode_two_byte_int(stream):
    if len(stream) < 2:
        raise Exception('Malformed Variable Byte Integer')
    value = struct.unpack(">H", stream[:2])[0]
    return value, 2

def decode_four_byte_int(stream):
    if len(stream) < 4:
        raise Exception('Malformed Variable Byte Integer')
    value = struct.unpack(">L", stream[:4])[0]
    return value, 4

def decode_utf8_encoded_string(stream):
    string_length, index = decode_two_byte_int(stream)
    if len(stream) < index + string_length:
        raise Exception('Malformed Variable Byte Integer')
    value = stream[index:(string_length+index)].decode("utf-8")
    return value, string_length + index

def decode_variable_byte_int(stream):
    multiplier = 1
    value = 0
    index = 0
    while len(stream) >= index + 1:
        encoded_byte = struct.unpack(">B", stream[index:index+1])[0]
        value += (encoded_byte & 0x7F) * multiplier
        if multiplier > 0x80**3:
            raise Exception('Malformed Variable Byte Integer')
        multiplier *= 0x80
        index += 1
        if encoded_byte & 0x80 == 0:
            return value, index
    return -1, -1

def decode_binary_data(stream):
    data_length, index = decode_two_byte_int(stream)
    if len(stream) < index + string_length:
        raise Exception('Malformed Variable Byte Integer')
    value = stream[index:(string_length+index)]
    return value, string_length + index

def decode_utf8_string_pair(stream):
    key, index1 = decode_utf8_string_pair(stream)
    value, index2 = decode_utf8_string_pair(stream[index1:])
    return (key, value), index1 + index2

def encode(value, data_type):
    if not encode_map.has_key(data_type):
        raise Exception('Unknown data type')
    return encode_map[data_type](value)

def encode_byte(value):
    return struct.pack('B', value)

def encode_two_byte_int(value):
    return struct.pack(">H", value)

def encode_four_byte_int(value):
    return struct.pack(">H", value)

def encode_utf8_encoded_string(value):
    stream = value.encode("utf-8")
    string_length = encode_two_byte_int(len(stream))
    return string_length + stream

def encode_variable_byte_int(value):
    stream = b""
    while value > 0:
        byte = value % 0x80
        value = value / 0x80
        if value > 0:
            byte |= 0x80
        stream += encode_byte(byte)
    return stream

def encode_binary_data(value):
    data_length = encode_two_byte_int(len(value))
    return data_length + value

def encode_utf8_string_pair(value):
    string1 = encode_utf8_string_pair(value[0])
    string2 = encode_utf8_string_pair(value[1])
    return string1 + string2
