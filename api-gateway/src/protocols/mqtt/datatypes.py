import struct

def check_bytes(value):
    if not isinstance(value, bytes):
        raise Exception('Parameter is not a byte string')

def check_number(value):
    if not isinstance(value, int):
        raise Exception("Parameter is not a number")

def check_string(value):
    if not isinstance(value, str):
        raise Exception("Parameter is not a string")

def check_tuple(value):
    if not isinstance(value, tuple) or len(value) < 2:
        raise Exception("Parameter is not a 2-sized tuple")

#==========================++++++++++++++++++==========================
#                          +++++        +++++
#                          +++++ DECODE +++++
#                          +++++        +++++
#==========================++++++++++++++++++==========================

def decode_byte(stream):
    check_bytes(stream)
    if len(stream) < 1:
        return 0, -1
    value = struct.unpack(">B", stream[:1])[0]
    return value, 1

def decode_two_byte_int(stream):
    check_bytes(stream)
    if len(stream) < 2:
        return 0, -1
    value = struct.unpack(">H", stream[:2])[0]
    return value, 2

def decode_four_byte_int(stream):
    check_bytes(stream)
    if len(stream) < 4:
       return 0, -1
    value = struct.unpack(">L", stream[:4])[0]
    return value, 4

def decode_variable_byte_int(stream):
    multiplier = 1
    value = 0
    number_of_bytes = 0
    while True:
        byte, index = decode_byte(stream)
        if index < 0:
            return 0, -1
        stream = stream[index:]
        value += (byte & 0x7F) * multiplier
        if multiplier > 0x80**3:
            return value, 5
            raise Exception('Malformed Variable Byte Integer')
        multiplier *= 0x80
        number_of_bytes += 1
        if byte & 0x80 == 0:
            break
    return value, number_of_bytes

def decode_binary_data(stream):
    length, index = decode_two_byte_int(stream)
    if index < 0:
        return b"", -1
    stream = stream[index:]
    if len(stream) < length:
        return b"", -1
    value = stream[:length]
    return value, length + index

def decode_utf8_encoded_string(stream):
    value, index = decode_binary_data(stream)
    return value.decode("utf-8"), index

def decode_utf8_string_pair(stream):
    key, index1 = decode_utf8_string_pair(stream)
    value, index2 = decode_utf8_string_pair(stream[index1:])
    if index1 < 0 or index2 < 0:
        return ("", ""), -1
    return (key, value), index1 + index2

#==========================++++++++++++++++++==========================
#                          +++++        +++++
#                          +++++ ENCODE +++++
#                          +++++        +++++
#==========================++++++++++++++++++==========================

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
    while value > 0 or len(stream) == 0:
        byte = value % 0x80
        value /= 0x80
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
    string1 = encode_utf8_string_pair(value[0])
    string2 = encode_utf8_string_pair(value[1])
    return string1 + string2
