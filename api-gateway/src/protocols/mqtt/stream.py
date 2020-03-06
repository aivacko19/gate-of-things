import logging
from . import datatypes
from . import const

class PropertiesError(Exception):
    def __init__(self, message):
        super().__init__(message)

class ParameterError(Exception):
    def __init__(self, message):
        super().__init__(message)

class OutOfBoundsError(Exception):
    def __init__(self):
        super().__init__("Operation causes stream to go out of bounds")

class MalformedVariableIntegerError(Exception):
    def __init__(self):
        super().__init__("Variable Byte Integer is malformed")

class Stream():
    def __init__(self):
        self.buffer = b""
        self.header = -1
        self.size = -1
        self.loading = False

    def load(self, data):
        if not isinstance(data, bytes):
            raise ParameterError("Argument is not a bytes type")
        if not self.loading and self.buffer:
            raise Exception("Cannot load new packet, old packet still in use")
        self.loading = True

        self.buffer += data
        if self.header < 0:
            value, index = datatypes.decode_byte(self.buffer)
            if index < 0:
                return
            self.buffer = self.buffer[index:]
            self.header = value
        if self.size < 0:
            value, index = datatypes.decode_variable_byte_int(self.buffer)
            if index < 0:
                return
            self.buffer = self.buffer[index:]
            self.size = value

        if len(self.buffer) > self.size:
            raise OverflowError('More than one packet')
        if len(self.buffer) == self.size:
            self.buffer = (datatypes.encode_byte(self.header)
                           + datatypes.encode_variable_byte_int(self.size)
                           + self.buffer)
            self.header = -1
            self.size = -1
            self.loading = False

    def is_loading(self):
        return self.loading

    def output(self, size):
        return self.buffer[:size]

    def update(self, size):
        self.buffer = self.buffer[size:]

    def empty(self):
        return not self.buffer

#==========================++++++++++++++++==========================
#                          +++++      +++++
#                          +++++ READ +++++
#                          +++++      +++++
#==========================++++++++++++++++==========================

    def get_byte(self):
        value, index = datatypes.decode_byte(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value

    def get_int(self):
        value, index = datatypes.decode_two_byte_int(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value

    def get_long(self):
        value, index = datatypes.decode_four_byte_int(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value

    def get_var_int(self):
        value, index = datatypes.decode_variable_byte_int(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        if index > 4:
            raise MalformedVariableIntegerError()
        self.buffer = self.buffer[index:]
        return value

    def get_binary(self):
        value, index = datatypes.decode_binary_data(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value 

    def get_string(self):
        value, index = datatypes.decode_utf8_encoded_string(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value 

    def get_string_pair(self):
        value, index = datatypes.decode_utf8_string_pair(self.buffer)
        if index < 0:
            raise OutOfBoundsError()
        self.buffer = self.buffer[index:]
        return value 

    def get_header(self):
        header = self.get_byte()
        packet_type = header & 0xF0
        dup = header & 0x08 != 0
        qos = (header >> 1) & 0x03
        retain = header & 0x01 != 0
        return (packet_type,
                dup,
                qos,
                retain,
                )

    def get_connect_flags(self):
        flags = self.get_byte()
        reserved = flags & 0x01 != 0
        clean_start = flags & 0x02 != 0
        will_flag = flags & 0x04 != 0
        will_qos = (flags >> 3) & 0x03
        will_retain = flags & 0x20 != 0
        password_flag = flags & 0x40 != 0
        username_flag = flags & 0x80 != 0
        return (username_flag, 
                password_flag,
                will_retain, 
                will_qos, 
                will_flag, 
                clean_start, 
                reserved,
                )

    def get_connack_flags(self):
        flags = self.get_byte()
        session_present = flags & 0x01 != 0
        reserved = flags & 0xFE != 0
        return (session_present,
                reserved,
                )

    def get_sub_flags(self):
        flags = self.get_byte()
        max_qos = flags & 0x03
        no_local = flags & 0x04 != 0
        retain_as_published = flags & 0x08 != 0
        retain_handling = (flags >> 4) & 0x03
        reserved =  flags & 0xC0 != 0
        return (retain_handling,
                retain_as_published,
                no_local,
                max_qos,
                reserved,
                )

    def get_properties(self):
        properties = list()
        length = self.get_var_int()
        while length > 0:
            code, index = datatypes.decode_variable_byte_int(self.buffer)
            if index > length:
                raise PropertiesError('Malformed Properties Length')
            length -= index
            self.buffer = self.buffer[index:]
            if code not in const.DICT:
                raise PropertiesError(f'Property not supported: {hex(code)}')
            data_type = const.DICT[code]['type']
            value, index = None, -1
            if data_type == const.BYTE:
                value, index = datatypes.decode_byte(self.buffer)
            elif data_type == const.TWO_BYTE_INT:
                value, index = datatypes.decode_two_byte_int(self.buffer)
            elif data_type == const.FOUR_BYTE_INT:
                value, index = datatypes.decode_four_byte_int(self.buffer)
            elif data_type == const.VARIABLE_BYTE_INT:
                value, index = datatypes.decode_variable_byte_int(self.buffer)
            elif data_type == const.BINARY_DATA:
                value, index = datatypes.decode_binary_data(self.buffer)
            elif data_type == const.UTF8_ENCODED_STRING:
                value, index = datatypes.decode_utf8_encoded_string(self.buffer)
            elif data_type == const.UTF8_STRING_PAIR:
                value, index = datatypes.decode_utf8_string_pair(self.buffer)
            else:
                raise PropertiesError('Data type not supported')
            if index < 0:
                raise OutOfBoundsError()
            if index > length:
                raise PropertiesError('Malformed Properties Length')
            length -= index
            self.buffer = self.buffer[index:]
            properties.append((code, value))
        return properties

    def dump(self):
        buf = self.buffer
        self.buffer = b""
        return buf

#==========================+++++++++++++++++==========================
#                          +++++       +++++
#                          +++++ WRITE +++++
#                          +++++       +++++
#==========================+++++++++++++++++==========================

    def put_byte(self, value):
        self.buffer += datatypes.encode_byte(value)

    def put_int(self, value):
        self.buffer += datatypes.encode_two_byte_int(value)

    def put_long(self, value):
        self.buffer += datatypes.encode_four_byte_int(value)

    def put_var_int(self, value):
        self.buffer += datatypes.encode_variable_byte_int(value)

    def put_binary(self, value):
        self.buffer += datatypes.encode_binary_data(value)

    def put_string(self, value):
        self.buffer += datatypes.encode_utf8_encoded_string(value)

    def put_string_pair(self, value):
        self.buffer += datatypes.encode_utf8_string_pair(value)

    def put_header(self, 
                   packet_type,
                   dup=False,
                   qos=0,
                   retain=False,
                   ):
        if not isinstance(packet_type, int):
            raise ParameterError("Packet type is not a number")
        if not isinstance(qos, int):
            raise ParameterError("QoS is not a number")
        if not isinstance(dup, bool):
            raise ParameterError("Duplication flag is not a boolean")
        if not isinstance(retain, bool):
            raise ParameterError("Retain flag is not a boolean")
        byte = packet_type
        byte |= qos << 1
        if dup:
            byte |= 0x08
        if retain:
            byte |= 0x01

        content = self.dump()
        self.put_byte(byte)
        self.put_var_int(len(content))
        self.append(content)

    def put_connect_flags(self,
                          username_flag,
                          password_flag,
                          will_retain,
                          will_qos,
                          will_flag,
                          clean_start,
                          ):
        if not isinstance(will_qos, int):
            raise ParameterError("QoS is not a number")
        if not isinstance(username_flag, bool):
            raise ParameterError("Duplication flag is not a boolean")
        if not isinstance(password_flag, bool):
            raise ParameterError("Retain flag is not a boolean")
        if not isinstance(will_retain, bool):
            raise ParameterError("Duplication flag is not a boolean")
        if not isinstance(will_flag, bool):
            raise ParameterError("Retain flag is not a boolean")
        if not isinstance(clean_start, bool):
            raise ParameterError("Retain flag is not a boolean")
        byte = will_qos << 3
        if username_flag:
            byte |= 0x80
        if password_flag:
            byte |= 0x40
        if will_retain:
            byte |= 0x20
        if will_flag:
            byte |= 0x04
        if clean_start:
            byte |= 0x02
        self.put_byte(byte)

    def put_connack_flags(self,
                          session_present,
                          ):
        if not isinstance(session_present, bool):
            raise ParameterError("Session Present is not a boolean")
        byte = 0
        if session_present:
            byte |= 0x01
        self.put_byte(byte)

    def put_sub_flags(self,
                      retain_handling,
                      retain_as_published,
                      no_local,
                      max_qos,
                      ):
        if not isinstance(max_qos, int):
            raise ParameterError("Max QoS is not a number")
        if not isinstance(retain_handling, int):
            raise ParameterError("Retain Handling is not a number")
        if not isinstance(retain_as_published, bool):
            raise ParameterError("Retain as Published flag is not a boolean")
        if not isinstance(no_local, bool):
            raise ParameterError("No local flag is not a boolean")
        byte =  max_qos
        byte |= retain_handling << 4
        if no_local:
            byte |= 0x04
        if retain_as_published:
            byte |= 0x08
        self.put_byte(byte)

    def put_properties(self, properties):
        if not isinstance(properties, list):
            raise ParameterError("Parameter for properties is not a list")
        properties_buffer = b""
        for prop in properties:
            if not isinstance(prop, tuple) or len(prop) < 2:
                raise ParameterError("Parameter element is wrong type (2-sized tuple)")

            code, value = prop[0], prop[1]
            if code not in const.DICT:
                raise PropertiesError(f'Property not supported: {hex(code)}')
            properties_buffer += datatypes.encode_byte(code)
            data_type = const.DICT[code]['type']
            if data_type == const.BYTE:
                properties_buffer += datatypes.encode_byte(value)
            elif data_type == const.TWO_BYTE_INT:
                properties_buffer += datatypes.encode_two_byte_int(value)
            elif data_type == const.FOUR_BYTE_INT:
                properties_buffer += datatypes.encode_four_byte_int(value)
            elif data_type == const.VARIABLE_BYTE_INT:
                properties_buffer += datatypes.encode_variable_byte_int(value)
            elif data_type == const.BINARY_DATA:
                properties_buffer += datatypes.encode_binary_data(value)
            elif data_type == const.UTF8_ENCODED_STRING:
                properties_buffer += datatypes.encode_utf8_encoded_string(value)
            elif data_type == const.UTF8_STRING_PAIR:
                properties_buffer += datatypes.encode_utf8_string_pair(value)
            else:
                raise PropertiesError('Data type not supported')

        length = datatypes.encode_variable_byte_int(len(properties_buffer))
        self.buffer += length + properties_buffer

    def append(self, value):
        if not isinstance(value, bytes):
            raise ParameterError("Parameter is not a byte string")
        self.buffer += value