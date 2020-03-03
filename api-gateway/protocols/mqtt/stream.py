import datatypes
import properties

from properties import PropertiesException

class MQTTStream():
	def __init__(self):
		self.buffer = b""
		self.header = None
		self.size = None
		self.loading = False

	def load(self, data):
		if type(data) is not bytes:
			raise Exception("Argument is not a bytes type")

		if not self.loading and len(self.buffer) != 0:
			raise Exception("Cannot load new packet, old packet still in use")
		self.loading = True

		self.buffer += data

		try:
			if self.header is None:
				self.header, index = datatypes.decode_byte(self.buffer)
				self.buffer = self.buffer[index:]
            if self.size is None:
                self.size, index = datatypes.decode_variable_byte_int(self.buffer)
                self.buffer = self.buffer[index:]
	    except datatypes.StreamLengthException:
	    	return 

        if len(self.buffer) > self.size:
            raise Exception('More than one packet')

        if len(self.buffer) == self.size:
        	self.stream = datatypes.encode_byte(self.header) +
        	 datatypes.encode_variable_byte_int(self.size) + self.stream
        	self.header = None
        	self.size = None
        	self.loading = False

    def dump(self):
    	buf = self.buffer
    	self.buffer = b""
    	return buf

    def append(self, value):
    	if type(value) is not bytes:
    		raise Exception("Parameter is not a byte string")
    	self.buffer += value

    def get_byte(self):
    	value, index = datatypes.decode_byte(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value

    def get_int(self):
    	value, index = datatypes.decode_two_byte_int(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value

    def get_long(self):
    	value, index = datatypes.decode_four_byte_int(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value

    def get_var_int(self):
    	value, index = datatypes.decode_variable_byte_int(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value

    def get_binary(self):
    	value, index = datatypes.decode_binary_data(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value 

    def get_string(self):
    	value, index = datatypes.decode_utf8_encoded_string(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value 

    def get_string_pair(self):
    	value, index = datatypes.decode_utf8_string_pair(self.buffer)
    	self.buffer = self.buffer[index:]
    	return value 

   	def get_header(self):
   		header = self.get_byte()
   		packet_type = header >> 4
   		dup = header & 0x08 != 0
   		qos = (header >> 1) & 0x03
   		retain = header & 0x01 != 0
   		return packet_type, dup, qos, retain

   	def get_connect_flags(self):
   		flags = self.get_byte()
   		reserved = flags & 0x01 != 0
   		clean_start = flags & 0x02 != 0
   		will_flag = flags & 0x04 != 0
   		will_qos = (header >> 3) & 0x03
   		will_retain = flags & 0x20 != 0
   		password_flag = flags & 0x40 != 0
   		username_flag = flags & 0x80 != 0
   		return username_flag, password_flag, 
   		 will_retain, will_qos, will_flag, clean_start, reserved

   	def get_connack_flags(self):
   		flags = self.get_byte()
   		session_present = flags & 0x01 != 0
   		reserved = flags & 0xFE
   		return session_present, reserved

    def get_sub_flags(self):
    	flags = self.get_byte()
    	max_qos = flags & 0x03
        no_local = flags & 0x04 != 0
        retain_as_published = flags & 0x08 != 0
        retain_handling = (flags >> 4) & 0x03
        reserved =  flags & 0xC0
        return retain_handling, retain_as_published, no_local, max_qos, reserved

   	def get_properties(self, packet_type):
   		props = list()
   		length = self.get_var_int()
        while length > 0:
            code, code_index = datatypes.decode_variable_byte_int(self.buffer)
            self.buffer = self.buffer[code_index:]
            if not properties.dictionary.has_key(code):
                raise PropertiesException('Unexisting Property Code')
            prop_type = dictionary[code]['type']
            value, index = datatypes.decode(self.buffer, prop_type)
            self.buffer = self.buffer[index:]
            index += code_index
            if index > length:
            	raise PropertiesException('Wrong Property Length')
            length -= index
            props.append((code, value))
        return properties.unpack(props, packet_type)

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

    def put_header(self, packet_type, dup=False, qos=0, retain=False):
    	if not isinstance(packet_type, (int, long)):
    		raise Exception("Packet type is not a number")
    	if not isinstance(qos, (int, long)):
    		raise Exception("QoS is not a number")
    	if type(dup) is not bool:
    		raise Exception("Duplication flag is not a boolean")
    	if type(retain) is not bool:
    		raise Exception("Retain flag is not a boolean")
    	byte = packet_type << 4
        byte |= qos << 1
        if dup:
            byte |= 0x08
        if retain:
            byte |= 0x01

        content = self.dump()
        self.put_byte(byte)
        self.put_var_int(len(content))
        self.append(content)

    def put_connect_flags(self, username_flag, password_flag,
     will_retain, will_qos, will_flag, clean_start):
    	if not isinstance(will_qos, (int, long)):
    		raise Exception("QoS is not a number")
    	if type(username_flag) is not bool:
    		raise Exception("Duplication flag is not a boolean")
    	if type(password_flag) is not bool:
    		raise Exception("Retain flag is not a boolean")
    	if type(will_retain) is not bool:
    		raise Exception("Duplication flag is not a boolean")
    	if type(will_flag) is not bool:
    		raise Exception("Retain flag is not a boolean")
    	if type(clean_start) is not bool:
    		raise Exception("Retain flag is not a boolean")
    	byte = qos << 3
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

    def put_connack_flags(self, session_present):
    	if type(session_present) is not bool:
    		raise Exception("Session Present is not a boolean")
    	byte = 0
    	if session_present:
    		byte |= 0x01
    	self.put_byte(byte)

    def put_sub_flags(self, retain_handling, retain_as_published, no_local, max_qos):
    	if not isinstance(max_qos, (int, long)):
    		raise Exception("Max QoS is not a number")
    	if not isinstance(retain_handling, (int, long)):
    		raise Exception("Retain Handling is not a number")
    	if type(retain_as_published) is not bool:
    		raise Exception("Retain as Published flag is not a boolean")
    	if type(no_local) is not bool:
    		raise Exception("No local flag is not a boolean")
    	byte =  max_qos
    	byte |= retain_handling << 4
    	if no_local:
    		byte |= 0x04
    	if retain_as_published:
    		byte |= 0x08
    	self.put_byte(byte)

    def put_properties(self, props):
    	if type(props) is not dict:
    		raise PropertiesException("Parameter for properties is not a dictionary")
    	props = properties.pack(props)
    	stream = b""
    	for prop in props:
    		if type(prop) is not tuple or len(prop) < 2:
    			raise PropertiesException("Parameter element is wrong type (2-sized tuple)")
    		code, value = prop[0], prop[1]
    		if not properties.dictionary.has_key(code):
                raise PropertiesException('Unexisting Property Code')
            stream += datatypes.encode_byte(code)
            stream += datatypes.encode(value, properties.dictionary[code]['type'])
	    length = datatypes.encode_variable_byte_int(len(stream))
	    self.buffer += length + stream

    def is_loading(self):
    	return self.loading

    def output(self, size):
    	return self.stream[:size]

    def update(self, size):
    	self.stream = self.stream[:size]

   	def empty(self):
   		return len(self.stream) == 0





		