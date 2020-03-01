import datatypes

class MQTTStream():
	def __init__(self):
		self.buffer = b""
		self.header = None
		self.length = None

	def input(self, data):
		if type(data) is not bytes:
			raise Exception("Argument is not a bytes type")

		self.buffer += data

		try:
			if not self.header is None:
				self.header, index = datatypes.decode_byte(self.buffer)
				self.buffer = self.buffer[index:]
	        if self.header is not None:
	            if self.length is None:
	                self.length, index = datatypes.decode_variable_byte_int(self.buffer)
	                self.buffer = self.buffer[index:]
	    except datatypes.StreamLengthException:
	    	return False

        if self.length is not None:
	        if len(self.buffer) > self.length:
	            raise Exception('More than one packet')
            if len(self.buffer) == self.length:
        		return True

        return False

		