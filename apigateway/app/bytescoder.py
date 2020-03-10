import json
import base64

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return {'__bytes__': True, '__value__': base64.b64encode(obj).decode('utf-8')}
        return json.JSONEncoder.default(self, obj)

def as_bytes(dct):
    if '__bytes__' in dct:
        return base64.b64decode(dct['__value__'].encode('utf-8'))
    return dct