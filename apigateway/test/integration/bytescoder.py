import json

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return {'__bytes__': True, '__value__': obj.decode('utf-8')}
        return json.JSONEncoder.default(self, obj)

def as_bytes(dct):
    if '__bytes__' in dct:
        return dct['__value__'].encode('utf-8')
    return dct