import pickle
import codecs
import zlib


def serialize(obj):  # pragma: no cover
    return codecs.encode(zlib.compress(pickle.dumps(obj, protocol=4)), "base64").decode()


def deserialize(s):  # pragma: no cover
    return pickle.loads(zlib.decompress(codecs.decode(s.encode(), "base64")))
