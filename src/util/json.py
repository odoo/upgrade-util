__all__ = ["dumps", "loads", "load"]

try:
    import orjson
except ImportError:
    import json

    def dumps(value, sort_keys=False):
        return json.dumps(value, sort_keys=sort_keys)

    def loads(value):
        return json.loads(value)

    def load(fp):
        return json.load(fp)
else:

    def dumps(value, sort_keys=False):
        option = orjson.OPT_SORT_KEYS if sort_keys else None
        return orjson.dumps(value, option=option).decode()

    def loads(value):
        return orjson.loads(value)

    def load(fp):
        return orjson.loads(fp.read())
