import json

class defaultparser(object):

    def __init__(self):
        self.schema = null

    def setSchema(self, schema):
        self.schema = schema

    def parse(self, line, params):
        o = list()
        o.append(line)
        return tuple(o)

    def formatter(self, spec, value):
        if spec["dt"] == "int":
            return int(value)
        elif spec["dt"] == "float":
            return float(value)
        else:
            return value



