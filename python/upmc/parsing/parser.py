import json
import stackedformatparser
import defaultparser
from schemas import schema

class parser:

    def __init__(self, schema):
        self.schemaName = schema.getName()
        self.config = schema
        self.parsertype = self.evaluate()
        self.parsertype.setSchema(self.config)

    def evaluate(self):
        parsers = {
            "STACKEDFIELDSPECIFICATION": stackedformatparser()
        }

        pType=""
        if self.schema.get("parsing") && self.schema["parsing"].get("type"):
            pType = self.schema["parsing"]["type"].upper()

        return parsers.get(pType, defaultparser())


    def parse(self, line, params):
        return self.parsertype.parse(line, params)
