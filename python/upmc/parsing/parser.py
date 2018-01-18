import json
import stackedformatparser
import defaultparser

class parser:

    def __init__(self, schema):
        self.config = schema
        self.parsertype = self.evaluate()

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
