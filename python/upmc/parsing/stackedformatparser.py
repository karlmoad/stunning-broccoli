import json
import defaultparser

class stackedformatparser(defaultparser):

    def __init__(self):
        defaultparser.__init__(self)

    def parse(self, line, params):
        if self.schema == null:
            raise Exception("Schema not set")

        spec = self.schema.getParsingSpecification()
        fldm = int(spec["field_id_size"])
        lenm = int(spec["field_length_size"])
        startChar = spec["record_start_char"]
        endChar = spec["record_end_char"]
        numFields = int(spec["field_count"])

        o = list()
        # parse the line into fields
        if not line[0:1] == startChar:
            raise Exception("Not properly Formatted")
        p = 1

        fields = dict()
        while True:
            if line[p:p + 1] == endChar:
                break
        field_id = line[p:p + fldm]
        p += fldm
        field_len = line[p:p + lenm]
        p += lenm
        fl = int(field_len)
        field_val = line[p:p + fl]
        p += fl
        fields[field_id] = field_val

        # apply schemas
        for f in self.schema.getFieldSpecification():
            if f.get("parsing"):
                item = f["parsing"]
                if item.get("param"):
                    o.append(self.schema.formatter(f, params[f["param"]]))
                elif item.get("id"):
                    o.append(self.schema.formatter(f, fields[f["field_id"]]))

        return tuple(o)
