import json
import os
import os.path
from pyspark.sql.types import *

schemaRaw = sc.wholeTextFiles(schemaFile)
schema = json.loads(schemaRaw.take(1)[0][1])
params = {"year": "2017"}


def parser(line):
    o = list()

    spec = schema["parser"]
    fldm = int(spec["field_id_size"])
    lenm = int(spec["field_length_size"])
    startChar = spec["record_start_char"]
    endChar = spec["record_end_char"]
    numFields = int(spec["field_count"])

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

        # apply schema
    for f in schema:
        if f.get("parsing"):
            item = f["parsing"]
            if item.get("param"):
                o.append(self.formatter(item, params[item["param"]]))
            elif item.get("id"):
                o.append(self.formatter(item, fields[item["field_id"]]))
    return tuple(o)


def formatter(spec, value):
    if spec["dt"] == "int":
        return int(value)
    elif spec["dt"] == "float":
        return float(value)
    else:
        return value


def SpkSchemaBuilder(schema):
    fbuilder = list()
    for item in schema:
        # derive spk sql data type
        dt = None
        if item["dt"] == "int":
            dt = IntegerType()
        elif item["dt"] == "float":
            dt = FloatType()
        else:
            dt = StringType()

        # find if field is nullable
        nullable = item["nullable"] == "True"

        # build the spk sql field spec
        fbuilder.append(StructField(item["name"], dt, nullable))

    return StructType(fbuilder)



dataRaw = sc.textFile(inFile)
parsed = dataRaw.map(parser)

tblSeason = sqlContext.createDataFrame(parsed, SpkSchemaBuilder(schema))
tblSeason.registerTempTable("temp_season")
