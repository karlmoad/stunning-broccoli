import json
import os
import os.path
from pyspark.sql.types import *

input1 = "/tmp/Pittsburgh-Penguins-2015-all_skaters.mpac"
input2 = "/tmp/Pittsburgh-Penguins-2016-all_skaters.mpac"
input3 = "/tmp/Pittsburgh-Penguins-2017-all_skaters.mpac"

schemaRaw = sc.wholeTextFiles("/tmp/all_skaters.json")
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

        # apply schemas
    for f in schema["fields"]:
        if f.get("parsing"):
            item = f["parsing"]
            if item.get("field_id"):
                if fields.get(item["field_id"]):
                    o.append(formatter(f, fields[item["field_id"]]))
                else:
                    o.append('')
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
    for item in schema["fields"]:
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



rdd1 = sc.textFile(input1)
data1 = rdd1.map(parser)

rdd2 = sc.textFile(input2)
data2 = rdd2.map(parser)

rdd3 = sc.textFile(input3)
data3 = rdd3.map(parser)

skaters = sc.union([data1,data2,data3])

skaters_mapped = skaters.map(lambda x: (x[0],x))

skaters_grouped = skaters_mapped.groupByKey();

def evaluatePlayerStats(record):
    curGoals = 0
    curAssists = 0
    key = record[0]
    recs = list(record[1])
    out = list()

    for rec in recs:
        if len(rec) == 0:
            rGoals = int(rec[10])
            rAssists = int(rec[11])

            if rGoals > curGoals:
                curGoals = rGoals
                curAssists = rAssists
                out = rec
            elif rGoals <= curGoals and rAssists > curAssists:
                curGoals = rGoals
                curAssists = rAssists
                out = rec


    return tuple(out)

skaters_final = skaters_grouped.map(evaluatePlayerStats)
