import sys
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

APP_NAME = "NHL_SEASON_DATA_TRANSFORMER"
sc = None
sqlContext = None
schema = None
params = dict()

def main(args):
    # gather arg values
    schemaPath = args[1]
    season = args[2]
    inputFile = args[3]
    outputFile = args[4]


    # set params
    global params
    params["year"] = season

    # get the schema structure for the data
    global schema
    schema = getSchema(schemaPath)

    # load an initial RDD with the raw input file contents
    raw = sc.textFile(inputFile)
    parsed = raw.map(parser)

    # load data frame
    tblSeason = sqlContext.createDataFrame(parsed, SpkSchemaBuilder(schema))

    # create a temporary table could also save to hive metastore/hcatalog here
    tblSeason.registerTempTable("temp_season")

    #save the file to the ouput location
    tblSeason.write.mode("append").save(outputFile, format="parquet")


def parser(line):
    o = list()
    # parse the line into fields
    if not line[0:1] == "{":
        raise Exception("Not properly Formatted")
    p = 1
    fldm = 3
    lenm = 4

    fields = dict()
   	while True:
        if line[p:p+1] == "}":
                break
        field_id = line[p:p+fldm]
        p+=fldm
        field_len = line[p:p+lenm]
        p+=lenm
        fl = int(field_len)
        field_val = line[p:p+fl]
        p += fl
        fields[field_id]=field_val

    #apply schema
    for item in schema:
        if item.get("param"):
            o.append(formatter(item,params[item["param"]]))
        elif item.get("id"):
            o.append(formatter(item,fields[item["id"]]))

    return tuple(o)

def formatter(spec, value):
    if spec["dt"] =="int":
        return int(value)
    elif spec["dt"] =="float":
        return float(value)
    else:
        return value

def SpkSchemaBuilder(sch):
    fbuilder = list()
    for item in sch:
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


def getSchema(schemaPath):
    return json.loads(sc.wholeTextFiles(schemaPath).take(1)[0][1])

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf)
    sqlContext = SQLContext(sc)
    main(sys.argv)
