import json
import os
import os.path
from pyspark.sql.types import *

schemaRaw = sc.wholeTextFiles(schemaFile)
schema = json.loads(schemaRaw.take(1)[0][1])
params = {"year":"2017"}

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
