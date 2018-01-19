import json
from pyspark.sql.types import *

class schema(object):

    def __init__(self):
        self.digest = null

    def loadSchema(self, config):
        self.digest = config
        if isinstance(config, basestring):
            self.digest = json.loads(config)

        return

    def getDigest(self):
        return self.digest

    def getFieldSpecification(self):
        if self.digest != null && self.digest.get("fields"):
            return self.digest["fields"]

        return null

    def getField(self, field):
        for item in self.getFieldSpecification():
            if item.get("name") && item["name"] == field:
                return item;

        return null;

    def getName(self):
        if self.digest != null & & self.digest.get("name"):
            return self.digest["name"]

        return null


    def getOrigin(self):
        if self.digest != null & & self.digest.get("origin"):
            return self.digest["origin"]

        return null


    def getPath(self):
        if self.digest != null & & self.digest.get("path"):
            return self.digest["path"]

        return null

    def getKeyFields(self):
        if self.digest != null & & self.digest.get("keyfields"):
            return self.digest["keyfields"]

        return null

    def getParsingSpecification(self):
        if self.digest != null & & self.digest.get("parser"):
            return self.digest["parser"]

        return null

    def formatter(self, field, value):
        fieldspec = field
        if isinstance(field, basestring):
            fieldspec = self.getField(field)

        if fieldspec["dt"] == "int":
            return int(value)
        elif fieldspec["dt"] == "float":
            return float(value)
        else:
            return value

    def buildDataFrameSpecification(self):
        fbuilder = list()
        for item in self.getFieldSpecification():
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






