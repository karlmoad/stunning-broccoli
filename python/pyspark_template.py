import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

APP_NAME="Karls Pyspark Template <CHANGE THIS>"
sc = None
sqlContext = None

def main(args):

  # Your program code goes here







if __name__ == "__main__":
  # Configure Spark context form environment settings
  conf = SparkConf().setAppName(APP_NAME)  # yes thats it, easy huh

  # create a spark context for use
  sc = SparkContext(conf=conf)

  # Create  SQLContext as well so at start we are basically at the same
  # point as the shell REPL
  sqlContext = SQLContext(sc)

  # setup complete call main program
  main(sys.argv)
