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
  conf = SparkConf()  # yes thats it, easy huh

  # Set the app name and create a spark context for use
  conf.setAppName(APP_NAME) # See variable declared above
  sc = SparkContext(conf)

  # Create  SQLContext as well so at start we are basically at the same
  # point as the shell REPL
  sqlContext = SQLContext(sc)

  # setup complete call main program
  main(sys.argv)
