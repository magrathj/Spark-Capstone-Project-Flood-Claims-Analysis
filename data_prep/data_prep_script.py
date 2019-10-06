# import pyspark
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# import zipfile, io, os
# import pandas as pd 
# from pyspark import SparkContext, SparkConf
# import glob
# import sys

# def main(argv):
#     #path = sys.argv[1]
#     path = 'C:\\Users\\jmagr\\Downloads\\policy'
#     spark = SparkSession.builder.appName("NFIP Dataset").getOrCreate()
#     spark.sparkContext.getConf().getAll()
#     print(spark)


    
#     files = [f for f in glob.glob(path + "**/*.csv", recursive=True)]
#     for file in files:
#         print(file)

# ## Example: python get_data_script.py "C:/Users/jmagr/Downloads/policy/"
# if __name__ == "__main__":
#    main(sys.argv[1:])

import findspark

findspark.init("C:/Users/jmagr/OneDrive/Desktop/Spark/spark-2.4.4-bin-hadoop2.7")

import pyspark
sc = pyspark.SparkContext(appName="myAppName")

