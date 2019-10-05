import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import requests, zipfile, io, os
import pandas as pd 
from pyspark import SparkContext, SparkConf
import glob

spark = SparkSession.builder.appName("NFIP Dataset").getOrCreate()

path = 'C:\\Users\\Jared\\OneDrive\\Documents\\GitHub\\nfip_data_prep'
files = [f for f in glob.glob(path + "**/*.csv", recursive=True)]


policies_path = [filename for filename in files if not filename.endswith("openFEMA_claims20190531.csv")]
df_policies_1 = spark.read.format("csv").option("header","true").option("mode", "DROPMALFORMED").load(policies_path[0])

for policies in policies_path[1:]:
    df_policies = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(policies) 
    df_policies_1.union(df_policies)


df_policies_1_out = df_policies_1.filter(df_policies_1.propertystate == "NY")
df_policies_1_out.toPandas().to_csv('policies.csv')
