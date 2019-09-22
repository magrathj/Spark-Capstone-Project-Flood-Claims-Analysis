
#%%
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import requests, zipfile, io, os
import pandas as pd 
from pyspark import SparkContext, SparkConf


#%% [markdown]
#  Since we're using Spark locally we already have both a sparkcontext and a sparksession running. We can update some of the parameters, such our application's name. Let's just call it "Our first Python Spark SQL example"

#%%
spark = SparkSession     .builder     .appName("NFIP Dataset")     .getOrCreate()

#%% [markdown]
#  Let's check if the change went through

#%%
spark.sparkContext.getConf().getAll()


#%%
spark

#%% [markdown]
#  reading in analysis




#%%
df_claims = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load("claims.csv")
df_claims.take(5) 

#%%
df_policies = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load("policies.csv")
df_policies.take(5) 



#%%
