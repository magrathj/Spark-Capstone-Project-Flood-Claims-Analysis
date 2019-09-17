# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
#%% [markdown]
# # Reading and Writing Data with Spark
# 
# This notebook contains the code from the previous screencast. The only difference is that instead of reading in a dataset from a remote cluster, the data set is read in from a local file. You can see the file by clicking on the "jupyter" icon and opening the folder titled "data".
# 
# Run the code cell to see how everything works. 
# 
# First let's import SparkConf and SparkSession

#%%
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import requests, zipfile, io, os
import pandas as pd 
from pyspark import SparkContext, SparkConf



#%% [markdown]
# Since we're using Spark locally we already have both a sparkcontext and a sparksession running. We can update some of the parameters, such our application's name. Let's just call it "Our first Python Spark SQL example"



#%%
spark = SparkSession     .builder     .appName("NFIP Dataset")     .getOrCreate()

#%% [markdown]
# Let's check if the change went through

#%%
spark.sparkContext.getConf().getAll()


#%%
spark


#%% [markdown]
# Let's download the datasets...this could take sometime

#%% 

all_zip_file_urls = [
                     'https://www.fema.gov/media-library-data/1566235780855-42d2142c3b1c2520a205192774268f84/openFEMA_policies20190531_1.zip',
                     'https://www.fema.gov/media-library-data/1566235780855-42d2142c3b1c2520a205192774268f84/openFEMA_policies20190531_2.zip',
                     'https://www.fema.gov/media-library-data/1566235780855-42d2142c3b1c2520a205192774268f84/openFEMA_policies20190531_3.zip',
                     'https://www.fema.gov/media-library-data/1566235431170-4120327dea121daff89b8ec3da22b832/openFEMA_policies20190531_4.zip',
                     'https://www.fema.gov/media-library-data/1566235780855-42d2142c3b1c2520a205192774268f84/openFEMA_policies20190531_5.zip'
                    ]

for zip_file_url in all_zip_file_urls:
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()



#%% [markdown]
# Lets grab all the files in the directory

#%% 

import glob

path = 'C:\\Users\\Jared\\OneDrive\\Documents\\GitHub\\nfip_data_prep'
files = [f for f in glob.glob(path + "**/*.csv", recursive=True)]

for f in files:
    print(f)




#%% [markdown]
# And just split them into claims and policy dataframes


#%% 

claims_path = [filename for filename in files if filename.endswith("openFEMA_claims20190531.csv")]
df_claims = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(claims_path[0])


#%%
df_claims.printSchema() 



#%% 

policies_path = [filename for filename in files if not filename.endswith("openFEMA_claims20190531.csv")]
df_policies_1 = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(policies_path[0])


for policies in policies_path[1:]:
    df_policies = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(policies) 
    df_policies_1.union(df_policies)


df_policies_1.printSchema() 


#%%
df_claims.describe().show()


#%%
df_policies_1.describe().show()


#%% [markdown]
# Now lets cut the policy and claims dataset down to just NY
# then lets write to disk



#%%
df_claims_out = df_claims.filter(df_claims.state == "NY").collect()
df_policies_1_out = df_policies_1.filter(df_policies_1.state == "NY").collect()

claims_out_path = "claims.csv"
policy_out_path = "policies.csv"

df_claims_out.write.save(claims_out_path, format="csv", header=True)
df_policies_1_out.write.save(policy_out_path, format="csv", header=True)




