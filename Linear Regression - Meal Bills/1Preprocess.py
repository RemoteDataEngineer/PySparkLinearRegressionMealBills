# Databricks notebook source
# MAGIC %md
# MAGIC ##Import libraries

# COMMAND ----------

print("0. Import libraries")

from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window

import random


# COMMAND ----------

# MAGIC %md
# MAGIC ##Read in dataframes

# COMMAND ----------

print("1. Read in tips.csv and display")
df_readin = spark.read.csv("/mnt/asdl2linearrg/tips/tips.csv", header=True, inferSchema=True)
display(df_readin)
df_readin.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # For Proof of Concept randomly generate total_bill, zip, sex, smoker, day, meal, size

# COMMAND ----------

print("2. For POC, randomly generate values for dataframe columns ")

# COMMAND ----------

print("# of rows read in")
row_count = df_readin.count()


print("create lists of categorical variables")
sex         = ["Female", "Male"]
smoker      = ["No", "Yes"]
daysofweek  = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
meal        = ["Breakfast", "Lunch", "Dinner"]


#you may want to set the random seed using random.seed() 

print("Generate a list of random values from the categories")
list_sex            = [random.choice(sex) for _ in range(row_count)]
list_smoker         = [random.choice(smoker) for _ in range(row_count)]
list_daysofweek     = [random.choice(daysofweek) for _ in range(row_count)]
list_meals          = [random.choice(meal) for _ in range(row_count)]
list_size           = [random.randint(1, 8) for _ in range(row_count)]


print("specify column names for dataframe of combined lists")
list_columns = ['sex', 'smoker', 'dow', 'meal', 'size'] 
  
print("creating a dataframe by zipping lists together")
df_combinelists = spark.createDataFrame( 
  zip(list_sex, list_smoker, list_daysofweek   , list_meals , list_size), list_columns) 

print("add a row_number for a common field to avoid cartesian join")
w = Window().orderBy(F.lit('A'))
df_readin_id = df_readin.withColumn("row_n", F.row_number().over(w))
df_cmblst_id = df_combinelists.withColumn("row_num", F.row_number().over(w))

print("df_tips is created by joining on the read in tips.csv and a dataframe of randomly generated values as features")
df_tips = df_readin_id.join(df_cmblst_id,df_readin_id["row_n"] == df_cmblst_id["row_num"])

df_tips = df_tips.drop("row_n")
display(df_tips)



# COMMAND ----------

#Write new tips dataframe to parquet
df_tips.write.mode("overwrite").parquet("/mnt/asdl2linearrg/tips/processed_tip_file.parquet")
