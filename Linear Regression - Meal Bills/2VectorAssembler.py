# Databricks notebook source
# MAGIC %md
# MAGIC ##Import libaries

# COMMAND ----------

print("0. Import libraries")
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Datframes

# COMMAND ----------

print("1. Read in tips.csv with other randomly generated values for ind features")
tips_df               = spark.read.parquet("/mnt/asdl2linearrg/tips/processed_tip_file.parquet")
display(tips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexer

# COMMAND ----------

print("2. Use StringIndexer to assign Integers to Categorical variables")
indexer = StringIndexer(inputCols = ["sex", "smoker", "dow", "meal"], outputCols=["sex_indexed", "smoker_indexed", "dow_indexed", "meal_indexed"])
df_r = indexer.fit(tips_df).transform(tips_df)
df_r.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Assembler

# COMMAND ----------

print("3. Use Feature Assembler to assemble all ind features into a vector")
featureassembler = VectorAssembler(inputCols = ['Tips', 'size', 'sex_indexed','smoker_indexed','dow_indexed','meal_indexed'], outputCol = "Ind Features")
output = featureassembler.transform(df_r)

display(output)
display(type(output))

# COMMAND ----------

#Write Vector Assembled dataframe to parquet
output.write.mode("overwrite").parquet("/mnt/asdl2linearrg/tips/vector_assembled_tip_file.parquet")
