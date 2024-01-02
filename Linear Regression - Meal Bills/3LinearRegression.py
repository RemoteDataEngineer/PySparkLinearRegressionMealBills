# Databricks notebook source
# MAGIC %md
# MAGIC ##Import Libraries

# COMMAND ----------

print("0. Import libraries")
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read in 

# COMMAND ----------

print("1. Read in vector assembled tips file")
tips_df               = spark.read.parquet("/mnt/asdl2linearrg/tips/vector_assembled_tip_file.parquet")
display(tips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select features

# COMMAND ----------

print("2. Select the ind features and the actual outcome")
finalized_data=tips_df.select("Ind Features", "Bill")
display(finalized_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split into train and test

# COMMAND ----------

print("3. use randomSplit to get train, test split")

train_data, test_data = finalized_data.randomSplit([0.75,0.25])

print("train data row count")
print(train_data.count())

print("test data row count")
print(test_data.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use Train Data to get Linear Regression Model

# COMMAND ----------

print("4. Create a linear regression model with train data")

regressor=LinearRegression(featuresCol = 'Ind Features', labelCol = "Bill")
regressor=regressor.fit(train_data)

display(type(regressor))

# COMMAND ----------

print("5. display results of Linear Regression")

# COMMAND ----------

print("Coefficients:")
regressor.coefficients

# COMMAND ----------

print("Intercept:")
regressor.intercept

# COMMAND ----------

# MAGIC %md
# MAGIC ##Predictions from Test Dataset

# COMMAND ----------

print("6. Generate predictions using Test Data")
pred_results = regressor.evaluate(test_data)

# COMMAND ----------

print("7. Print out Prediction Results")

# COMMAND ----------

print("7a. Prediction compared to Actual")

# COMMAND ----------

pred_results.predictions.show()

# COMMAND ----------

print("7b. R Squared ")

# COMMAND ----------

print(pred_results.r2)

# COMMAND ----------

print("7b. Mean Absolute Error ")

# COMMAND ----------

print(pred_results.meanAbsoluteError)

# COMMAND ----------

print("7c. Mean Squared Error ")

# COMMAND ----------

 print(pred_results.meanSquaredError)
