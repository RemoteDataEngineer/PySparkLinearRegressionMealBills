# PySpark LinearRegression - Meal Bills
In Azure Databricks notebooks, perform Linear Regression on Categorical Features to Predict Total Bill

# Run this notebook - LinearRegression - Meal Bills to kick off:
## 0MountDatalake -
Mounts containers for storing processed files

## 1Preprocess -
Reads in tips.csv, a Kaggle file containing a listing of bill totals and tips. For this POC, randomly generate values for zip, sex, smoker, day, meal, size and save parquet file in container

## 2VectorAssembler -
Take pre-proccessed parquet file from container and use Feature Assembler to assemble all ind features into a vector

## 3LinearRegression -
Divide Vector Assembled dataset into test and train. Create a model from the train dataset and run it against Test to get predictions

