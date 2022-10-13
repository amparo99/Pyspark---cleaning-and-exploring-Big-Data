#!/usr/bin/env python
# coding: utf-8

# Task 1 - Install Spark, download datasets, create final dataframe.
# If you get an error regarding tar or wget, it is probably due to the Spark file being removed from the repository. Go to https://downloads.apache.org/spark/ and choose an equivalent version of Spark and Hadoop to download. So if 2.4.7 is not available, download the next version. At the time of this project creation, 2.4.7 exists.
# pip install -q findspark
# pip install pyspark
# download --> https://downloads.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz --> unzip
# brew install openjdk-8-jdk-headless -qq > /dev/null

import os
#os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk"
os.environ["SPARK_HOME"] = "/Users/amparoalias/Documents/spark-3.3.0-bin-hadoop3"

import findspark
findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import isnan, when, count, col, lit, trim, avg, ceil
from pyspark.sql.types import StringType
#import matplotlib.pyplot as plt
#import pandas as pd
#import seaborn as sns


sc = SparkSession.builder.master("local[*]").getOrCreate()

features = sc.read.csv('features.csv', inferSchema=True, header=True)
labels = sc.read.csv('labels.csv', inferSchema=True, header = True)

print("Rows of features: ", features.count())
print("Columns of features: ", features.columns)
print("Rows of labels: ", labels.count())
print("Columns of labels: ", labels.columns)

data = features.join(labels, on = ('id'))
print("Rows of data: ", data.count())
print("Columns of data: ", data.columns)

# Task 2 - Change column type, drop duplicated rows, remove whitespacs.
print(data.printSchema())
print(data.show(10))

## we need to change the variable type from integer to string of variables: region_code and district_code
data = data.withColumn("region_code", col("region_code").cast(StringType()))\
            .withColumn("district_code", col("district_code").cast(StringType()))

data = data.dropDuplicates(['id'])
print(data.count())

str_cols = [item[0] for item in data.dtypes if item[1].startswith("string")]
for col in str_cols:
    data = data.withColumn(col, trim(data[col]))

# Task 3 - Remove columns with null values more than a threshold.

# Task 4 - Group, aggregate, create pivot table.

# Task 5 - Convert categories with low frequency to Others, impute missing values.

# Task 6 - Make visualizations.

color_status = {'functional': 'green', 'non functional': 'red', 'functional needs repair': 'blue'}




