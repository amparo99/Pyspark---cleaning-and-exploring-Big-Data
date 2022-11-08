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
from pyspark.sql.functions import isnan, when, count, col, trim, lit, avg, ceil
from pyspark.sql.types import StringType
import matplotlib.pyplot as plt
import seaborn as sns

sc = SparkSession.builder.master("local[*]").getOrCreate()

features = sc.read.csv('features.csv', inferSchema=True, header=True)
labels = sc.read.csv('labels.csv', inferSchema=True, header = True)

print("\nRows of features: ", features.count())
print("Number of columns of features: ", len(features.columns))
print("Rows of labels: ", labels.count())
print("Number of columns of labels: ", len(labels.columns))

data = features.join(labels, on = ('id'))
print("\nRows of final_data: ", data.count())
print("Number of columns of final_data: ", len(data.columns))

# Task 2 - Change column type, drop duplicated rows, remove whitespacs.
print("\n\nData schema:")
data.printSchema()
print("\n\nFirst ten rows of data: \n")
data.show(10)

## we need to change the variable type from integer to string of variables: region_code and district_code
data = data.withColumn("region_code", col("region_code").cast(StringType()))\
            .withColumn("district_code", col("district_code").cast(StringType()))

print("\n\nData count *before* dropping duplicates: ",data.count())
data = data.dropDuplicates(['id'])
print("Data count *after* dropping duplicates: ", data.count())

str_cols = [item[0] for item in data.dtypes if item[1].startswith("string")]
for column in str_cols:
    data = data.withColumn(column, trim(data[column]))

# Task 3 - Remove columns with null values more than a threshold.
aggrow = data.select([(count(when(isnan(c) | col(c).isNull(), c))/data.count()).alias(c) for c in data.columns if c not in {'date_recorded', 'public_meeting', 'permit'}]).collect()

agg_dict_list = [row.asDict() for row in aggrow]
agg_dict = agg_dict_list[0]
print("\n\nDictionary with percentage of nulls per column: \n", agg_dict)

col_null = list({i for i in agg_dict if agg_dict[i]>0.4})
print("Cols with more than 40% of nulls: ", col_null)
data = data.drop(*col_null)


# Task 4 - Group, aggregate, create pivot table.
agg_data = data.groupBy('water_quality').count().orderBy('count', ascending = False)
print("\nAggregated data by water_quality: ")
agg_data.show()

pivot_data = data.drop('recorded_by').groupBy('status_group').pivot('region').sum('amount_tsh')
print("Pivoted data on region, grouped by status_group, suming amount_tsh:")
pivot_data.show()

# Task 5 - Convert categories with low frequency to Others, impute missing values.
print("The string columns we have are: \n", str_cols)
column = str_cols[0]
print("* Column - ", column)
# print(data.groupBy(column).count().orderBy('count', ascending = False).show())
print("Before: ", data.select(column).distinct().count())
values_cat = data.groupBy(column).count().collect()
lessthan = [x[0] for x in values_cat if x[1]<1000]
data = data.withColumn(column, when(col(column).isin(lessthan), 'Others').otherwise(col(column)))
print("After: ", data.select(column).distinct().count())
# print(data.groupBy(column).count().orderBy('count', ascending = False).show())


print("\nBefore filling nulls with mean: \n")
data.groupBy('population').count().orderBy('population').show(10)
data = data.withColumn('population', when(col('population') < 2, lit(None)).otherwise(col('population')))
w = Window.partitionBy(data['district_code'])
data = data.withColumn('population', when(col('population').isNull(), avg(data['population']).over(w)).otherwise(col('population')))
print("\nAfter filling nulls with mean: \n")
data.groupBy('population').count().orderBy('population').show(10) ####REVISAR PORQUÉ HAY NULOS..


# Task 6 - Make visualizations.
color_status = {'functional': 'green', 'non functional': 'red', 'functional needs repair': 'blue'}
cols = ['status_group', 'payment_type', 'longitude', 'latitude', 'gps_height']
df = data.select(cols).toPandas()

fig1, ax1 = plt.subplots(figsize = (12,8))
sns.countplot(x = 'payment_type',
              hue = 'status_group',
              data = df,
              ax = ax1,
              palette = color_status)
plt.xticks(rotation = 45)
plt.title('amount of payment type')
plt.show()

fig2, ax2 = plt.subplots(figsize = (12,8))
sns.scatterplot(x = 'longitude',
                y = 'latitude',
                data = df,
                hue = 'status_group',
                ax = ax2,
                palette = color_status)
plt.title('geographic distribution by status')
plt.show()

row_functional = (df['status_group'] == 'functional')
row_non_functional = (df['status_group'] == 'non functional')
row_repair = (df['status_group'] == 'functional needs repair')

col = 'gps_height'
fig3, ax3 = plt.subplots(figsize = (12,8))
sns.displot(df[col][row_functional], color = 'green', label = 'functional', ax = ax3)
sns.displot(df[col][row_non_functional], color = 'red', label = 'non functional', ax = ax3)
sns.displot(df[col][row_repair], color = 'blue', label = 'functional need repair', ax = ax3)
plt.legend()
plt.title('gps_heigh functionality')
plt.show()