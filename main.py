#!/usr/bin/env python
# coding: utf-8

# Task 1 - Install Spark, download datasets, create final dataframe. If you get an error regarding tar or wget, it is probably due to the Spark file being removed from the repository. Go to https://downloads.apache.org/spark/ and choose an equivalent version of Spark and Hadoop to download. So if 2.4.7 is not available, download the next version. At the time of this project creation, 2.4.7 exists.
#brew install openjdk-8-jdk-headless -qq > /dev/null'
#wget -q https://downloads.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
#tar xf spark-2.4.7-bin-hadoop2.7.tgz

#pip install -q findspark
#pip install pyspark

import os
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#os.environ["SPARK_HOME"] = "/content/spark-2.4.7-bin-hadoop2.7"

import findspark
findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import isnan, when, count, col, lit, trim, avg, ceil
from pyspark.sql.types import StringType
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

features = pd.read_csv('features.csv')
labels = pd.read_csv('labels.csv')




# Task 2 - Change column type, drop duplicated rows, remove whitespacs. If you are disconnected, please run the previous cells by clicking on this cell, going to Runtime, then clicking Run before.

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# Task 3 - Remove columns with null values more than a threshold. If you are disconnected, please run the previous cells by clicking on this cell, going to Runtime, then clicking Run before.

# In[ ]:





# In[ ]:





# In[ ]:





# Task 4 - Group, aggregate, create pivot table. If you are disconnected, please run the previous cells by clicking on this cell, going to Runtime, then clicking Run before.

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# Task 5 - Convert categories with low frequency to Others, impute missing values. If you are disconnected, please run the previous cells by clicking on this cell, going to Runtime, then clicking Run before.

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# Task 6 - Make visualizations. If you are disconnected, please run the previous cells by clicking on this cell, going to Runtime, then clicking Run before.

# In[ ]:


color_status = {'functional': 'green', 'non functional': 'red', 'functional needs repair': 'blue'}


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




