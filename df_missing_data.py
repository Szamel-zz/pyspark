from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv('data/ContainsNull.csv',inferSchema=True, header=True)

df.show()
df.printSchema()

# Drop missing values
df.na.drop().show()

# if there are at least 1 non null values
df.na.drop(thresh=1).show()
# if there are at least 2 non null values
df.na.drop(thresh=2).show()

# How
df.na.drop(how='all').show()
df.na.drop(how='any').show()

# Subsets
df.na.drop(subset=['Sales']).show()


# Fill missing values
# fill string values
df.na.fill('FILL VALUE').show()
# fill numeric values
df.na.fill(0).show()
# fill subset
df.na.fill('No Name', subset=['Name']).show()

# Fill numeric values with the mean
mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0]
df.na.fill(mean_sales, subset=['Sales']).show()
# one line:
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0], subset=['Sales']).show()