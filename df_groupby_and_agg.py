from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg, stddev, format_number



spark = SparkSession.builder.appName('aggs').getOrCreate()

df = spark.read.csv('data/sales_info.csv', inferSchema=True, header=True)

df.show()
df.printSchema()

# Group together the Company column

print(df.groupBy('Company'))
df.groupBy('Company').mean().show()
df.groupBy('Company').count().show()

# Aggregation

df.agg({'Sales': 'sum'}).show()
df.agg({'Sales': 'max'}).show()

# Same as groupBy with max
group_data = df.groupBy('Company')
group_data.agg({'Sales': 'max'}).show()


df.select(avg('Sales').alias('Average Sales')).show()

sales_std = df.select(stddev('sales').alias('std'))
sales_std.select(format_number('std', 2).alias('std')).show()


# Orderby

# ascending
df.orderBy('Sales').show()
# decreasing
df.orderBy(df['Sales'].desc()).show()

