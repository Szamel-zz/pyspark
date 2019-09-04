from pyspark.sql import SparkSession


# Spark DataFrame Basic Operations

spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('data/appl_stock.csv', inferSchema=True, header=True)

df.printSchema()
df.describe().show()
print(df.columns)
print(df.head())

# Filtering
# less than
df.filter('Close < 500').select(['Open', 'Close']).show()
# or
df.filter(df['Close'] < 500).select(['Open', 'Close']).show()
# equality
df.filter(df['low'] == 197.16).show()


# multiple condition
# and
df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()
# and not
df.filter((df['Close'] < 200) & ~(df['Open'] > 200)).show()

result = df.filter(df['low'] == 197.16).collect()
print(result)
print(result[0])
row = result[0]
print(row.asDict())
print(row.asDict()['Volume'])