from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, weekofyear, dayofyear, dayofmonth, hour, format_number, date_format

spark = SparkSession.builder.appName('dates').getOrCreate()

df = spark.read.csv('data/appl_stock.csv', inferSchema=True, header=True)

print(df.head(1))

df.select(dayofmonth(df['Date'])).show()

# Average closing price per year
# new dataframe with Year columns
df.withColumn('Year', year(df['Date'])).show()
new_df = df.withColumn('Year', year(df['Date']))

result = new_df.groupBy('Year').mean().select(['Year', 'avg(Close)'])
result = result.select(['year', format_number('avg(Close)', 2)])
result = result.withColumnRenamed('format_number(avg(Close), 2)', 'Average Closing Price')
result.show()