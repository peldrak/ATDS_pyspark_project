from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import time
import datetime
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

#create list of paths to read all parquet files at once
InputPaths = [
    "hdfs://master:9000/records/yellow_tripdata_2022-01.parquet",
    "hdfs://master:9000/records/yellow_tripdata_2022-02.parquet",
    "hdfs://master:9000/records/yellow_tripdata_2022-03.parquet",
    "hdfs://master:9000/records/yellow_tripdata_2022-04.parquet",
    "hdfs://master:9000/records/yellow_tripdata_2022-05.parquet",
    "hdfs://master:9000/records/yellow_tripdata_2022-06.parquet"
]

#DataFrames
df_tripdata = spark.read.parquet(*InputPaths)
df_zones = spark.read.csv("hdfs://master:9000/records/taxi+_zone_lookup.csv")

#Preprocessing
df_tripdata.na.drop()
df_tripdata = df_tripdata.withColumn('tpep_dropoff_datetime', to_timestamp(df_tripdata.tpep_dropoff_datetime, 'MM-dd-yyyy HH:mm:ss.SSS'))
df_tripdata = df_tripdata.withColumn('tpep_pickup_datetime', to_timestamp(df_tripdata.tpep_pickup_datetime, 'MM-dd-yyyy HH:mm:ss.SSS'))
df_tripdata = df_tripdata.filter((year(df_tripdata['tpep_dropoff_datetime']) == 2022) & (year(df_tripdata['tpep_pickup_datetime']) == 2022))
df_tripdata = df_tripdata.filter((month(df_tripdata['tpep_dropoff_datetime']) < 7) & (month(df_tripdata['tpep_pickup_datetime']) < 7))

#RDDs
rdd_tripdata = df_tripdata.rdd
rdd_zones = df_zones.rdd


'''
Q1 - Find trip with the biggest tip in March and Drop Off at Battery Park (DataFrames Only)
'''
start_Q1 = time.time()
#Join with df_zones to get locationID for Battery park
df_Q1 = df_tripdata.join(df_zones, df_tripdata.DOLocationID==df_zones._c0, 'inner')

#Filter by DOLocationID = "Battery Park" and tpep_dropoff_datetime -> month=3
df_Q1 = df_Q1.filter((month(df_Q1['tpep_dropoff_datetime']) == 3) & (df_Q1['_c2'] == 'Battery Park'))

#Highest Tip_amount
window1 = Window.orderBy(df_Q1['Tip_amount'].desc())
df_Q1 = df_Q1.select('*', rank().over(window1).alias('rank')).filter(col('rank')<=1).drop(col('rank'))
df_Q1.show()
end_Q1 = time.time()
print(f'Time taken Q1: {end_Q1-start_Q1} seconds.')


'''
Q2 - Find for each month the trip with highest Tolls_amount
'''
start_Q2 = time.time()
df_Q2 = df_tripdata.filter(df_tripdata['Tolls_amount'] > 0) #ignore zero tolls
df_new = df_Q2

df_new = df_new.groupby(month(df_new['tpep_dropoff_datetime'])).max('Tolls_amount').orderBy(asc(month(df_new['tpep_dropoff_datetime'])))
df_Q2 = df_Q2.join(df_new, df_Q2['Tolls_amount'] == df_new['max(Tolls_amount)'])
df_Q2 = df_Q2.orderBy(asc('tpep_dropoff_datetime'))

df_Q2.show()
end_Q2 = time.time()
print(f'Time taken Q2: {end_Q2-start_Q2} seconds.')

'''
Q3 - 15 day Avg of Trip_distance and Total_amount when PULocationID not equal to DOLocationID
'''
#Using DataFrame
start_Q3_df = time.time()

df_Q3 = df_tripdata.filter(df_tripdata['PULocationID'] != df_tripdata['DOLocationID'])
w = df_Q3.groupBy(window('tpep_pickup_datetime', '15 days', startTime='70 hours'))\
        .agg(avg('Total_amount').alias('Avg(Total_amount)'), avg('Trip_distance').alias('Avg(Trip_distance)'))

w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "Avg(Total_amount)", "Avg(Trip_distance)")\
        .orderBy(asc('start')).show()

end_Q3_df = time.time()
print(f'Time taken Q3 with DF: {end_Q3_df-start_Q3_df} seconds.')

#Using RDD
start_Q3_rdd = time.time()

def get15(date):
        if date.day > 15:
                return int(date.month*2)
        else:
                return int(date.month*2-1)

rdd = rdd_tripdata \
        .filter(lambda x: x.PULocationID != x.DOLocationID)\
        .map(lambda x: ((get15(x.tpep_pickup_datetime)), (x.total_amount, x.trip_distance, 1)))


avg_by_key = rdd \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))\
        .map(lambda x: ((x[0]), (x[1][0] / x[1][2], x[1][1] / x[1][2]))) \
        .sortByKey(ascending=True)

for y in avg_by_key.collect():
        print(y)

end_Q3_rdd = time.time()
print(f'Time taken Q3 with RDD: {end_Q3_rdd-start_Q3_rdd} seconds.')

'''
Q4 - Top 3 hours per weekday
'''
start_Q4 = time.time()

df_Q4p = df_tripdata.groupBy(month('tpep_pickup_datetime'), dayofmonth('tpep_pickup_datetime'), dayofweek('tpep_pickup_datetime'), hour('tpep_pickup_datetime')).agg(sum('Passenger_count')).orderBy(month('tpep_pickup_datetime').asc(), dayofweek('tpep_pickup_datetime').asc(), hour('tpep_pickup_datetime').asc())
df_Q4p = df_Q4p.withColumnRenamed('month(tpep_pickup_datetime)', 'month')\
        .withColumnRenamed('dayofmonth(tpep_pickup_datetime)', 'day')\
        .withColumnRenamed('dayofweek(tpep_pickup_datetime)', 'weekday')\
        .withColumnRenamed('hour(tpep_pickup_datetime)', 'hour')\
        .withColumnRenamed('sum(Passenger_count)', 'pickups')

df_Q4d = df_tripdata.groupBy(month('tpep_dropoff_datetime'), dayofmonth('tpep_pickup_datetime'), dayofweek('tpep_dropoff_datetime'), hour('tpep_dropoff_datetime')).agg(sum('Passenger_count')).orderBy(month('tpep_dropoff_datetime').asc(), dayofweek('tpep_dropoff_datetime').asc(), hour('tpep_dropoff_datetime').asc())
df_Q4d = df_Q4d.withColumnRenamed('month(tpep_dropoff_datetime)', 'month')\
        .withColumnRenamed('dayofmonth(tpep_pickup_datetime)', 'day')\
        .withColumnRenamed('dayofweek(tpep_dropoff_datetime)', 'weekday')\
        .withColumnRenamed('hour(tpep_dropoff_datetime)', 'hour')\
        .withColumnRenamed('sum(Passenger_count)', 'dropoffs')

w=Window.orderBy(lit(1))

df_Q4p=df_Q4p.withColumn("rn",row_number().over(w)-1)
df_Q4d=df_Q4d.withColumn("rn",row_number().over(w)-1).drop('month', 'day', 'weekday', 'hour')

df = df_Q4p.join(df_Q4d,["rn"]).drop("rn")


windowSpec = Window.orderBy(df["month"].asc(), df["day"].asc(), df["hour"].asc())
df = df.withColumn("Act_pass", df['pickups'] + lag(df['pickups'] - df['dropoffs'], 1, default=0).over(windowSpec))

df = df.groupBy('weekday', 'hour').agg(round(mean("Act_pass"), 2)).orderBy(df['weekday'].asc(), df['hour'].asc())
df = df.withColumnRenamed('round(avg(Act_pass), 2)', 'passenger_count')
window = Window.partitionBy(df['weekday']).orderBy(df['passenger_count'].desc())
df.select('*', rank().over(window).alias('rank')).filter(col('rank')<=3).show(21)

end_Q4 = time.time()
print(f'Time taken Q4: {end_Q4-start_Q4} seconds.')

'''
Q5 - Top 5 days per month where the tip percentage was the highest
'''
start_Q5 = time.time()
#df_Q5 = df_tripdata.filter((df_tripdata['Tip_amount']<=df_tripdata['Fare_amount']) & (df_tripdata['Fare_amount']>0))
df_Q5 = df_tripdata.withColumn('Tip_percentage(%)', (col('Tip_amount')/col('Fare_amount'))*100)
df_Q5 = df_Q5.withColumn('DayOfMonth', date_format(col('tpep_dropoff_datetime'), 'd'))

df_Q5 = df_Q5.groupBy('DayOfMonth', month('tpep_dropoff_datetime').alias('Month'))\
        .agg(avg('Tip_percentage(%)').alias('Tip_percentage_per_day(%)')).orderBy(asc('Month'))

window5 = Window.partitionBy(df_Q5['Month']).orderBy(df_Q5['Tip_percentage_per_day(%)'].desc())
df_Q5 = df_Q5.select('*', rank().over(window5).alias('rank')).filter(col('rank')<=5).drop(col('rank'))

df_Q5.show(30)
end_Q5 = time.time()
print(f'Time taken Q5: {end_Q5-start_Q5} seconds.')
