import pandas as pd
from datetime import datetime
import datetime as dt

import os
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long

from pyspark.sql.functions import udf, date_format, split, col, first, upper

get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

def get_session():
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark

def process_immigration_data(spark, path):
    us_immg_df=spark.read.parquet(path)
    us_immg_df=us_immg_df.select(col("i94res").cast(Int()),col("i94port"),
                           col("arrdate").cast(Int()), \
                           col("i94mode").cast(Int()),col("depdate").cast(Int()),
                           col("i94bir").cast(Int()),col("i94visa").cast(Int()), 
                           col("count").cast(Int()), \
                           "gender",col("admnum").cast(Long()))
    us_immg_df=us_immg_df.dropDuplicates()
    travel_mode=get_travel_mode(spark)
    visa_type = get_visa_type(spark)
    ports = get_ports_data(spark)
    us_immg_df = us_immg_df.join(ports, us_immg_df.i94port==ports.id, how='left')

    us_immg_df = us_immg_df.withColumn("arrival_date", get_date(us_immg_df.arrdate))
    us_immg_df=us_immg_df.drop("id")
    get_timestamp(spark,us_immg_df )

def get_travel_mode(spark):
    travel_mode =[[1,'Air'],[2,'Sea'],[3,'Land'],[9,'Not Reported']]
    travel_mode=spark.createDataFrame(travel_mode)
    travel_mode.write.mode("overwrite").parquet('travel_mode.parquet')
    return travel_mode

def get_visa_type(spark):
    visa_type = [[1, 'Business'], [2, 'Pleasure'], [3, 'Student']]
    visa_type=spark.createDataFrame(visa_type)
    visa_type.write.mode('overwrite').parquet('visa_type.parquet')
    return visa_type 

def get_ports_data(spark):
    ports_df = pd.read_csv('ports.txt',sep='=',names=['id','port'])
    ports_df['id']=ports_df['id'].str.strip().str.replace("'",'')
    ports_df['port_city'], ports_df['port_state']=ports_df['port'].str.strip().str.replace("'",'').str.strip().str.split(',',1).str

    ports_df['port_state']=ports_df['port_state'].str.strip()

    ports_df.drop(columns =['port'], inplace = True)

    ports_data=ports_df.values.tolist()
    ports_schema = R([
        Fld('id', Str(), True),
        Fld('port_city', Str(), True),
        Fld('port_state', Str(), True)
    ])
    ports=spark.createDataFrame(ports_data, ports_schema)
    ports.write.mode('overwrite').parquet('./data/ports.parquet')
    return ports 

def get_residence_cities(spark):
    cities = pd.read_csv('residence_city.txt',sep='=',names=['id','country'])
    cities['country']=cities['country'].str.replace("'",'').str.strip()
    cities_data=cities.values.tolist()
    cities_schema = R([
        Fld('id', Str(), True),
        Fld('country', Str(), True)
    ])
    cities=spark.createDataFrame(cities_data, cities_schema)
    cities.write.mode('overwrite').parquet('resident_city.parquet')
    return cities

def get_timestamp(spark,us_immg_df ):

    timestamp=us_immg_df.select(col('arrdate').alias('arrival_sasdate'),
                                       col('arrival_date').alias('arrival_iso_date'),
                                       date_format('arrival_date', 'y').alias('arrival_year'), 
                                       date_format('arrival_date','M').alias('arrival_month'),
                                       date_format('arrival_date','E').alias('arrival_dayofweek'), 
                                       date_format('arrival_date', 'd').alias('arrival_day')).dropDuplicates()

    timestamp.createOrReplaceTempView("timestamp")
    timestamp=spark.sql('''select arrival_sasdate,
                             arrival_iso_date,
                             arrival_month,
                             arrival_dayofweek,
                             arrival_year,
                             arrival_day,
                             CASE WHEN arrival_month IN (12, 1, 2) THEN 'winter' 
                                    WHEN arrival_month IN (3, 4, 5) THEN 'spring' 
                                    WHEN arrival_month IN (6, 7, 8) THEN 'summer' 
                                    ELSE 'autumn' 
                             END AS date_season from timestamp''')

    timestamp.write.mode("overwrite").partitionBy("arrival_year", "arrival_month").parquet('timestamp.parquet')
    return timestamp

def get_demographics_info(spark, path):
    demographics_df=spark.read.csv(path, sep=';', header=True)
    race_counts=(demographics_df.select("city","state code","Race","count")
        .groupby(demographics_df.City, "state code")
        .pivot("Race")
        .agg(first("Count")))
    us_dem_data=demographics_df.drop(*["Number of Veterans","Race","Count"]).dropDuplicates()

    us_dem_data.join(race_counts, ["city","state code"]).orderBy("city").show(5)
    us_dem_data=us_dem_data.join(race_counts, ["city","state code"])
    us_dem_data=us_dem_data.select('City', col('State Code').alias('State_Code'), 'State',
         col('Median Age').alias('MedianAge'),
         col('Male Population').alias('Males'), col('Female Population').alias('Females'), 
         col('Total Population').alias('TotalPop'), 'Foreign-born', 
         col('Average Household Size').alias('AvgHouseholdSize'),
         col('American Indian and Alaska Native').alias('NativePopulation'), 
         col('Asian').alias('AsianPopulation'), 
         col('Black or African-American').alias('BlackPopulation'), 
         col('Hispanic or Latino').alias('LatinoPopulation'), 
         col('White').alias('WhitePoplation'))

    us_dem_data=us_dem_data.drop("state")
    us_dem_data.write.mode('overwrite').parquet("us-cities-demographics.parquet")
    return us_dem_data, race_counts

def qa_checks(us_dem_data, race_counts, us_immg_df ):
    if us_dem_data.count() > 0:
        print('Deomographics data successfully persisted')
    else:
        print('Deomographics data could not be saved!')
        raise ValueError("Deomographics data could not be saved!")

    if us_dem_data.count() == race_counts.count():
        print('Aggregations were successful.')
    else:
        print('Failed while aggregating data')
        raise ValueError("Failed while aggregating data!")

    if us_immg_df.count() > 0:
        print('Immigration data saved successfully')
    else:
        print('No data in immigration data frame')
        raise ValueError("Failed while aggregating data!")

def run_pipeline():
    sc = get_session()
    us_immg_df = process_immigration_data(sc, 'sas_data')
    us_dem_data, race_counts =  get_demographics_info(spark, "us-cities-demographics.csv")
    us_immg_df=us_immg_df.join(us_dem_data, (upper(us_immg_df.port_city)==upper(us_dem_data.City)) & \
                                           (upper(us_immg_df.port_state)==upper(us_dem_data.State_Code)), how='left')
    us_immg_df.count()
    us_immg_df=us_immg_df.drop("City","State_Code")

    us_immg_df.drop('arrival_date').write.mode("overwrite").parquet('i94-immigration-data.parquet')
    qa_checks(us_dem_data, race_counts, us_immg_df )
        
        
if __name__ =='__main__':
    run_pipeline()
    