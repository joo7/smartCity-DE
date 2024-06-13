from dotenv import load_dotenv
import os

import schema
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql import functions as func


# Load environment variables from the .env file
load_dotenv()
def main():
    # Create SparkConf object
    bucket = os.environ['BUCKET']
    conf = SparkConf() \
        .setAppName('smart-city-streaming') \
        .set('spark.jars.packages',
             'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk:1.12.740') \
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .set('spark.hadoop.fs.s3a.access.key', os.environ['ACCESS_KEY']) \
        .set('spark.hadoop.fs.s3a.secret.key', os.environ['SECRET_KEY']) \
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # adjust the log level
    spark.sparkContext.setLogLevel('WARN')

    def read_kafka_topic(topic, schema):
        print(f"fetching the data for topic{topic}")
        return (spark.readStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'broker:29092')\
                .option('subscribe', topic)\
                .option('startingOffsets', 'earliest')\
                .load()\
                .selectExpr('CAST (value as STRING)')\
                .select(func.from_json(func.col('value'), schema).alias('data'))\
                .select('data.*')\
                .withWatermark(eventTime='timestamp', delayThreshold='2 minutes'))


    vehicleDF = read_kafka_topic('vehicle_data', schema.vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', schema.gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', schema.trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', schema.weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', schema.incidentSchema).alias('emergency')
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    base_checkpoint_path = 's3a://{bucket}/checkpoints/'.format(bucket= bucket)
    base_data_path = 's3a://{bucket}/data/'.format(bucket= bucket)
    query1 = streamWriter(vehicleDF, base_checkpoint_path  + 'vehicle_data', base_data_path+ 'vehicle_data')
    query2 = streamWriter(gpsDF, base_checkpoint_path  + 'gps_data', base_data_path+ 'gps_data')
    query3 = streamWriter(trafficDF, base_checkpoint_path  + 'traffic_data', base_data_path+ 'traffic_data')
    query4 = streamWriter(weatherDF, base_checkpoint_path  + 'weather_data', base_data_path+ 'weather_data')
    query5 = streamWriter(emergencyDF, base_checkpoint_path  + 'emergency_data', base_data_path+ 'emergency_data')
    query5.awaitTermination()

if __name__=="__main__":
    main()