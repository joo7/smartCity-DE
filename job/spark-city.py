from job.config import configuration
from job.schema import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as func


def main():
    # Create SparkConf object
    conf = SparkConf() \
        .setAppName('smart-city-streaming') \
        .set('spark.jars.packages',
             'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk:1.12.739') \
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .set('spark.hadoop.fs.s3a.access.key', configuration['access_key']) \
        .set('spark.hadoop.fs.s3a.secret.key', configuration['secret_key']) \
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

    # spark = SparkSession.builder.appName('smart-city-streaming') \
    #     .config('spark.jars.packages',
    #             'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1',
    #             'org.apache.hadoop:hadoop-aws:3.4.0',
    #             'com.amazonaws.:aws-java-sdk:1.12.739') \
    #     .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    #     .config('spark.hadoop.fs.s3a.access_key', configuration['access_key']) \
    #     .config('spark.hadoop.fs.s3a.secret_key', configuration['secret_key']) \
    #     .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentials')\
    #     .getOrCreate()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # adjust the log level
    spark.sparkContext.setLogLevel('WARN')

    def read_kafka_topic(topic, schema):
        return (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST (values as STRING')
                .select(func.from_json(func.col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark(eventTime='timestamp', delayThreshold='2 minutes'))
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', incidentSchema).alias('emergency')

