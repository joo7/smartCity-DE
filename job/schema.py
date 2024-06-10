from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DoubleType, TimestampType
# vehicle schema
vehicleSchema = StructType(
    [
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True),

    ]
)

# traffic Schema
trafficSchema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("deviceId", StringType(), nullable=True),
    StructField("cameraId", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("snapshot", StringType(), nullable=True)
])

# weather Schema
weatherSchema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("deviceId", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("weatherCondition", StringType(), nullable=True),
    StructField("precipitation", DoubleType(), nullable=True),
    StructField("windSpeed", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("airQualityIndex", DoubleType(), nullable=True)
])
# Emergency Schema
incidentSchema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("deviceId", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("type", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True)
])

gpsSchema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("deviceId", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("speed", DoubleType(), nullable=True),
    StructField("direction", StringType(), nullable=True),
    StructField("vehicleType", StringType(), nullable=True)
])