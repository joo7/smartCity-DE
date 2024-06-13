import os
import traceback
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import time

LONDON_COORDINATES = {
    "latitude": 51.5074,
    "longitude": -0.1278
}
BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}
# movement increment
LATITUDE_INCREMENT = (LONDON_COORDINATES['latitude'] - BIRMINGHAM_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (LONDON_COORDINATES['longitude'] - BIRMINGHAM_COORDINATES['longitude']) / 100
# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')
WEATHER = ['Rainy', 'Snowy', 'Sunny', 'Cloudy']
INCIDENT_TYPE = ['Accident', 'Fire', "Medical", "Police", "None"]
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()
random.seed(99)


def get_next_time():
    global start_time
    start_time += timedelta(random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(10, 40),
        "direction": 'North-East',
        "vehicleType": vehicle_type
    }


def simulate_vehicle_movement():
    global start_location
    # moving towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location['latitude'], location['longitude']),
        "speed": random.uniform(10, 40),
        "direction": 'North-East',
        "make": "Ford",
        "model": "GT",
        "year": 2024,
        "fuelType": "Petrol"
    }


def generate_traffic_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "cameraId": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString"
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5, 26),
        "weatherCondition": random.choice(WEATHER),
        "precipitation": random.uniform(0, 25),
        "windSpeed": random.uniform(0, 100),
        "humidity": random.uniform(0, 100),
        "airQualityIndex": random.uniform(0, 500)
    }


def generate_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "location": location,
        "timestamp": timestamp,
        "type": random.choice(INCIDENT_TYPE),
        "status": random.choice(['Resolved', 'Active']),
        "description": "description of a incident"
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object is of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message Delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(kafka_producer, topic, data):
    kafka_producer.produce(topic,
                           key=str(data['id']),
                           value=json.dumps(data, default=json_serializer).encode('utf-8'),
                           on_delivery=delivery_report)
    kafka_producer.flush()


def simulate_journey(kafka_producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        timestamp = vehicle_data['timestamp']
        location = vehicle_data['location']
        gps_data = generate_gps_data(device_id, timestamp)
        traffic_data = generate_traffic_data(device_id, timestamp, location, camera_id='cam-1231')
        weather_data = generate_weather_data(device_id, timestamp, location)
        emergency_data = generate_incident_data(device_id, timestamp, location)
        if location[0] >= BIRMINGHAM_COORDINATES['latitude'] and location[1] >= BIRMINGHAM_COORDINATES['longitude']:
            print('vehicle reached to Birmingham')
            break
        produce_data_to_kafka(kafka_producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(kafka_producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(kafka_producer, TRAFFIC_TOPIC, traffic_data)
        produce_data_to_kafka(kafka_producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(kafka_producer, EMERGENCY_TOPIC, emergency_data)
        time.sleep(5)

# main
if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'error_cb': lambda err: print('kafka error: ' + err)
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'device-123')
    except KeyboardInterrupt:
        print('Simulation has been ended by the user')
    except Exception as e:
        print(traceback.format_exc())
        print(f'Unexpected error occurred {e}')
