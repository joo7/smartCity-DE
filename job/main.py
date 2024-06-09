import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime
import random

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

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def simulate_vehicle_movement():
    global start_location
    # moving towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform()
    start_location['longitude'] += LONGITUDE_INCREMENT

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.server': KAFKA_BOOTSTRAP_SERVER,
        'error_cb': lambda err: print('kafka error: ' + err)
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'Ford-Mustang')
    except KeyboardInterrupt:
        print('Simulation has been ended by the user')
    except Exception as e:
        print(f'Unexpected error occured {e}')
