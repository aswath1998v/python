import Adafruit_DHT
from google.cloud import pubsub_v1
from pymongo import MongoClient
import time
import os
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/rpi/iotsf-407206-eabd36c6fbef.json"

# Google Cloud Pub/Sub configuration
project_id = "iotsf-407206"
topic_name = "message"
subscription_name = "message-sub"

# MongoDB configuration
mongodb_connection_string = "mongodb://adminUser:adminPassword@34.170.242.3:27017/?authMechanism=DEFAULT&authSource=admin"
database_name = "admin"
collection_name = "temperature_readings"

# DHT11 configuration
DHT_SENSOR = Adafruit_DHT.DHT11
DHT_PIN = 4  # Change this to the GPIO pin you connected the DHT11 sensor to

# Create a connection to the Pub/Sub topic
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Create a connection to the Pub/Sub subscription
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Create a connection to MongoDB
mongo_client = MongoClient(mongodb_connection_string)
db = mongo_client[database_name]
collection = db[collection_name]

def publish_temperature_reading():
    while True:
        humidity, temperature = Adafruit_DHT.read(DHT_SENSOR, DHT_PIN)
        if humidity is not None and temperature is not None:
            # Get the current time
            current_time = datetime.now().isoformat()

            message = f"Time: {current_time}, Temperature: {temperature} °C, Humidity: {humidity} %"
            message_bytes = message.encode("utf-8")

            # Publish the message to Pub/Sub
            publisher.publish(topic_path, data=message_bytes)

            print(f"Published: {message}")

            # Adjust the sleep time based on your desired data rate
            time.sleep(1)  # Publish a reading every 1 second
        else:
            print("Sensor failure. Check wiring.");

def insert_temperature_reading(message):
    data = message.data.decode("utf-8")
    print(f"Received message: {data}")

    # Insert the temperature and humidity reading into MongoDB
    split_data = data.split(", ")
    if len(split_data) == 3:
        time_str, temperature_str, humidity_str = split_data

        # Parse the time, temperature, and humidity from the received message
        current_time = datetime.fromisoformat(time_str.split(": ")[1])
        temperature = float(temperature_str.split(": ")[1].split(" ")[0])
        humidity = float(humidity_str.split(": ")[1].split(" ")[0])

        # Include the time when inserting the data into MongoDB
        collection.insert_one({"time": current_time, "temperature": temperature, "humidity": humidity})

    else:
        print(f"Error: Expected data to be in format 'Time: <time>, Temperature: <temperature> °C, Humidity: <humidity> %', but got {data}")

    message.ack()  # Acknowledge the message

if __name__ == "__main__":
    # Start the publisher in a separate thread to continuously publish data
    import threading
    publisher_thread = threading.Thread(target=publish_temperature_reading)
    publisher_thread.start()

    # Subscribe and consume messages from Pub/Sub and insert into MongoDB
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=insert_temperature_reading)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()