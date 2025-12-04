from kafka import KafkaProducer
import csv
import json
import time

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CSV file
filename = "seattle-weather.csv"

with open(filename, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert types
        row_converted = {
            "date": row["date"],
            "precipitation": float(row["precipitation"]) if row["precipitation"] else None,
            "temp_max": float(row["temp_max"]) if row["temp_max"] else None,
            "temp_min": float(row["temp_min"]) if row["temp_min"] else None,
            "wind": float(row["wind"]) if row["wind"] else None,
            "weather": row["weather"]
        }
        # Send to Kafka topic
        producer.send("weather_topic", row_converted)
        print("Sent:", row_converted)
        time.sleep(0.2)  # simulate streaming speed

producer.flush()
print("All data sent!")
