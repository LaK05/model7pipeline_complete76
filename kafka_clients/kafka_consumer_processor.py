import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from joblib import dump, load
from collections import deque

# Initialize ML components
scaler = StandardScaler()
model = LogisticRegression()

# Create rolling buffer
buffer = deque(maxlen=10)

# Create Kafka Consumer
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='host.docker.internal:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening to Kafka topic: sensor-data")

for msg in consumer:
    record = msg.value
    buffer.append(record)

    df = pd.DataFrame(buffer)
    avg_temp = df['temperature'].mean()
    avg_hum = df['humidity'].mean()

    # Dummy binary classification based on temperature threshold
    df['alert'] = (df['temperature'] > 35).astype(int)

    print(f"Avg Temp: {avg_temp:.2f} | Avg Hum: {avg_hum:.2f} | Latest Alert: {df['alert'].iloc[-1]}")
