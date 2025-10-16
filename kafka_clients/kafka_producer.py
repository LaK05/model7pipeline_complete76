import json, time, os, csv
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic = 'streaming-input'
csv_path = os.path.join('data','raw_customer_data.csv')
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        # Simplify and send a small JSON per row; convert empty strings to None/numeric types
        msg = {
            'customer_id': row.get('customer_id') or None,
            'age': int(row['age']) if row.get('age') else None,
            'salary': float(row['salary']) if row.get('salary') else None,
            'num_transactions': int(row['num_transactions']) if row.get('num_transactions') else 0,
            'region': row.get('region') or 'UNK'
        }
        producer.send(topic, value=msg)
        print('Sent', i+1)
        time.sleep(0.2)
producer.flush()
print('Producer finished.')
