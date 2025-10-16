# CDC simulator: emits small change events to 'cdc-topic' that represent row inserts/updates/deletes
import time, json, os, csv
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic = 'cdc-topic'
# simple simulation: reuse raw dataset and send occasional update events
csv_path = os.path.join('data','raw_customer_data.csv')
with open(csv_path,'r',encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i,row in enumerate(reader):
        event = {'op':'u' if i%5==0 else 'i', 'row': row}
        producer.send(topic, value=event)
        print('CDC event sent', i+1)
        time.sleep(0.5)
producer.flush()
print('CDC simulation complete.')
