# Consumer that performs inference and incremental updates (partial_fit)
import json, os, joblib, time
from kafka import KafkaConsumer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier
from sklearn.feature_extraction import DictVectorizer
import numpy as np
MODEL_PATH = os.path.join('models', 'sgd_model.pkl')
topic = 'streaming-input'
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         enable_auto_commit=True,
                         consumer_timeout_ms=10000)
# Create or load model
if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
    print('Loaded existing model.')
else:
    model = Pipeline([('vect', DictVectorizer(sparse=False)),
                      ('scaler', StandardScaler()),
                      ('sgd', SGDClassifier(loss='log_loss', max_iter=1, tol=None, warm_start=True))])
    # For partial_fit we need an initial call; simulate two classes [0,1]
    X_init = [{'age':30,'salary':50000,'num_transactions':1,'region':'UNK'}]
    y_init = np.array([0])
    model.named_steps['vect'].fit(X_init)
    # scaler fit with numeric array
    model.named_steps['scaler'].fit(np.array([[30,50000,1]]))
    # wrap sgd via partial_fit on encoded features
    # We'll use a small workaround: keep SGD classifier separate for partial_fit in streamed processing
    print('Initialized new pipeline model.')
# Stream loop: predict and update model every N records
buffer = []
N_UPDATE = 50
for msg in consumer:
    rec = msg.value
    # build feature dict
    feat = {'age': rec.get('age') or 30,
            'salary': rec.get('salary') or 50000,
            'num_transactions': rec.get('num_transactions') or 0,
            'region': rec.get('region') or 'UNK'}
    buffer.append(feat)
    if len(buffer) >= N_UPDATE:
        # convert and partial_fit
        X = model.named_steps['vect'].transform(buffer)
        X_scaled = model.named_steps['scaler'].transform(X[:, :3]) if X.shape[1] >=3 else X
        # For demo, create synthetic labels (e.g., high-spender if salary>60000)
        y = (X[:,1].astype(float) > 60000).astype(int)
        # Fit the SGD classifier in the pipeline by accessing underlying estimator
        # We must transform to numeric array of shape (n_samples, n_features)
        # Here we will re-fit the entire pipeline's SGD via fit for simplicity (not ideal in production).
        try:
            from sklearn.base import clone
            sgd = model.named_steps['sgd']
            sgd.partial_fit(X[:, :3].astype(float), y, classes=[0,1])
        except Exception as e:
            # fallback: retrain sgd using fit on small buffer
            sgd = SGDClassifier(loss='log_loss', max_iter=1000)
            sgd.fit(X[:, :3].astype(float), y)
            model.named_steps['sgd'] = sgd
        # persist model snapshot
        os.makedirs('models', exist_ok=True)
        joblib.dump(model, MODEL_PATH)
        print('Model snapshot saved.')
        buffer = []
print('Consumer finished.')
