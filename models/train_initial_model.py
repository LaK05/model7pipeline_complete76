# Train and serialize an initial SGDClassifier model for streaming updates.
import os, joblib, numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline

data_path = os.path.join('data','raw_customer_data.csv')
# Simple CSV loader (minimal preprocessing)
import csv
X = []
y = []
with open(data_path,'r',encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        age = int(row['age']) if row.get('age') else 30
        salary = float(row['salary']) if row.get('salary') else 50000.0
        num_txn = int(row.get('num_transactions') or 0)
        X.append({'age':age,'salary':salary,'num_transactions':num_txn,'region': row.get('region') or 'UNK'})
        # Synthetic label: high spender if salary > 60000
        y.append(1 if salary>60000 else 0)
        if len(X)>=500: break
vect = DictVectorizer(sparse=False)
X_enc = vect.fit_transform(X)
scaler = StandardScaler()
numeric_cols = X_enc[:,:3] if X_enc.shape[1]>=3 else X_enc
scaler.fit(numeric_cols)
sgd = SGDClassifier(loss='log_loss', max_iter=1000)
sgd.fit(numeric_cols, y)
# Combine into a simple dict for saving: vect, scaler, sgd
model_obj = {'vect': vect, 'scaler': scaler, 'sgd': sgd}
os.makedirs('models', exist_ok=True)
joblib.dump(model_obj, os.path.join('models','initial_model.pkl'))
print('Initial model trained and saved to models/initial_model.pkl')
