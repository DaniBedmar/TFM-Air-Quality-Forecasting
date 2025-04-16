from sklearn.metrics import accuracy_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from scipy.stats import randint
import polars as pl
import pickle
import os

from utils.dictionaries import types_parque_post

path = os.path.join("..","Data", "DGT")
clean_park = os.path.join(path,'Parque_exacto','clean_park.csv')
rf_file = os.path.join("..","Models","rf_model.pkl")

columns = ['MARCA','EMISIONES_CO2','EMISIONES_EURO']
target = 'EMISIONES_EURO'
features = [col for col in columns if col != target]
categorical_features = []
for key in types_parque_post:
    if key in columns:
        if (types_parque_post[key] == pl.String) & (key!=target):
            categorical_features.append(key)

parke = pl.scan_csv(clean_park,separator='|', schema=types_parque_post).select(columns).head(1000000)
parke = parke.drop_nulls().head(100000)
parke = parke.collect()

X = parke.select(features).to_dummies(categorical_features,drop_first=True).to_numpy()
y = parke.select(target).to_numpy().ravel()
y = LabelEncoder().fit_transform(y)
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.33, random_state=42)

rf = RandomForestClassifier(n_estimators=100,
                            max_depth=20,
                            n_jobs=2)
rf.fit(X_train, y_train)
y_pred = rf.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)
with open(rf_file,'wb') as f:
    pickle.dump(rf,f)

with open(rf_file, 'rb') as f:
    rf = pickle.load(f)