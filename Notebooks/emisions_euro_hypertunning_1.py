from sklearn.metrics import accuracy_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from scipy.stats import randint
import datetime
import polars as pl
import pickle
import os

from utils.dictionaries import types_parque_post,common_cols_parque_exacto

path = os.path.join("..","Data", "DGT")
clean_park = os.path.join(path,'Parque_exacto','clean_park.csv')
rf_file = os.path.join("..","Models","rf_model.pkl")

# Read the file keepink just the usefull columns
columns = ["FECHA_MATR", "FECHA_PRIM_MATR", "CLASE_MATR", "TIPO_DGT", "PROPULSION", "CILINDRADA", 
           "POTENCIA", "KW", "TARA","PESO_MAX", "MOM", "MMTA", "PLAZAS", "PLAZAS_MAX", "NUEVO_USADO", 
           "EMISIONES_CO2", "RENTING", "EMISIONES_EURO","CONSUMO", "ALIMENTACION", "CATELECT",'MARCA',
           "MODELO","TIPO","VARIANTE","VERSION","FABRICANTE"]
parke = pl.scan_csv(clean_park,separator='|', schema=types_parque_post).select(columns).head(10000000)
parke = parke.drop_nulls().head(1000000)
parke = parke.collect()

#Merge the columns that indicate the car model and drop them
model_variables = ['MARCA',"MODELO","TIPO","VARIANTE","VERSION","FABRICANTE"]
parke = parke.with_columns((pl.col('MARCA') + " " + pl.col("MODELO") + " " + 
                            pl.col("TIPO") + " " + pl.col("FABRICANTE")).alias("MODEL")).drop(model_variables)

#Select which are categorical variables from the columns list
target = 'EMISIONES_EURO'
[columns.remove(i) for i in model_variables]
features = [col for col in columns if col != target]
features.append('MODEL')
categorical_features = []
for key in types_parque_post:
    if key in columns:
        if (types_parque_post[key] == pl.String) & (key!=target):
            categorical_features.append(key)
categorical_features.append('MODEL')

#Change datetimes columns to antiquity
import datetime 
reference_date = datetime.date(2023, 12, 29)

parke = parke.with_columns(((pl.lit(reference_date) - pl.col("FECHA_MATR"))
     .cast(pl.Duration).dt.total_days().cast(pl.Int32).alias("FECHA_MATR")),
     ((pl.lit(reference_date) - pl.col("FECHA_PRIM_MATR"))
     .cast(pl.Duration).dt.total_days().cast(pl.Int32).alias("FECHA_PRIM_MATR")))

X = parke.select(features).to_dummies(categorical_features,drop_first=True).to_numpy()
y = parke.select(target).to_numpy().ravel()
y = LabelEncoder().fit_transform(y)
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.33, random_state=42)

param_grid = {'n_estimators': randint(50,500),
              'max_features': ['log2', 'sqrt', None],
              'max_depth': randint(5,40),
              'min_samples_split': [2, 5, 10],
              'min_samples_leaf': [1, 2, 4]}

rf = RandomForestClassifier()
rand_search = RandomizedSearchCV(rf,param_distributions = random_grid,n_iter=5, cv=5, n_jobs=-1)
rand_search.fit(X_train, y_train)
best_rf = rand_search.best_estimator_