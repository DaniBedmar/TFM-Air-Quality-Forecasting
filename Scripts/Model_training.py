import os
import polars as pl
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt


dataset_path = os.path.join('..','Data','Final_Dataset','final_dataset.parquet')
dataset = pl.read_parquet(dataset_path)
pollutants_cols = [col for col in dataset.columns if col.startswith('MONTHLY')]

for i,pollutant_col in enumerate(pollutants_cols):
    pollutant = pollutant_col.split('_')[0][7:]
    exclude = set(pollutants_cols)
    cols_of_interest = [s for s in dataset.columns if s not in exclude]
    cols_of_interest.append(pollutant_col)

    dataset_aux = dataset.select(cols_of_interest)
    dataset_aux = dataset_aux.filter(pl.col(pollutant_col).is_not_null())
    target_col = pollutant_col
 
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous", "year", "month","month_sin","month_cos","TotalFleet",
        "POPULATION","CARS_PER_CAPITA", "EURO_1_PER_CAPITA", "EURO_2_PER_CAPITA",
        "EURO_3_PER_CAPITA","EURO_4_PER_CAPITA","EURO_5_PER_CAPITA","EURO_6_PER_CAPITA",
        "EURO_CLEAN_PER_CAPITA","Previous_PER_CAPITA"]
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous", "year","CITY_AREA","TotalFleet",
        "CARS_PER_KM2", "EURO_1_PER_KM2", "EURO_2_PER_KM2","EURO_3_PER_KM2","EURO_4_PER_KM2",
        "EURO_5_PER_KM2","EURO_6_PER_KM2","EURO_CLEAN_PER_KM2","Previous_PER_KM2","POPULATION",
        "Population_density","CARS_PER_CAPITA", "EURO_1_PER_CAPITA", "EURO_2_PER_CAPITA",
        "EURO_3_PER_CAPITA","EURO_4_PER_CAPITA","EURO_5_PER_CAPITA","EURO_6_PER_CAPITA",
        "EURO_CLEAN_PER_CAPITA","Previous_PER_CAPITA","EURO_1_PROPORTION","EURO_2_PROPORTION",
        "EURO_3_PROPORTION","EURO_4_PROPORTION","EURO_5_PROPORTION","EURO_6_PROPORTION",
        "EURO_CLEAN_PROPORTION","Previous_PROPORTION"]
    
    
    # Basicas
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous","TotalFleet"]
    
    # Basicas + area y poblacion
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous","TotalFleet","CITY_AREA","POPULATION"]
    
    # Basicas + cohes/km2
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous","TotalFleet",
        "CARS_PER_KM2", "EURO_1_PER_KM2", "EURO_2_PER_KM2","EURO_3_PER_KM2","EURO_4_PER_KM2",
        "EURO_5_PER_KM2","EURO_6_PER_KM2","EURO_CLEAN_PER_KM2","Previous_PER_KM2"]
    
    # Basicas + coches/capita
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous","TotalFleet",
        "CARS_PER_CAPITA", "EURO_1_PER_CAPITA", "EURO_2_PER_CAPITA",
        "EURO_3_PER_CAPITA","EURO_4_PER_CAPITA","EURO_5_PER_CAPITA","EURO_6_PER_CAPITA",
        "EURO_CLEAN_PER_CAPITA","Previous_PER_CAPITA"]

    #Basica
    dataset_aux = dataset_aux.to_pandas()
    dataset_aux = dataset_aux.dropna(subset=feature_cols + [target_col])
    X = dataset_aux[feature_cols]
    y = dataset_aux[target_col]

    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    scaler.fit(X)
    X = scaler.transform(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    lgb_train = lgb.Dataset(X_train, label=y_train)#, categorical_feature=["CITY"])
    lgb_val = lgb.Dataset(X_test, label=y_test, reference=lgb_train)

    lgb_params = {
        "objective": "regression",
        "boosting_type": "gbdt",
        "metric": "rmse",
        "learning_rate": 0.05,
        "max_depth": 30,
        "num_leaves": 2,
        "verbose": -1
    }

    lgb_model = lgb.train(
        params=lgb_params,
        train_set=lgb_train,
        valid_sets=[lgb_val],
        num_boost_round=10000
    )

    y_pred_lgb = lgb_model.predict(X_test)
    rmse_lgb = np.sqrt(mean_squared_error(y_test, y_pred_lgb))
    realtive_rmse = rmse_lgb/y.mean()
    print(f"LightGBM RMSE for {pollutant}: {100*realtive_rmse:.2f}")
    print(dataset_aux.shape)