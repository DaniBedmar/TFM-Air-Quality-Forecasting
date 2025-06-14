import os
import polars as pl
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

pollutant_cols = ["MONTHLYNi_concentration","MONTHLYPM10_concentration","MONTHLYPM2.5_concentration"]
for i,pollutant in enumerate(['dataset_Ni.parquet','dataset_PM10.parquet','dataset_PM25.parquet']):

    dataset_path = os.path.join('..','Data','Final_Dataset',pollutant)
    dataset = pl.read_parquet(dataset_path).to_pandas()
    dataset["CITY"] = dataset["CITY"].astype("category")
    target_col = pollutant_cols[i]  # "MONTHLYNi_concentration" MONTHLYPM10_concentration,MONTHLYPM25_concentration
    feature_cols = [
        "EURO_1", "EURO_2", "EURO_3", "EURO_4", "EURO_5", "EURO_6", "EURO_CLEAN",
        "Previous", "CITY", "year","CITY_AREA", "month","month_sin","month_cos","TotalFleet",
        "CARS_PER_KM2", "EURO_1_PER_KM2", "EURO_2_PER_KM2","EURO_3_PER_KM2","EURO_4_PER_KM2",
        "EURO_5_PER_KM2","EURO_6_PER_KM2","EURO_CLEAN_PER_KM2","Previous_PER_KM2","POPULATION",
        "Population_density","CARS_PER_CAPITA", "EURO_1_PER_CAPITA", "EURO_2_PER_CAPITA",
        "EURO_3_PER_CAPITA","EURO_4_PER_CAPITA","EURO_5_PER_CAPITA","EURO_6_PER_CAPITA",
        "EURO_CLEAN_PER_CAPITA","Previous_PER_CAPITA"
        ]

    dataset = dataset.dropna(subset=feature_cols + [target_col])
    X = dataset[feature_cols]
    y = dataset[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    lgb_train = lgb.Dataset(X_train, label=y_train, categorical_feature=["CITY"])
    lgb_val = lgb.Dataset(X_test, label=y_test, reference=lgb_train)

    lgb_params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.05,
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
    rmse_lgb = mean_squared_error(y_test, y_pred_lgb)
    print(f"LightGBM RMSE for {pollutant}: {rmse_lgb:.4f}")