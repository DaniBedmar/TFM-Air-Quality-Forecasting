import os
import polars as pl
import numpy as np
import datetime

from utils.dictionaries import cities_area, cities

valid_stations_path = os.path.join('..','Data','Air_quallity','valid_stations.csv')
df_cities = pl.read_csv(valid_stations_path, separator='|').select(["CITY", "PROVINCIA", "MUNICIPIO"]).unique()

cities = {
    row["CITY"]: [row["PROVINCIA"], row["MUNICIPIO"]]
    for row in df_cities.to_dicts()
}

fleet_distribution = os.path.join('..', 'Data', 'DGT', 'Exact_fleet', 'monthly_park_distribution.csv')
air_quallity_paths = [
    os.path.join('..', 'Data','Air_quallity','historical_air_quallity_data_Ni.csv'),
    os.path.join('..', 'Data','Air_quallity','historical_air_quallity_data_PM25.csv'),
    os.path.join('..', 'Data','Air_quallity','historical_air_quallity_data_PM10.csv')
    ]

path_to_surface = os.path.join('..','Data','SUPERFICIE','MUNICIPIOS.csv')
df_surface = pl.read_csv(path_to_surface, separator=';')

df_surface = df_surface.with_columns([
    pl.col("COD_INE").cast(str).str.slice(0, 2).cast(pl.Int32).alias("PROVINCIA"),
    pl.col("COD_INE").cast(str).str.slice(2, 3).cast(pl.Int32).alias("MUNICIPIO"),
    pl.col("SUPERFICIE").str.replace(",", ".").cast(pl.Float64)
]).drop('COD_INE')

area_df = df_cities.join(
    df_surface,
    on=["PROVINCIA", "MUNICIPIO"],
    how="inner"
).rename({'SUPERFICIE':'CITY_AREA'}).select(["CITY", "CITY_AREA"])

fleet = pl.read_csv(fleet_distribution, separator='|').cast({'date': pl.Date})
covid_inferior = datetime.date(2023,1,1)
covid_superior = datetime.date(2023,12,1)
fleet = fleet.filter(~
                     ((pl.col("date") >= covid_inferior) &
                      (pl.col("date") <= covid_superior))
                      )

for air_quallity_path in air_quallity_paths:
    pollutant = air_quallity_path.split('_')[-1][:-4]
    output_name = 'dataset'+'_'+pollutant+'.parquet'
    output_file_path = os.path.join('..','Data','Final_Dataset',output_name)

    air_quallity = pl.read_csv(air_quallity_path, separator= '|').cast({'date': pl.Date})
    dataset = fleet.join(air_quallity, on=['date','CITY']).sort('date')

    dataset = dataset.with_columns(pl.col("date").dt.year().alias("year"))

    path = os.path.join('..','Data','INE')
    
    files = sorted([f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))])
    files_path = [os.path.join(path,f) for f in files]

    population = {city : [] for city in cities}
    population['year'] =  [2015,2016,2017,2018,2019,2020,2021,2022,2023]
    for file_path in files_path:
        df = pl.read_excel(file_path)
        df = df.rename({df.columns[0]: 'PROVINCIA',
                        df.columns[2]: 'MUNICIPIO',
                        df.columns[4]: 'POPULATION'})

        df = df.select(['PROVINCIA','MUNICIPIO','POPULATION'])
        df = df.remove(pl.col('PROVINCIA') == "CPRO")
        df = df.cast({'PROVINCIA': pl.Int32,
                    'MUNICIPIO': pl.Int32,
                    'POPULATION': pl.Int32})
        for city in cities.keys():
            pop = df.filter(
                (pl.col('PROVINCIA') == cities[city][0]) &
                (pl.col('MUNICIPIO') == cities[city][1])
            )['POPULATION'][0]
            population[city].append(pop)

    population_long = pl.DataFrame(population).melt(id_vars="year", variable_name="CITY", value_name="POPULATION")

    dataset = dataset.join(population_long, on=['CITY','year'])

    dataset = dataset.join(area_df, on="CITY", how="left")

    dataset = dataset.with_columns([
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.year().alias("year"),
        (np.sin(2 * np.pi * pl.col("date").dt.month() / 12)).alias("month_sin"),
        (np.cos(2 * np.pi * pl.col("date").dt.month() / 12)).alias("month_cos")
    ])


    dataset = dataset.with_columns(
        (pl.col("EURO_1") + pl.col("EURO_2") + pl.col("EURO_3") +
        pl.col("EURO_4") + pl.col("EURO_5") + pl.col("EURO_6") +
        pl.col("EURO_CLEAN") + pl.col("Previous")).alias("TotalFleet")
    )

    dataset = dataset.with_columns(
        (pl.col("TotalFleet") / pl.col("CITY_AREA")).alias(f"CARS_PER_KM2"),
        (pl.col("EURO_1") / pl.col("CITY_AREA")).alias(f"EURO_1_PER_KM2"),
        (pl.col("EURO_2") / pl.col("CITY_AREA")).alias(f"EURO_2_PER_KM2"),
        (pl.col("EURO_3") / pl.col("CITY_AREA")).alias(f"EURO_3_PER_KM2"),
        (pl.col("EURO_4") / pl.col("CITY_AREA")).alias(f"EURO_4_PER_KM2"),
        (pl.col("EURO_5") / pl.col("CITY_AREA")).alias(f"EURO_5_PER_KM2"),
        (pl.col("EURO_6") / pl.col("CITY_AREA")).alias(f"EURO_6_PER_KM2"),
        (pl.col("EURO_CLEAN") / pl.col("CITY_AREA")).alias(f"EURO_CLEAN_PER_KM2"),
        (pl.col("Previous") / pl.col("CITY_AREA")).alias(f"Previous_PER_KM2"),

        (pl.col("POPULATION") / pl.col("CITY_AREA")).alias(f"Population_density"),

        (pl.col("TotalFleet") / pl.col("POPULATION")).alias(f"CARS_PER_CAPITA"),
        (pl.col("EURO_1") / pl.col("POPULATION")).alias(f"EURO_1_PER_CAPITA"),
        (pl.col("EURO_2") / pl.col("POPULATION")).alias(f"EURO_2_PER_CAPITA"),
        (pl.col("EURO_3") / pl.col("POPULATION")).alias(f"EURO_3_PER_CAPITA"),
        (pl.col("EURO_4") / pl.col("POPULATION")).alias(f"EURO_4_PER_CAPITA"),
        (pl.col("EURO_5") / pl.col("POPULATION")).alias(f"EURO_5_PER_CAPITA"),
        (pl.col("EURO_6") / pl.col("POPULATION")).alias(f"EURO_6_PER_CAPITA"),
        (pl.col("EURO_CLEAN") / pl.col("POPULATION")).alias(f"EURO_CLEAN_PER_CAPITA"),
        (pl.col("Previous") / pl.col("POPULATION")).alias(f"Previous_PER_CAPITA"),

        (pl.col("EURO_1") / pl.col("TotalFleet")).alias(f"EURO_1_PROPORTION"),
        (pl.col("EURO_2") / pl.col("TotalFleet")).alias(f"EURO_2_PROPORTION"),
        (pl.col("EURO_3") / pl.col("TotalFleet")).alias(f"EURO_3_PROPORTION"),
        (pl.col("EURO_4") / pl.col("TotalFleet")).alias(f"EURO_4_PROPORTION"),
        (pl.col("EURO_5") / pl.col("TotalFleet")).alias(f"EURO_5_PROPORTION"),
        (pl.col("EURO_6") / pl.col("TotalFleet")).alias(f"EURO_6_PROPORTION"),
        (pl.col("EURO_CLEAN") / pl.col("TotalFleet")).alias(f"EURO_CLEAN_PROPORTION"),
        (pl.col("Previous") / pl.col("TotalFleet")).alias(f"Previous_PROPORTION"),
        )

    dataset = dataset.sort(["CITY", "date"])

    #for pollutant in ["MONTHLY[Ni]", "MONTHLY[PM10]", "MONTHLY[PM2.5]"]:
    #    dataset = dataset.with_columns(
    #        pl.col(pollutant).shift(1).over("CITY").alias(f"{pollutant}_lag1"),
    #        pl.col(pollutant).shift(2).over("CITY").alias(f"{pollutant}_lag2"),
    #        pl.col(pollutant).shift(3).over("CITY").alias(f"{pollutant}_lag3")
    #    )
    dataset = dataset.drop_nulls()
    dataset.write_parquet(output_file_path)