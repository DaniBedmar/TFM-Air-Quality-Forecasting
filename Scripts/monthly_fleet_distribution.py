import os
import polars as pl
import datetime
import fastexcel
from calendar import monthrange

from utils.functions import get_valid_stations,filter_pollutant,simplify_euro_emissions,tramit_file_reader
from utils.dictionaries import types_fleet_post

valid_stations_path = os.path.join('..','Data','Air_quallity','valid_stations.csv')
clearn_park_path = os.path.join('..','Data','DGT','Exact_fleet','clean_park.csv')
bajas_dir_path = os.path.join('..','Data','DGT','bajas')

common_cols_tramites = ['FEC_MATRICULA','FEC_PRIM_MATRICULACION','COD_PROPULSION_ITV','COD_MUNICIPIO_INE_VEH']
common_cols_mapping = {"FEC_MATRICULA": "FECHA_MATR", "FEC_PRIM_MATRICULACION": "FECHA_PRIM_MATR", 
                       "COD_PROPULSION_ITV": "PROPULSION", "COD_MUNICIPIO_INE_VEH": "MUNICIPIO"}
necessary_cols = ['FECHA_PRIM_MATR','FECHA_MATR','PROPULSION','MUNICIPIO']

valid_stations = pl.read_csv(valid_stations_path, separator='|')
park = pl.scan_csv(clearn_park_path,separator='|', schema=types_fleet_post).select(necessary_cols)
park = park.collect().rename({'MUNICIPIO': 'MUNICIPIO_COD'})
cities = pl.read_csv(valid_stations_path, separator='|', schema_overrides={'MUNICIPIO_COD': pl.String})
cities = cities.select(["MUNICIPIO_COD", "CITY"]).unique()
park = park.join(cities,on="MUNICIPIO_COD",how="inner").drop('MUNICIPIO_COD')
park = simplify_euro_emissions(park).drop('PROPULSION')
park = park.with_columns(pl.lit(datetime.date(2023,12,1)).alias("DATE_TRAMIT"))

bajas_files = [
    f for f in os.listdir(bajas_dir_path)
    if os.path.isfile(os.path.join(bajas_dir_path, f)) and f.lower().endswith(".txt")
]

first_file_path = os.path.join(bajas_dir_path,bajas_files[0])
file_bajas = tramit_file_reader(first_file_path).select(common_cols_tramites).rename(common_cols_mapping).rename({'MUNICIPIO': 'MUNICIPIO_COD'})
file_bajas = file_bajas.join(cities, on='MUNICIPIO_COD', how='inner').drop('MUNICIPIO_COD')
file_bajas = simplify_euro_emissions(file_bajas).drop('PROPULSION')
dato = bajas_files[0][-10:-4]
dato = datetime.date(int(dato[:4]),int(dato[-2:]),1)
file_bajas = file_bajas.with_columns(
    pl.lit(dato).alias("DATE_TRAMIT"))

for i in range(1,len(bajas_files)):
    file_path = os.path.join(bajas_dir_path,bajas_files[i])
    new_file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping).rename({'MUNICIPIO': 'MUNICIPIO_COD'})
    new_file = new_file.join(cities, on='MUNICIPIO_COD', how='inner').drop('MUNICIPIO_COD')
    new_file = simplify_euro_emissions(new_file).drop('PROPULSION')
    dato = bajas_files[i][-10:-4]
    dato = datetime.date(int(dato[:4]),int(dato[-2:]),1)
    new_file = new_file.with_columns(
        pl.lit(dato).alias("DATE_TRAMIT")
        )
    
    file_bajas = pl.concat([file_bajas,new_file])

file_bajas = file_bajas.select(['FECHA_PRIM_MATR', 'FECHA_MATR', 'CITY', 'Simplified_EURO','DATE_TRAMIT'])
file = pl.concat([park,file_bajas])
file_path = os.path.join('..','Data','DGT','Exact_fleet','park_w_deregisters.csv')
file.write_csv(file_path,separator='|')

dates = []
date = datetime.date(2015, 1, 1)
while date <= datetime.date(2023, 12, 1):
    dates.append(date)
    if date.month == 12:
        date = datetime.date(date.year + 1, 1, 1)
    else:
        date = datetime.date(date.year, date.month + 1, 1)

rows = []
date_exact = datetime.date(2023, 12, 1)
all_classes = sorted(file['Simplified_EURO'].unique().to_list())

for date in dates:
    print(date)
    for city in file['CITY'].unique().to_list():
        selected = file.filter(
            (pl.col('FECHA_MATR') < date) &
            (pl.col('CITY') == city) &
            ((pl.col('DATE_TRAMIT') == date_exact) |
             ((pl.col('DATE_TRAMIT') < date_exact) & (pl.col('DATE_TRAMIT') >= date))
            ))
        
        vc = selected['Simplified_EURO'].value_counts()
        class_counts = dict(zip(vc['Simplified_EURO'].to_list(), vc['count'].to_list()))
        
        row = {'date': date, 'CITY': city}
        for euro_class in all_classes:
            row[euro_class] = class_counts.get(euro_class, 0)
        
        rows.append(row)

monthly_distribution = pl.DataFrame(rows)

column_order = ['date', 'CITY'] + all_classes
monthly_distribution = monthly_distribution.select(column_order)

monthly_distribution
file_path = os.path.join('..', 'Data', 'DGT', 'Exact_fleet', 'monthly_park_distribution.csv')
monthly_distribution.write_csv(file_path, separator='|')