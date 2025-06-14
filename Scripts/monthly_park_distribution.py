import os
import datetime
import polars as pl

from utils.dictionaries import types_fleet_post
from utils.functions import simplify_euro_emissions,tramit_file_reader
clearn_park_path = os.path.join('..','Data','DGT','Exact_fleet','clean_park.csv')
bajas_dir_path = os.path.join('..','Data','DGT','bajas')

common_cols_tramites = ['FEC_MATRICULA','FEC_PRIM_MATRICULACION','COD_PROPULSION_ITV','COD_MUNICIPIO_INE_VEH']
common_cols_mapping = {"FEC_MATRICULA": "FECHA_MATR", "FEC_PRIM_MATRICULACION": "FECHA_PRIM_MATR", 
                       "COD_PROPULSION_ITV": "PROPULSION", "COD_MUNICIPIO_INE_VEH": "MUNICIPIO"}
necessary_cols = ['FECHA_PRIM_MATR','FECHA_MATR','PROPULSION','MUNICIPIO']

cities = pl.DataFrame({
    "MUNICIPIO": [
        "08019",  # Barcelona
        "46250",  # València
        "48020",  # Bilbao
        "28079",  # Madrid
        "41091",  # Sevilla
        "50297",  # Zaragoza
        "29067",  # Málaga
        "30030",  # Murcia
        "07040",  # Palma de Mallorca
        "35016",  # Las Palmas de G.C.
        "03014",  # Alicante
        "14021",  # Córdoba
        "47186",  # Valladolid
        "36057",  # Vigo
        "33024",  # Gijón
        "01059",  # Vitoria-Gasteiz
        "03065",  # Elche
        "18087",  # Granada
        "08279",  # Terrassa
        "08187",  # Sabadell
        "33044",  # Oviedo
        "31201",  # Pamplona
        "04013",  # Almería
    ],
    "CITY": ["BCN","VLC","BILB","MAD","SEV","ZAR","MAL","MURC","MALL","PGC",
        "ALIC","COR","VALL","VIG","GIJ","VIT","ELCH","GRAN","TERR","SAB",
        "OVI","PAMP","ALM"]
})

park = pl.scan_csv(clearn_park_path,separator='|', schema=types_fleet_post).select(necessary_cols)
park = park.collect()
park = park.join(cities, on='MUNICIPIO', how='inner').drop('MUNICIPIO')
park = simplify_euro_emissions(park).drop('PROPULSION')
park = park.with_columns(pl.lit(datetime.date(2023,12,1)).alias("FEC_TRAMITE"))

bajas_files = [
    f for f in os.listdir(bajas_dir_path)
    if os.path.isfile(os.path.join(bajas_dir_path, f)) and f.lower().endswith(".txt")
]

first_file_path = os.path.join(bajas_dir_path,bajas_files[0])
file_bajas = tramit_file_reader(first_file_path).select(common_cols_tramites).rename(common_cols_mapping)
file_bajas = file_bajas.join(cities, on='MUNICIPIO', how='inner').drop('MUNICIPIO')
file_bajas = simplify_euro_emissions(file_bajas).drop('PROPULSION')
dato = bajas_files[0][-10:-4]
dato = datetime.date(int(dato[:4]),int(dato[-2:]),1)
file_bajas = file_bajas.with_columns(
    pl.lit(dato).alias("FEC_TRAMITE"))

for i in range(1,len(bajas_files)):
    file_path = os.path.join(bajas_dir_path,bajas_files[i])
    new_file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
    new_file = new_file.join(cities, on='MUNICIPIO', how='inner').drop('MUNICIPIO')
    new_file = simplify_euro_emissions(new_file).drop('PROPULSION')
    dato = bajas_files[i][-10:-4]
    dato = datetime.date(int(dato[:4]),int(dato[-2:]),1)
    new_file = new_file.with_columns(
        pl.lit(dato).alias("FEC_TRAMITE")
        )
    
    file_bajas = pl.concat([file_bajas,new_file])

file_bajas = file_bajas.select(['FECHA_PRIM_MATR', 'FECHA_MATR', 'CITY', 'Simplified_EURO','FEC_TRAMITE'])
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

necessary_cols = ['FECHA_PRIM_MATR', 'FECHA_MATR', 'CITY', 'Simplified_EURO', 'FEC_TRAMITE']
schema_file = {
    'FECHA_PRIM_MATR': pl.Date,
    'FECHA_MATR': pl.Date,
    'CITY': pl.String,
    'Simplified_EURO': pl.String,
    'FEC_TRAMITE': pl.Date
}
file_path = os.path.join('..', 'Data', 'DGT', 'Exact_fleet', 'park_w_deregisters.csv')
file = pl.scan_csv(file_path, separator='|', schema=schema_file).collect()

all_classes = sorted(file['Simplified_EURO'].unique().to_list())

rows = []
date_exact = datetime.date(2023, 12, 1)

for date in dates:
    for city in file['CITY'].unique().to_list():
        selected = file.filter(
            (pl.col('FECHA_MATR') < date) &
            (pl.col('CITY') == city) &
            ((pl.col('FEC_TRAMITE') == date_exact) |
             ((pl.col('FEC_TRAMITE') < date_exact) & (pl.col('FEC_TRAMITE') >= date))
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