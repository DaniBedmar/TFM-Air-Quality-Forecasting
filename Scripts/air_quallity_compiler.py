import os
import polars as pl

from utils.functions import get_valid_stations,filter_pollutant

air_path = os.path.join('..','Data','Air_quallity')
dirs = sorted([f for f in os.listdir(air_path) if os.path.isdir(os.path.join(air_path,f))])
dirs_path = [os.path.join(air_path,f) for f in dirs]

for i,dir in enumerate(dirs_path):
    files = sorted([f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir,f))])
    meta_info_file = [f for f in files if f.startswith('M')][0]
    path_to_meta = os.path.join(dir,meta_info_file)
    if i == 0:
        valid_stations = get_valid_stations(path_to_meta)
    else:
        valid_stations = pl.concat([valid_stations, get_valid_stations(path_to_meta)])

valid_stations = valid_stations.explode("ESTACIONES")
valid_stations = valid_stations.rename({"ESTACIONES": "ESTACION"})
valid_stations_path = os.path.join('..','Data','Air_quallity','valid_stations.csv')
valid_stations.write_csv(valid_stations_path,separator='|')

for i,dir in enumerate(dirs_path):
    print(f'Processing the year {dir}')
    files = sorted([f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir,f))])
    meta_info_file = [f for f in files if f.startswith('M')][0]
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    files.remove(meta_info_file)
    year = dir[-4:]
    valid_stations_aux = valid_stations.filter(pl.col('year') == year)

    for j,file in enumerate(files):
        print(f'Processing {file}')
        polutant = file.split('_')[0]
        file_path = os.path.join(dir,file)
        
        if j == 0:
            data = filter_pollutant(file_path,valid_stations_aux,polutant,0.25)

        else:
            aux_data = filter_pollutant(file_path,valid_stations_aux,polutant,0.25)
            data = data.join(aux_data, on = ['date','CITY'], how='left')

    if i == 0:
        final_data = data
    else:
        final_data =  pl.concat([final_data,data])

final_data_path = os.path.join('..','Data','Air_quallity','historical_air_quallity_data.csv')
final_data.write_csv(final_data_path, separator='|')