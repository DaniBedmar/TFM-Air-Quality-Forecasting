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
    files = sorted([f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir,f))])
    meta_info_file = [f for f in files if f.startswith('M')][0]
    files.remove('.DS_Store')
    files.remove(meta_info_file)
    year = dir[-4:]
    valid_stations_aux = valid_stations.filter(pl.col('year') == year)

    for j,file in enumerate(files):
        polutant = file.split('_')[0]
        file_path = os.path.join(dir,file)
        
        if j == 0:
            data1 = filter_pollutant(file_path,valid_stations_aux,polutant,0.25)

        elif j == 1:
            data2 = filter_pollutant(file_path,valid_stations_aux,polutant,0.25)

        elif j == 2:
            data3 = filter_pollutant(file_path,valid_stations_aux,polutant,0.25)

    if i == 0:
        final_data1, final_data2, final_data3 = data1,data2,data3
    else:
        final_data1, final_data2, final_data3 = pl.concat([final_data1,data1]),pl.concat([final_data2,data2]),pl.concat([final_data3,data3])

final_data1_path = os.path.join('..','Data','Air_quallity','historical_air_quallity_data_NI.csv')
final_data2_path = os.path.join('..','Data','Air_quallity','historical_air_quallity_data_PM10.csv')
final_data3_path = os.path.join('..','Data','Air_quallity','historical_air_quallity_data_PM25.csv')
final_data1.write_csv(final_data1_path, separator='|')
final_data2.write_csv(final_data2_path, separator='|')
final_data3.write_csv(final_data3_path, separator='|')