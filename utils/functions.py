import os
import zipfile
import datetime
import requests
import fastexcel
import polars as pl
from io import BytesIO
from bs4 import BeautifulSoup
from calendar import monthrange

from utils.dictionaries import tramit_file_columns,tramit_file_index
        
def download_mat(path = os.path.join('..','Data', 'DGT', 'mat')):
    print('Let\'s proceed to download all the registrations')
    url = 'https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/matriculaciones-automoviles-mensual.html'

    os.makedirs(path, exist_ok=True)

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    for li in soup.find_all('li', class_='list-group-item'):
        a_tag = li.find('a')
        if a_tag and a_tag['href'].endswith('.zip'):
            file_url = a_tag['href']
            if file_url.startswith('/'):
                file_url = 'https://www.dgt.es' + file_url

            file_name = file_url.split('/')[-1]
            print(f'Downloading and extracting {file_name}...')

            zip_response = requests.get(file_url)
            zip_file = zipfile.ZipFile(BytesIO(zip_response.content))

            zip_file.extractall(path)

def download_bajas(path = os.path.join('..','Data', 'DGT', 'bajas')):
    print('Let\'s proceed to download all the de-registrations')
    url = 'https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/bajas-automoviles-mensual.html'

    os.makedirs(path, exist_ok=True)

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    for li in soup.find_all('li', class_='list-group-item'):
        a_tag = li.find('a')
        if a_tag and a_tag['href'].endswith('.zip'):
            file_url = a_tag['href']
            if file_url.startswith('/'):
                file_url = 'https://www.dgt.es' + file_url

            file_name = file_url.split('/')[-1]
            print(f'Downloading and extracting {file_name}...')

            zip_response = requests.get(file_url)
            zip_file = zipfile.ZipFile(BytesIO(zip_response.content))

            zip_file.extractall(path)

def download_fleet(path = os.path.join('..','Data', 'DGT', 'Exact_fleet')):
    print('Let\'s proceed to download the exact vehicle fleet ')

    url= 'https://www.dgt.es/menusecundario/dgt-en-cifras/dgt-en-cifras-resultados/dgt-en-cifras-detalle/Microdatos-de-parque-de-vehiculos-anual/'

    os.makedirs(path, exist_ok=True)

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    descarga_links = soup.find_all('a', id='descarga')

    if descarga_links:
        for a_tag in descarga_links:
            if 'href' in a_tag.attrs and a_tag['href'].strip().endswith('.zip'):
                file_url = a_tag['href'].strip()

                print(f'Downloading and extracting {file_url}...')

                zip_response = requests.get(file_url)
                zip_file = zipfile.ZipFile(BytesIO(zip_response.content))

                zip_file.extractall(path)
                break

def tramit_file_reader(file_path, cols_to_keep = tramit_file_columns):
    
    col_widths = tramit_file_index
    col_names = tramit_file_columns

    slice_tuples = []
    offset = 0

    for i, separation in enumerate(col_widths):
        start = offset
        end = separation
        slice_tuples.append((start, end,col_names[i]))
        offset += separation

    df = pl.read_csv(
        file_path,
        has_header=False,
        encoding='ISO-8859-1',
        truncate_ragged_lines=True,
        new_columns=["full_str"],
        skip_rows=1
    )

    exprs = [
        pl.col("full_str")
        .str.slice(start, end)
        .str.strip_chars()
        .alias(col_name)
        for start, end, col_name in slice_tuples
    ]

    df = df.with_columns(exprs).select(cols_to_keep)
    df = df.with_columns(pl.col("FEC_MATRICULA","FEC_PRIM_MATRICULACION").str.to_date("%d%m%Y", strict=False))
    return df

def dates_range(start, end,type):
    dates = []
    current = datetime.date(start.year, start.month, 1)
    while current <= end:
        if (current == datetime.date(2023, 12, 1)):
            current = datetime.date(current.year + 1, 1, 1)
            continue

        if type == 'mat':
            dates.append(f"export_mensual_mat_{current.year}{current.month:02d}.txt")

        elif type == 'bajas':
            dates.append(f"export_mensual_bajas_{current.year}{current.month:02d}.txt")

        if current.month == 12:
            current = datetime.date(current.year + 1, 1, 1)
        else:
            current = datetime.date(current.year, current.month + 1, 1)
    return dates

def simplify_euro_emissions(df):
    df = df.with_columns([
        pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
        .otherwise(pl.col("FECHA_PRIM_MATR"))
        .alias("FECHA_PRIM_MATR")])
    
    df = df.with_columns([
        pl.when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(2015, 9, 1))).then(pl.lit("EURO_6"))
        .when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(2011, 1, 1))).then(pl.lit("EURO_5"))
        .when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(2006, 1, 1))).then(pl.lit("EURO_4"))
        .when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(2001, 1, 1))).then(pl.lit("EURO_3"))
        .when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(1997, 1, 1))).then(pl.lit("EURO_2"))
        .when(pl.col("FECHA_PRIM_MATR") >= pl.lit(datetime.date(1992, 12, 31))).then(pl.lit("EURO_1"))
        .when(pl.col("FECHA_PRIM_MATR") < pl.lit(datetime.date(1992, 12, 31))).then(pl.lit("Previous"))
        .otherwise(pl.lit("Unkwown"))
        .alias("Simplified_EURO")
        ])
    
    df = df.with_columns(
        pl.when((pl.col("PROPULSION") == "ELEC") | (pl.col("PROPULSION") == "H"))
        .then(pl.lit("EURO_CLEAN"))
        .otherwise(pl.col("Simplified_EURO"))
        .alias("Simplified_EURO")
        )

    return df

def get_cars(fecha,park,
             mat_path = os.path.join("..","Data", "DGT",'matr'),
             bajas_path = os.path.join("..","Data", "DGT",'bajas')):
    '''
    fecha : date in string with format %d%m%Y
    ''' 
    fecha_foto = datetime.date(2023,12,1)
    fecha = datetime.datetime.strptime(fecha, "%d%m%Y").date()
    common_cols_tramites = ['FEC_MATRICULA','FEC_PRIM_MATRICULACION','COD_PROPULSION_ITV']
    common_cols_mapping = {
        "FEC_MATRICULA": "FECHA_MATR",
        "FEC_PRIM_MATRICULACION": "FECHA_PRIM_MATR",
        "COD_PROPULSION_ITV": "PROPULSION"}

    print(f'Fecha seleccionada: {fecha}, fecha de la foto: {fecha_foto}')
    if fecha == fecha_foto:
        #Todos los coches en parque_exacto
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        print('La fecha coincide con la fecha de la foto')
        return park_distribution
    
    elif fecha > fecha_foto:
        # Todos los coches en parque_exacto con t_mat <= fecha (en teoria todos)+
        # todas las matr entre (parque_exacto, fecha) con t_mat > fecha_foto (realmente que datetime.date(2023,12,31) (en teoria todos) -
        # todas las bajas entre (parque_exacto, fecha) con fecha_mat <= fecha
        print('Estamos mirando una fecha posterior a la foto')
        print('Este es el parque exacto:')
        print(park)
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        print(park_distribution)
        print('Vamos a tener en cuenta los siguiuentes ficheros de matriculaciones')
        mat_files = dates_range(fecha_foto,fecha,'mat')
        print(mat_files)
        print('Vamos a tener en cuenta los siguiuentes ficheros de bajas')
        bajas_files = dates_range(fecha_foto,fecha,'bajas')
        print(bajas_files)
        print('Miremos las matriculaciones')
        for file in mat_files:
            print(f'Miremos el fichero {file}')
            file_path = os.path.join(mat_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            print('Este es el fichero:')
            print(file)
            file.filter(pl.col('FECHA_MATR') > datetime.date(2023,12,31))
            print('Este es el fichero despues de filtrar las matriculas despues de la foto:')
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file)
            print(file)
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            print(file)
            print('Recordemos que el parque era:')
            print(park_distribution)
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))
            print('Despues de sumar:')
            print(park_distribution)

        print('Miremos las bajas')
        for file in bajas_files:
            park_distribution = park_distribution.with_columns(
                pl.col("count").cast(pl.Int64)
                )
            print(f'Miremos el fichero {file}')
            file_path = os.path.join(bajas_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            print('Este es el fichero:')
            print(file)
            file.filter(pl.col("FECHA_MATR") <= fecha)
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file)
            print('Este es el fichero despues de filtrar:')
            print(file)
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            print(file)
            print('tenemos que restar asi que invertimos:')
            file = file.with_columns((pl.col("count") * -1).alias("count"))
            print(file)
            print('Recordemos que parque + matriculaciones es:')
            print(park_distribution)
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))   
            print('Despues de restar:')        
            print(park_distribution)
        return park_distribution

    elif  fecha < fecha_foto:
        # Todos los coches en parque_exacto con fecha_mat <= fecha  +
        # Todas las bajas entre fehca <= t_baja < fecha_foto con  t_mat <= fecha
        print('Estamos mirando una fecha anterior a la foto')
        print('Este es el parque en la foto exacta:')
        print(park)
        park = park.filter(pl.col("FECHA_MATR") <= fecha)
        print('Y este es el parque cosniderando solo coches matriculados antes o en la fecha seleccionada:')
        print(park)
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        print(park_distribution)
        bajas_files = dates_range(fecha,fecha_foto,'bajas')
        print('Consideramos que tendremos que mirar los siguientes archivos de bajas:')
        print(bajas_files)
        print('Miremos als bajas')
        for file in bajas_files:
            print(f'En el caso del fichero {file}:')
            file_path = os.path.join(bajas_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            print('Estas son las bajas:')
            print(file)
            file.filter(pl.col("FECHA_MATR") <= fecha)
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file).drop('PROPULSION')
            print('Y estas son las bajas despues de filtrar solo las de coches con matriculacion anterioir a la fecha:')
            print(file)
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            print(file)
            print('Recordemos que el parque era:')
            print(park_distribution)
            print('Despues de sumar:')
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))
            print(park_distribution)

        return park_distribution
    
def get_cars_(fecha,park,
             mat_path = os.path.join("..","Data", "DGT",'matr'),
             bajas_path = os.path.join("..","Data", "DGT",'bajas')):
    '''
    fecha : date in string with format %d%m%Y
    ''' 
    fecha_foto = datetime.date(2023,12,1)
    fecha = datetime.datetime.strptime(fecha, "%d%m%Y").date()
    common_cols_tramites = ['FEC_MATRICULA','FEC_PRIM_MATRICULACION','COD_PROPULSION_ITV']
    common_cols_mapping = {
        "FEC_MATRICULA": "FECHA_MATR",
        "FEC_PRIM_MATRICULACION": "FECHA_PRIM_MATR",
        "COD_PROPULSION_ITV": "PROPULSION"}

    if fecha == fecha_foto:
        #Todos los coches en parque_exacto
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        return park_distribution
    
    elif fecha > fecha_foto:
        # Todos los coches en parque_exacto con t_mat <= fecha (en teoria todos)+
        # todas las matr entre (parque_exacto, fecha) con t_mat > fecha_foto (realmente que datetime.date(2023,12,31) (en teoria todos) -
        # todas las bajas entre (parque_exacto, fecha) con fecha_mat <= fecha
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        mat_files = dates_range(fecha_foto,fecha,'mat')
        bajas_files = dates_range(fecha_foto,fecha,'bajas')
        for file in mat_files:
            file_path = os.path.join(mat_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            file.filter(pl.col('FECHA_MATR') > datetime.date(2023,12,31))
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file)
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))

        for file in bajas_files:
            park_distribution = park_distribution.with_columns(
                pl.col("count").cast(pl.Int64)
                )
            file_path = os.path.join(bajas_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            file.filter(pl.col("FECHA_MATR") <= fecha)
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file)
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            file = file.with_columns((pl.col("count") * -1).alias("count"))
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))   
        return park_distribution

    elif  fecha < fecha_foto:
        # Todos los coches en parque_exacto con fecha_mat <= fecha  +
        # Todas las bajas entre fehca <= t_baja < fecha_foto con  t_mat <= fecha
        print('Estamos mirando una fecha anterior a la foto')
        park = park.filter(pl.col("FECHA_MATR") <= fecha)
        park_distribution = park['Simplified_EURO'].value_counts().sort('Simplified_EURO')
        bajas_files = dates_range(fecha,fecha_foto,'bajas')
        mat_files = dates_range(fecha,fecha_foto,'mat')

        for file in bajas_files:
            file_path = os.path.join(bajas_path,file)
            file = tramit_file_reader(file_path).select(common_cols_tramites).rename(common_cols_mapping)
            file.filter(pl.col("FECHA_MATR") <= fecha)
            file = file.with_columns([
                pl.when(pl.col("FECHA_PRIM_MATR").is_null()).then(pl.col("FECHA_MATR"))
                .otherwise(pl.col("FECHA_PRIM_MATR"))
                .alias("FECHA_PRIM_MATR")
                ])
            file = simplify_euro_emissions(file).drop('PROPULSION')
            file = file['Simplified_EURO'].value_counts().sort('Simplified_EURO')
            park_distribution = (
                pl.concat([park_distribution, file])
                .group_by("Simplified_EURO")
                .agg(pl.sum("count"))
                .sort("Simplified_EURO"))

        return park_distribution
    
def get_valid_stations2(file_path,cities):
    valid_stations = {}
    sheet_names = fastexcel.read_excel(file_path).sheet_names

    for sheet in sheet_names:
        if 'staci' in sheet:
            sheet_of_interest = sheet

    data = pl.read_excel(file_path, sheet_name=sheet_of_interest).select(['PROVINCIA','MUNICIPIO','ESTACION','TIPO_ESTACION'])
    data = data.filter(pl.col('TIPO_ESTACION') == 'TRAFICO')

    for city in cities:
        Stations = data.filter(
            (pl.col('PROVINCIA') == cities[city][0]) & (pl.col('MUNICIPIO') == cities[city][1])
            ).get_column('ESTACION').to_list()
        
        valid_stations[city] = Stations

    return valid_stations

def get_valid_stations(file_path):
    sheet_names = fastexcel.read_excel(file_path).sheet_names
    for sheet in sheet_names:
        if 'staci' in sheet:
            sheet_of_interest = sheet
    year = file_path.split('/')[-1].split('.')[0][-4:]

    meta = pl.read_excel(file_path, sheet_name=sheet_of_interest)
    meta = meta.filter(pl.col('TIPO_ESTACION') =='TRAFICO')

    stations = meta.with_columns([
        pl.col("PROVINCIA").cast(pl.Int32),
        pl.col("MUNICIPIO").cast(pl.Int32),
        pl.col("PROVINCIA").cast(str).str.zfill(2).alias("prov_str"),
        pl.col("MUNICIPIO").cast(str).str.zfill(3).alias("muni_str")
    ])

    stations = stations.with_columns([
        (pl.col("prov_str") + pl.col("muni_str")).alias("MUNICIPIO_COD")
    ])

    stations = stations.group_by(["MUNICIPIO_COD", "PROVINCIA", "MUNICIPIO", "N_MUNICIPIO"]).agg([
        pl.col("ESTACION").unique().alias("ESTACIONES")
    ])

    stations = stations.rename({"N_MUNICIPIO": "CITY"})

    stations = stations.select([
        "MUNICIPIO_COD", "PROVINCIA", "MUNICIPIO", "CITY", "ESTACIONES"
    ])
    stations = stations.with_columns([
        pl.lit(year).alias('year')
    ])
    return stations

def filter_pollutant(file_path,valid_stations,coverage_threshold = 0.3):
    if file_path.lower().endswith('.csv'):
        df = pl.read_csv(file_path, separator=';').drop('PUNTO_MUESTREO')
    elif file_path.lower().endswith(('xls','xlsx')):
        df = pl.read_excel(file_path).drop('PUNTO_MUESTREO')
    else:
        raise ValueError("Unsupported file type: must be .csv or .xls/.xlsx")
    
    pollutant = file_path.split('/')[-1].split('_')[0]

    df = df.join(valid_stations, on=['PROVINCIA','MUNICIPIO','ESTACION'], how='inner')
    
    hours = [c for c in df.columns if c.startswith('H')]
    df = df.with_columns(
        pl.concat_list(hours).list.mean().alias('daily_avg')
        ).drop(hours)
    
    if df.is_empty():  
        return pl.DataFrame(schema=['date', 'CITY', f'city_mean_weighted'])
    
    station_month = df.pivot(
        values='daily_avg',
        index=['PROVINCIA','MUNICIPIO','ESTACION','CITY','ANNO','MES'],
        columns='DIA',
        aggregate_function='first'
    ).rename(lambda c: f"D{int(c):02d}" if c.isdigit() else c)

    day_cols = [c for c in station_month.columns if c.startswith('D')]
    valid = station_month.group_by(['CITY','ANNO','MES']).agg([pl.mean(c).alias(c) for c in day_cols])

    measures = valid[day_cols].transpose()
    measure_col = f"MONTHLY{pollutant}_concentration"
    means = measures.mean().transpose().rename({'column_0': measure_col})
    missing = measures.null_count().transpose().rename({'column_0': 'not_valid_days'})
    valid = pl.concat([valid.drop(day_cols), means, missing], how='horizontal')

    valid = valid.with_columns(
        pl.map_batches(
            [pl.col('ANNO'),pl.col('MES')],
            lambda cols: pl.Series([monthrange(int(y),int(m))[1] for y,m in zip(cols[0],cols[1])]),
            return_dtype=pl.Int32
        ).alias('days_in_month'),
        pl.date(
            year=pl.col('ANNO').cast(pl.Int32),
            month=pl.col('MES').cast(pl.Int32),
            day=pl.lit(1)
        ).alias('date')
    ).with_columns(
        ((pl.col('days_in_month')-pl.col('not_valid_days'))/pl.col('days_in_month')).alias('coverage')
    ).drop(['ANNO','MES','days_in_month','not_valid_days'])

    filtered = valid.filter(pl.col('coverage') >= coverage_threshold)
    filtered = filtered.group_by(['date','CITY']).agg((pl.col(measure_col)*pl.col('coverage')).sum()/pl.col('coverage').sum()).sort('date')

    return filtered