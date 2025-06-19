The code in this repository aims to predict the air quallity in urban areas from data of the vehcile fleet.


# Exctracting the distribution of the vehcile fleet each month
The spanish organism in charge of everything related to vehicles, the DGT (Dirección General de Tráfico), offers a wide range of data but the one we have only used three types of data provided by them:

- A file containing the information of each registered car in one specific time (as of today's date) the end of 2023
- Monthly updates of which cars where registred each month from December 2014 untill the present
- Monthly updates of which cars where de-registred each month from December 2014 untill the present

With this data we are able to re-create how the vehicle fleet looked like at any moment from December 2014 untill the present but in this current implementation we have only considered the data from December 2014 untill the date of the file containing the information of the whole vehicle fleet (December 2023). 

All the data treatment has been performed using the Python library [polars](https://pola.rs). Since polars was designed with performance in mind is one of the fastest Python libraries to perform Data Analysis and treatment and in this project we deal with big amounts of data we decided to use polars instead of a more traditional library sucha s pandas.

It is important to note that in the code used in this repo we refeer to the car registartions as  _mat_ or _matriculaciones_ and to the de-registrations as _baja_ or _bajas_, these are the spanish translations of car registratrion and car de-registartion and have been used because of the greater difference betwween _matricualaciones_ and _bajas_ when compared to registartion and de-registartion.

## Download of the necessary Data
In order to automatically download all the necesseary Data from DGT the download_data.py must be executed.
With this script we can choose which data we want to download by specifying a keyword as a command-line argument:

- Vehicle registrations (`mat`)
- Vehicle de-registrations (`bajas`)
- Or all of them at once (`all`)

For example, to download all files:
```bash
python3 download_script.py all
```

## Pre-processing of the necessary Data
Because we use real-world data with information of the vehicles from a whole country, the raw data need to be cleaned in order to delete irregularities and exctarct the useful information form our pourpose.

### Data cleaning of the exact vehicle fleet file
The documentation provided by the DGT of this file can be found in this [link](https://www.dgt.es/export/sites/web-DGT/.galleries/downloads/dgt-en-cifras/publicaciones/parque-de-vehiculos/Interfaz-de-Salida-Fichero-Parque-Anual.pdf). And the cleaning of this data is performed in the file fleet_data_cleaning.py.

The code in this file:
- Maintains format unformity by setting all the deicmal delimiters to points
- Correcting format errors, such as deleting unncecessary spaces and incorrect characters
- Giving a more clear encoding to PROPULSION column to working better with it later
- Correcting the emisions of vehciles that are motorless

### Data cleaning of the tramit files


## Obtain the vehcile fleet distributio
