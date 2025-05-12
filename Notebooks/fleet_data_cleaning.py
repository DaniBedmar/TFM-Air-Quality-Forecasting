import polars as pl
import os

from utils.dictionaries import types_parque_original,propulsion_mapping,mapping
sub_tipo_mapping = mapping['SUBTIPO_DGT']
                           
path = os.path.join("..","Data", "DGT")
complete_park = os.path.join(path,'Parque_exacto','mat_2023.txt')
clean_park = os.path.join(path,'Parque_exacto','clean_park.csv')
               
parque = pl.scan_csv(complete_park, separator='|', schema=types_parque_original)

parque = parque.filter(pl.col("FECHA_MATR") >= pl.col("FECHA_PRIM_MATR")).with_columns(
    pl.col('POTENCIA').str.replace(",", ".").cast(pl.Float64),
    pl.col('KW').str.replace(",", ".").cast(pl.Float64),
    pl.col('SUBTIPO_DGT').replace(sub_tipo_mapping).alias("SUBTIPO_DGT"),
    pl.col('PROPULSION').replace(propulsion_mapping).alias("PROPULSION"),
    pl.col('AUTONOMIA').str.replace(" ", "").str.replace(",", ".").str.replace("0000Û0","000000").cast(pl.Float64))

remolques = []
for type in list(sub_tipo_mapping.values()):
    if 'REMOLQUE' in type:
        remolques.append(type)

inverse_sub_tipo_mapping = {v: k for k, v in sub_tipo_mapping.items()}

parque = parque.filter(~pl.col("SUBTIPO_DGT").is_in(remolques))

#parque.with_columns(pl.when((pl.col("SUBTIPO_DGT").is_in(remolques)) |
#                            (pl.col('CLASE_MATR') == "Remolque"))
#                            .then(pl.lit("NO_EMISIONS"))
#                            .otherwise(pl.col("EMISIONES_EURO"))
#                            .alias("EMISIONES_EURO"),
#                    pl.col('SUBTIPO_DGT').replace(inverse_sub_tipo_mapping).alias("SUBTIPO_DGT"))

park = parque.collect()
park.write_csv(clean_park,separator='|')