import polars as pl

types_fleet_original = {'PROVINCIA': pl.String,
                 'MUNICIPIO': pl.String,
                 'FABRICANTE': pl.String,
                 'MARCA': pl.String,
                 'MODELO': pl.String,
                 'TIPO': pl.String,
                 'VARIANTE': pl.String,
                 'VERSION': pl.String,
                 'PROVINCIA_MATR': pl.String,
                 'FECHA_MATR': pl.Date,
                 'FECHA_PRIM_MATR': pl.Date,
                 'CLASE_MATR': pl.String,
                 'PROCEDENCIA': pl.String,
                 'NUEVO_USADO': pl.String,
                 'TIPO_TITULAR': pl.String,
                 'NUM_TITULARES': pl.Int16,
                 'SUBTIPO_DGT': pl.String,
                 'TIPO_DGT': pl.String,
                 'CAT_EURO': pl.String,
                 'CLAS_CONSTRUCCION': pl.String,
                 'CLAS_UTILIZACION': pl.String,
                 'SERVICIO': pl.String,
                 'RENTING': pl.String,
                 'TARA': pl.Int32,
                 'PESO_MAX': pl.Int32,
                 'MOM': pl.Int32,
                 'MMTA': pl.Int32,
                 'CILINDRADA': pl.Int32,
                 'POTENCIA': pl.String,
                 'KW': pl.String,
                 'PROPULSION': pl.String,
                 'CATELECT': pl.String,
                 'CONSUMO': pl.Int32,
                 'AUTONOMIA': pl.String,
                 'ALIMENTACION': pl.String,
                 'TIPO_DISTINTIVO': pl.String,
                 'EMISIONES_EURO': pl.String,
                 'EMISIONES_CO2': pl.Float32,
                 'CARROCERIA': pl.String,
                 'DISTANCIA_EJES': pl.Int16,
                 'EJE_ANTERIOR': pl.Int16,
                 'EJE_POSTERIOR': pl.Int16,
                 'PLAZAS': pl.Int16,
                 'PLAZAS_MAX': pl.Int16,
                 'PLAZAS_PIE': pl.Int16}

types_fleet_post = {'PROVINCIA': pl.String,
                 'MUNICIPIO': pl.String,
                 'FABRICANTE': pl.String,
                 'MARCA': pl.String,
                 'MODELO': pl.String,
                 'TIPO': pl.String,
                 'VARIANTE': pl.String,
                 'VERSION': pl.String,
                 'PROVINCIA_MATR': pl.String,
                 'FECHA_MATR': pl.Date,
                 'FECHA_PRIM_MATR': pl.Date,
                 'CLASE_MATR': pl.String,
                 'PROCEDENCIA': pl.String,
                 'NUEVO_USADO': pl.String,
                 'TIPO_TITULAR': pl.String,
                 'NUM_TITULARES': pl.Int16,
                 'SUBTIPO_DGT': pl.String,
                 'TIPO_DGT': pl.String,
                 'CAT_EURO': pl.String,
                 'CLAS_CONSTRUCCION': pl.String,
                 'CLAS_UTILIZACION': pl.String,
                 'SERVICIO': pl.String,
                 'RENTING': pl.String,
                 'TARA': pl.Int32,
                 'PESO_MAX': pl.Int32,
                 'MOM': pl.Int32,
                 'MMTA': pl.Int32,
                 'CILINDRADA': pl.Int32,
                 'POTENCIA': pl.Float64,
                 'KW': pl.Float64,
                 'PROPULSION': pl.String,
                 'CATELECT': pl.String,
                 'CONSUMO': pl.Int32,
                 'AUTONOMIA': pl.Float64,
                 'ALIMENTACION': pl.String,
                 'TIPO_DISTINTIVO': pl.String,
                 'EMISIONES_EURO': pl.String,
                 'EMISIONES_CO2': pl.Float32,
                 'CARROCERIA': pl.String,
                 'DISTANCIA_EJES': pl.Int16,
                 'EJE_ANTERIOR': pl.Int16,
                 'EJE_POSTERIOR': pl.Int16,
                 'PLAZAS': pl.Int16,
                 'PLAZAS_MAX': pl.Int16,
                 'PLAZAS_PIE': pl.Int16}

types_tramit_post = {'PROVINCIA': pl.String,
                 'MUNICIPIO': pl.String,
                 'FABRICANTE': pl.String,
                 'MARCA': pl.String,
                 'MODELO': pl.String,
                 'TIPO': pl.String,
                 'VARIANTE': pl.String,
                 'VERSION': pl.String,
                 'PROVINCIA_MATR': pl.String,
                 'FECHA_MATR': pl.Date,
                 'FECHA_PRIM_MATR': pl.Date,
                 'CLASE_MATR': pl.String,
                 'PROCEDENCIA': pl.String,
                 'NUEVO_USADO': pl.String,
                 'TIPO_TITULAR': pl.String,
                 'NUM_TITULARES': pl.Int16,
                 'CAT_EURO': pl.String,
                 'SERVICIO': pl.String,
                 'RENTING': pl.String,
                 'TARA': pl.Int32,
                 'PESO_MAX': pl.Int32,
                 'MOM': pl.Int32,
                 'MMTA': pl.Int32,
                 'CILINDRADA': pl.Int32,
                 'POTENCIA': pl.Float64,
                 'KW': pl.Float64,
                 'PROPULSION': pl.String,
                 'CATELECT': pl.String,
                 'CONSUMO': pl.Int32,
                 'AUTONOMIA': pl.Float64,
                 'ALIMENTACION': pl.String,
                 'EMISIONES_EURO': pl.String,
                 'EMISIONES_CO2': pl.Float32,
                 'CARROCERIA': pl.String,
                 'DISTANCIA_EJES': pl.Int16,
                 'EJE_ANTERIOR': pl.Int16,
                 'EJE_POSTERIOR': pl.Int16,
                 'PLAZAS': pl.Int16,
                 'PLAZAS_MAX': pl.Int16,
                 'PLAZAS_PIE': pl.Int16}

propulsion_mapping = {
    "0": "GAS",
    "1": "DIES",
    "2": "ELEC",
    "3": "OTR",
    "4": "BUT",
    "5": "SOL",
    "6": "GLP",
    "7": "GNC",
    "8": "GNL",
    "9": "H",
    "A": "BIOMET",
    "B": "ETH",
    "C": "BIODIES",
    "D": "DIES",
    "G": "GAS"
    }

mapping = {
    "SUBTIPO_DGT" : {
    "00": "CAMIÓN",
    "01": "CAMIÓN PLATAFORMA",
    "02": "CAMIÓN CAJA",
    "03": "CAMIÓN FURGÓN",
    "04": "CAMIÓN BOTELLERO",
    "05": "CAMIÓN CISTERNA",
    "06": "CAMIÓN JAULA",
    "07": "CAMIÓN FRIGORÍFICO",
    "08": "CAMIÓN TALLER",
    "09": "CAMIÓN PARA CANTERA",
    "0A": "CAMIÓN PORTAVEHÍCULOS",
    "0B": "CAMIÓN MIXTO",
    "0C": "CAMIÓN PORTACONTENEDORES",
    "0D": "CAMIÓN BASURERO",
    "0E": "CAMIÓN ISOTERMO",
    "0F": "CAMIÓN SILO",
    "0G": "VEHÍCULO MIXTO ADAPTABLE",
    "10": "CAMIÓN ARTICULADO",
    "11": "CAMIÓN ARTICULADO PLATAFORMA",
    "12": "CAMIÓN ARTICULADO CAJA",
    "13": "CAMIÓN ARTICULADO FURGÓN",
    "14": "CAMIÓN ARTICULADO BOTELLERO",
    "15": "CAMIÓN ARTICULADO CISTERNA",
    "16": "CAMIÓN ARTICULADO JAULA",
    "17": "CAMIÓN ARTICULADO FRIGORÍFICO",
    "18": "CAMIÓN ARTICULADO TALLER",
    "19": "CAMIÓN ARTICULADO PARA CANTERA",
    "1A": "CAMIÓN ARTICULADO VIVIENDA O CARAVANA",
    "1C": "CAMIÓN ARTICULADO HORMIGONERA",
    "1D": "CAMIÓN ARTICULADO VOLQUETE",
    "1E": "CAMIÓN ARTICULADO GRÚA",
    "1F": "CAMIÓN ARTICULADO CONTRA INCENDIOS",
    "20": "FURGONETA",
    "21": "FURGONETA MIXTA",
    "22": "AMBULANCIA",
    "23": "COCHE FÚNEBRE",
    "24": "CAMIONETA",
    "25": "TODO TERRENO",
    "30": "AUTOBÚS",
    "31": "AUTOBÚS ARTICULADO",
    "32": "AUTOBÚS MIXTO",
    "33": "BIBLIOBÚS",
    "34": "AUTOBÚS LABORATORIO",
    "35": "AUTOBÚS TALLER",
    "36": "AUTOBÚS SANITARIO",
    "40": "TURISMO",
    "50": "MOTOCICLETA DE 2 RUEDAS SIN SIDECAR",
    "51": "MOTOCICLETA CON SIDECAR",
    "52": "MOTOCARRO",
    "53": "AUTOMÓVIL DE 3 RUEDAS",
    "54": "CUATRICICLO PESADO",
    "60": "VEHÍCULO DE MOVILIDAD PERSONAL",
    "70": "VEHÍCULO ESPECIAL",
    "71": "PALA CARGADORA",
    "72": "PALA EXCAVADORA",
    "73": "CARRETILLA ELEVADORA",
    "74": "MONIVELADORA",
    "75": "COMPACTADORA",
    "76": "APISONADORA",
    "77": "GIROGRAVILLADORA",
    "78": "MACHACADORA",
    "79": "QUITANIEVES",
    "7A": "VIVIENDA",
    "7B": "BARREDORA",
    "7C": "HORMIGONERA",
    "7D": "VOLQUETE DE CANTERAS",
    "7E": "GRÚA",
    "7F": "SERVICIO CONTRA INCENDIOS",
    "7G": "ASPIRADORA DE FANGOS",
    "7H": "MOTOCULTOR",
    "7I": "MAQUINARIA AGRÍCOLA AUTOMOTRIZ",
    "7J": "PALA CARGADORA-RETROEXCAVADORA",
    "7K": "TREN HASTA 160 PLAZAS",
    "80": "TRACTOR",
    "81": "TRACTOCAMIÓN",
    "82": "TRACTOCARRO",
    "90": "CICLOMOTOR DE 2 RUEDAS",
    "91": "CICLOMOTOR DE 3 RUEDAS",
    "92": "CUATRICICLO LIGERO",
    "EX": "EXTRANJERO",
    "R0": "REMOLQUE",
    "R1": "REMOLQUE PLATAFORMA",
    "R2": "REMOLQUE CAJA",
    "R3": "REMOLQUE FURGÓN",
    "R4": "REMOLQUE BOTELLERO",
    "R5": "REMOLQUE CISTERNA",
    "R6": "REMOLQUE JAULA",
    "R7": "REMOLQUE FRIGORÍFICO",
    "R8": "REMOLQUE TALLER",
    "R9": "REMOLQUE PARA CANTERAS",
    "RA": "REMOLQUE VIVIENDA O CARAVANA",
    "RB": "REMOLQUE DE VIAJEROS O DE AUTOBÚS",
    "RC": "REMOLQUE HORMIGONERA",
    "RD": "REMOLQUE VOLQUETE DE CANTERA",
    "RE": "REMOLQUE DE GRÚA",
    "RF": "REMOLQUE CONTRA INCENDIOS",
    "RH": "MAQ.AGRÍCOLA ARRASTRADA DE 2 EJES",
    "S0": "SEMIRREMOLQUE",
    "S1": "SEMIRREMOLQUE PLATAFORMA",
    "S2": "SEMIRREMOLQUE CAJA",
    "S3": "SEMIRREMOLQUE FURGÓN",
    "s3": "SEMIRREMOLQUE FURGÓN",
    "S4": "SEMIRREMOLQUE BOTELLERO",
    "S5": "SEMIRREMOLQUE CISTERNA",
    "S6": "SEMIRREMOLQUE JAULA",
    "S7": "SEMIRREMOLQUE FRIGORÍFICO",
    "S8": "SEMIRREMOLQUE TALLER",
    "S9": "SEMIRREMOLQUE CANTERA",
    "SA": "SEMIRREMOLQUE VIVIENDA O CARAVANA",
    "SB": "SEMIRREMOLQUE VIAJEROS O AUTOBÚS",
    "SC": "SEMIRREMOLQUE HORMIGONERA",
    "SD": "SEMIRREMOLQUE VOLQUETE DE CANTERA",
    "SE": "SEMIRREMOLQUE GRÚA",
    "SF": "SEMIRREMOLQUE CONTRA INCENDIOS",
    "SH": "MAQ.AGRÍCOLA ARRASTRADA DE 1 EJE",
    "": "SIN ESPECIFICAR"
    },
    
    "CLASE_MATR" : {
    "0": "Ordinaria",
    "2": "Remolque",
    "3": "Diplomática",
    "5": "Vehículo especial",
    "6": "Ciclomotor",
    "8": "Histórica"
    },

    "PROPULSION" : {
    "GAS": "Gasolina",
    "DIES": "Diésel",
    "ELEC": "Eléctrico",
    "OTR": "Otros",
    "BUT": "Buatno",
    "SOL": "Solar",
    "GLP": "Gas licuado de Petróleo",
    "GNC": "Gas Natural Comprimido",
    "GNL": "Gas Natural Licuado",
    "H": "Hidrógeno",
    "BIOMET": "Biometano",
    "ETH": "Etanol",
    "BIODIES": "Biodiesel",
    "DIES": "Diésel",
    "GAS": "Gasolina"
    },

    "PROVINCIA_MATR" : {
    "01": "Araba/Álava",
    "02": "Albacete",
    "03": "Alicante/Alacant",
    "04": "Almería",
    "05": "Ávila",
    "06": "Badajoz",
    "07": "Balears (Illes)",
    "08": "Barcelona",
    "09": "Burgos",
    "1": "Araba/Álava",
    "2": "Albacete",
    "3": "Alicante/Alacant",
    "4": "Almería",
    "5": "Ávila",
    "6": "Badajoz",
    "7": "Balears (Illes)",
    "8": "Barcelona",
    "9": "Burgos",
    "10": "Cáceres",
    "11": "Cádiz",
    "12": "Castellón/Castelló",
    "13": "Ciudad Real",
    "14": "Córdoba",
    "15": "Coruña (A)",
    "16": "Cuenca",
    "17": "Girona",
    "18": "Granada",
    "19": "Guadalajara",
    "20": "Gipuzkoa",
    "21": "Huelva",
    "22": "Huesca",
    "23": "Jaén",
    "24": "León",
    "25": "Lleida",
    "26": "Rioja (La)",
    "27": "Lugo",
    "28": "Madrid",
    "29": "Málaga",
    "30": "Murcia",
    "31": "Navarra",
    "32": "Ourense",
    "33": "Asturias",
    "34": "Palencia",
    "35": "Palmas (Las)",
    "36": "Pontevedra",
    "37": "Salamanca",
    "38": "Santa Cruz de Tenerife",
    "39": "Cantabria",
    "40": "Segovia",
    "41": "Sevilla",
    "42": "Soria",
    "43": "Tarragona",
    "44": "Teruel",
    "45": "Toledo",
    "46": "Valencia/València",
    "47": "Valladolid",
    "48": "Bizkaia",
    "49": "Zamora",
    "50": "Zaragoza",
    "51": "Ceuta",
    "52": "Melilla",
    "99": "Extranjero",
    "100": "Desconocido"
    },

    "ALIMENTACION" : {
    "0": "Sin informar", 
    "B": "Bifuel",
    "F": "Flexifuel", 
    "M": "Monofuel",
    },

    "CATELECT" : {
    "REEV": "Eléctrico de Autonomía Extendida",
    "HEV":  "Eléctrico Híbrido",
    "BEV":  "Eléctrico de Batería",
    "PHEV": "Eléctrico Híbrido Enchufable",
    "FCEV": "Pila de combustible-hidrógeno"
    }
}

tramit_file_columns = ["FEC_MATRICULA","COD_CLASE_MAT","FEC_TRAMITACION","MARCA_ITV","MODELO_ITV","COD_PROCEDENCIA_ITV","BASTIDOR_ITV","COD_TIPO",
    "COD_PROPULSION_ITV","CILINDRADA_ITV","POTENCIA_ITV","TARA","PESO_MAX","NUM_PLAZAS","IND_PRECINTO","IND_EMBARGO","NUM_TRANSMISIONES",
    "NUM_TITULARES","LOCALIDAD_VEHICULO","COD_PROVINCIA_VEH","COD_PROVINCIA_MAT","CLAVE_TRAMITE","FEC_TRAMITE","CODIGO_POSTAL","FEC_PRIM_MATRICULACION",
    "IND_NUEVO_USADO","PERSONA_FISICA_JURIDICA","CODIGO_ITV","SERVICIO","COD_MUNICIPIO_INE_VEH","MUNICIPIO","KW_ITV","NUM_PLAZAS_MAX","CO2_ITV","RENTING", 
    "COD_TUTELA", "COD_POSESION","IND_BAJA_DEF","IND_BAJA_TEMP", "IND_SUSTRACCION","BAJA_TELEMATICA","TIPO_ITV","VARIANTE_ITV","VERSION_ITV",
    "FABRICANTE_ITV", "MASA_ORDEN_MARCHA_ITV","MASA_MAXIMA_TECNICA_ADMISIBLE_ITV","CATEGORÍA_HOMOLOGACION_EUROPEA_ITV","CARROCERIA","PLAZAS_PIE",
    "NIVEL_EMISIONES_EURO_ITV","CONSUMO_WH/KM_ITV","CLASIFICACIÓN_REGLAMENTO_VEHICULOS_ITV", "CATEGORÍA_VEHICULO_ELECTRICO","AUTONOMÍA_VEHÍCULO_ELÉCTRICO",
    "MARCA_VEHÍCULO_BASE","FABRICANTE_VEHÍCULO_BASE","TIPO_VEHÍCULO_BASE","VARIANTE_VEHÍCULO_BASE","VERSIÓN_VEHÍCULO_BASE","DISTANCIA_EJES_12_ITV",
    "VIA_ANTERIOR_ITV","VIA_POSTERIOR_ITV","TIPO_ALIMENTACION_ITV","CONTRASEÑA_HOMOLOGACION_ITV","ECO_INNOVACION_ITV","REDUCCION_ECO_ITV","CODIGO_ECO_ITV","FEC_PROCESO"
    ]

tramit_file_index = [8,1,8,30,22,1,21,2,1,5,6,6,6,3,2,2,2,2,24,2,2,1,8,5,8,1,1,9,3,5,30,7,3,5,1,1,1,1,1,1,11,25,25,35,70,6,6,4,4,3,8,4,4,4,6,30,50,35,25,35,4,4,4,1,25,1,4,25,8]

common_cols_from_exact_fleet = ["FECHA_MATR", "FECHA_PRIM_MATR", "CLASE_MATR", "MARCA", "MODELO", 
    "PROCEDENCIA","TIPO", "TIPO_DGT", "PROPULSION", "CILINDRADA", "POTENCIA", "KW", "TARA",
    "PESO_MAX", "MOM", "MMTA", "PLAZAS", "PLAZAS_MAX", "PLAZAS_PIE", "NUM_TITULARES",
    "PROVINCIA", "MUNICIPIO", "PROVINCIA_MATR", "NUEVO_USADO", "TIPO_TITULAR", "SERVICIO", 
    "EMISIONES_CO2", "RENTING", "VARIANTE", "VERSION", "FABRICANTE","CARROCERIA", "DISTANCIA_EJES", 
    "EJE_ANTERIOR", "EJE_POSTERIOR", "EMISIONES_EURO","CONSUMO", "AUTONOMIA", "ALIMENTACION", "CATELECT"]

common_cols_from_tramit_files = ["FEC_MATRICULA", "FEC_PRIM_MATRICULACION", "COD_CLASE_MAT", "MARCA_ITV", "MODELO_ITV",
    "COD_PROCEDENCIA_ITV", "COD_TIPO", "TIPO_ITV", "COD_PROPULSION_ITV", "CILINDRADA_ITV",
    "POTENCIA_ITV", "KW_ITV", "TARA", "PESO_MAX", "MASA_ORDEN_MARCHA_ITV",
    "MASA_MAXIMA_TECNICA_ADMISIBLE_ITV", "NUM_PLAZAS", "NUM_PLAZAS_MAX", "PLAZAS_PIE",
    "NUM_TITULARES", "COD_PROVINCIA_VEH", "COD_MUNICIPIO_INE_VEH", "COD_PROVINCIA_MAT",
    "IND_NUEVO_USADO", "PERSONA_FISICA_JURIDICA", "SERVICIO", "CO2_ITV", "RENTING",
    "VARIANTE_ITV", "VERSION_ITV","FABRICANTE_ITV", "CARROCERIA", "DISTANCIA_EJES_12_ITV",
    "VIA_ANTERIOR_ITV", "VIA_POSTERIOR_ITV", "NIVEL_EMISIONES_EURO_ITV",
    "CONSUMO_WH/KM_ITV", "AUTONOMÍA_VEHÍCULO_ELÉCTRICO", "TIPO_ALIMENTACION_ITV",
    "CATEGORÍA_VEHICULO_ELECTRICO"
    ]

common_cols_mapping = {
    "FEC_MATRICULA": "FECHA_MATR",
    "FEC_PRIM_MATRICULACION": "FECHA_PRIM_MATR",
    "COD_CLASE_MAT": "CLASE_MATR",
    "MARCA_ITV": "MARCA",
    "MODELO_ITV": "MODELO",
    "COD_PROCEDENCIA_ITV": "PROCEDENCIA",
    "COD_TIPO": "TIPO",
    "TIPO_ITV": "TIPO_DGT",
    "COD_PROPULSION_ITV": "PROPULSION",
    "CILINDRADA_ITV": "CILINDRADA",
    "POTENCIA_ITV": "POTENCIA",
    "KW_ITV": "KW",
    "TARA": "TARA",
    "PESO_MAX": "PESO_MAX",
    "MASA_ORDEN_MARCHA_ITV": "MOM",
    "MASA_MAXIMA_TECNICA_ADMISIBLE_ITV": "MMTA",
    "NUM_PLAZAS": "PLAZAS",
    "NUM_PLAZAS_MAX": "PLAZAS_MAX",
    "PLAZAS_PIE": "PLAZAS_PIE",
    "NUM_TITULARES": "NUM_TITULARES",
    "COD_PROVINCIA_VEH": "PROVINCIA",
    "COD_MUNICIPIO_INE_VEH": "MUNICIPIO",
    "COD_PROVINCIA_MAT": "PROVINCIA_MATR",
    "IND_NUEVO_USADO": "NUEVO_USADO",
    "PERSONA_FISICA_JURIDICA": "TIPO_TITULAR",
    "SERVICIO": "SERVICIO",
    "CO2_ITV": "EMISIONES_CO2",
    "RENTING": "RENTING",
    "VARIANTE_ITV": "VARIANTE",
    "VERSION_ITV": "VERSION",
    "FABRICANTE_ITV": "FABRICANTE",
    "CARROCERIA": "CARROCERIA",
    "DISTANCIA_EJES_12_ITV": "DISTANCIA_EJES",
    "VIA_ANTERIOR_ITV": "EJE_ANTERIOR",
    "VIA_POSTERIOR_ITV": "EJE_POSTERIOR",
    "NIVEL_EMISIONES_EURO_ITV": "EMISIONES_EURO",
    "CONSUMO_WH/KM_ITV": "CONSUMO",
    "AUTONOMÍA_VEHÍCULO_ELÉCTRICO": "AUTONOMIA",
    "TIPO_ALIMENTACION_ITV": "ALIMENTACION",
    "CATEGORÍA_VEHICULO_ELECTRICO": "CATELECT"
}

cities = {
    "BCN":  [8,   19], 
    "VLC":  [46, 250], 
    "BILB": [48,  20], 
    "MAD":  [28,  79], 
    "SEV":  [41,  91],
    "ZAR":  [50, 297],
    "MAL":  [29,  67],
    "MURC": [30,  30],
    "MALL": [7,   40],
    "PGC":  [35,  16],
    "ALIC": [3,   14],
    "COR":  [14,  21],
    "VALL": [47, 186],
    "VIG":  [36,  57],
    "GIJ":  [33,  24],
    "VIT":  [1,   59],
    "ELCH": [3,   65], 
    "GRAN": [18,  87], 
    "TERR": [8,  279], 
    "SAB":  [8,  187],  
    "OVI":  [33,  44],  
    "PAMP": [31, 201], 
    "ALM":  [4,   13],
}

cities_area = {
    "BCN": 101.9,
    "BILB": 41.3,
    "MAD": 604.3,
    "VLC": 134.6,
    "SEV": 141.0,
    "ZAR": 973.78,
    "MAL": 398.0, 
    "MURC": 881.86,  
    "MALL": 208.63,  
    "PGC": 100.55,  
    "ALIC": 201.27, 
    "COR": 1254.25,  
    "VALL": 197.47,  
    "VIG": 109.06, 
    "HOSP": 12.4,  
    "GIJ": 181.6,   
    "VIT": 276.98,  
    "ELCH": 326.1,  
    "GRAN": 88.02, 
    "TERR": 70.2,   
    "BAD": 21.2,      
    "SAB": 37.79,    
    "OVI": 186.7,   
    "CART": 558.3,   
    "MOST": 45.36,   
    "JER": 1188.14,   
    "SCRUZ": 150.56,   
    "PAMP": 25.14,   
    "ALM": 295.51,  
    "ALC": 38.5
}

