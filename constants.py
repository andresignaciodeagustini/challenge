from pathlib import Path

BASE_FILE_DIR = Path("/tmp") #DONDE SE GUARDARAN LOS ARCHIVOS
ROOT_DIR = Path().resolve().parent #DONDE ESTA MI PROYECTO
SQL_DIR = ROOT_DIR / "challenge/sql" # DONDE ESTAN TODOS LOS ARVHICOS SQL

#NOMBRES DE LAS TABLAS CREADAS
RAW_TABLE_NAME = "raw"
CINE_INSIGHTS_TABLE_NAME = "cine_insights"
SOURCE_SIZE_TABLE_NAME = "size_by_datasource"
CATEGORY_COUNT_TABLE_NAME = "size_by_category"
PROV_CAT_COUNT_TABLE_NAME = "size_by_datasource"
#LISTA CON TODOS LOS NOMBRES DE LAS TABLAS
TABLE_NAMES = [
    RAW_TABLE_NAME,
    CINE_INSIGHTS_TABLE_NAME,
    SOURCE_SIZE_TABLE_NAME,
    CATEGORY_COUNT_TABLE_NAME,
    PROV_CAT_COUNT_TABLE_NAME,
]
