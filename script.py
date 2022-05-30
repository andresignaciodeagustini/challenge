from sqlalchemy import create_engine
from sqlalchemy.sql import text
from constants import SQL_DIR, TABLE_NAMES
from config import DB_CONNSTR
import logging


engine = create_engine(DB_CONNSTR)
log = logging.getLogger()




def create_tables():
    """Creando tabla db"""
    with engine.connect() as con:
        for file in TABLE_NAMES:#PARA CADA UNA DE LAS TABLES_NAMES DEFINIDAS EN EL CONSTANTS
            log.info(f"Creating table {file}")#BUSCA EL ARCHIVO "FILE"
            with open(SQL_DIR / f"{file}.sql") as f: #EL ARCHIVO SQL LO TOMA COMO CADENA DE TEXTO
                query = text(f.read()) #SE GUARDA 

            con.execute(f"DROP TABLE IF EXISTS {file}")
            con.execute(query)


if __name__ == "__main__":
    create_tables()