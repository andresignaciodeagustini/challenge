import click
from extract import MuseoExtractor, UrlExtractor
import pandas as pd
from constants import BASE_FILE_DIR
from config import (
    museo_ds,
    cines_ds,
    espacios,
)
from load import (
    Cine_carga,
    Categoria_total_carga,
    Fuente_total_carga,
    ProvinciaCategoria_total_carga
    
)
import logging


log = logging.getLogger()
extractors_dict = {
    "museo": MuseoExtractor(museo_ds["name"], museo_ds["url"]),
    "cines": UrlExtractor(cines_ds["name"], cines_ds["url"]),
    "espacios": UrlExtractor(espacios["name"], espacios["url"]),
}


def extract_raws(date_str: str) -> list[str]:
    """
    Esta función recibe como argumento una fecha:
    
    
        date_str (str): donde correra el archivo con el formato yyyy-mm-dd
    
    Devuelve una lista de file_path(donde estan almacenados estos datos)
        list[str]: list of stored data file paths
        
    """
    file_paths = dict()
    
    """Recorre el extractor con la fecha
     Devolverá file_path en un diccionario
    
        """
    for name, extractor in extractors_dict.items():
        file_path = extractor.extract(date_str)
        file_paths[name] = file_path
    return file_paths


def merge_raw(file_paths: list[str], out_file_path: str) -> str:
    """
    Va por cada uno de los file_paths descargados, los lee como un dataframe
     los transforma y los agrega en un nuevo dataframe transformado.
    y una vez que tiene los tres, los concatena es decir los transforma en uno solo 
    y los guarda en una misma direccion
    
    merge raw data and stores it on out_file_path
    Args:
        file_paths (list[str]): list of raw data location
        out_file_path (str): destination location
    Returns:
        str: destination location
    """
    dfs_transformed = list()
    for name, extractor in extractors_dict.items():
        df = pd.read_csv(file_paths[name])
        dft = extractor.transform(df)
        dfs_transformed.append(dft)
        pd.concat(dfs_transformed, axis=0).to_csv(out_file_path)
    return out_file_path


@click.command()
@click.option("--date", help="run date in format yyyy-mm-dd")


#ETL

def run_pipeline(date: str) -> None:
    """Run the pipeline
    Args:
        date (str): Job date format YYYY-MM-DD
    """
    # EXTRACCION
    
    

    log.info("Extracting")
    file_paths = extract_raws(date)

    # TRANSFORMACION
    """ Como se van a crear tres dataframes   """
    merge_path = BASE_FILE_DIR / "merge_df_{date}.csv"
    merge_raw(file_paths, merge_path)

    # CARGA
    
    
    """Se crean distintas clases y se le pasan los path   """
    log.info("Loading")
    Cine_carga().load_table(file_paths["cines"])
    Categoria_total_carga().load_table(merge_path)
    Fuente_total_carga().load_table(file_paths)
    ProvinciaCategoria_total_carga().load_table(merge_path)
    log.info("Done")


if __name__ == "__main__":
    run_pipeline()