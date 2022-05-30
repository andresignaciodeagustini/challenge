from decouple import AutoConfig
from constants import ROOT_DIR


config = AutoConfig(search_path=ROOT_DIR)

DB_CONNSTR = config("DB_CONNSTR")
MUSEO_URL = config("MUSEO_URL")
CINES_URL = config("CINES_URL")
ESPACIOS_URL = config("ESPACIOS_URL")


museo_ds = {
    "name": "museo",
    "url": MUSEO_URL,
}
cines_ds = {
    "name": "cines",
    "url": CINES_URL,
}
espacios = {
    "name": "bibliotecas_populares",
    "url": ESPACIOS_URL,
}