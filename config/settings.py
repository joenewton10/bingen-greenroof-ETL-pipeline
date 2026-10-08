import os
from dotenv import load_dotenv

# Load environment variables from config/.env (optional — DuckDB needs no credentials,
# this is only for overriding the DB file path or raw-data subfolders if ever needed).
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)

# Use local data folder (copy of original)
PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(PIPELINE_DIR, "data", "raw")
GREENROOF_DIR = os.path.join(RAW_DATA_DIR, os.getenv("GREENROOF_SUBDIR", "greenroof"))
PARKPLATZ_DIR = os.path.join(RAW_DATA_DIR, os.getenv("PARKPLATZ_SUBDIR", "parkplatz"))
BLACK_GLOBES_DIR = os.path.join(RAW_DATA_DIR, os.getenv("BLACK_GLOBES_SUBDIR", "black_globes"))
TIME_RESOLUTION = os.getenv("TIME_RESOLUTION", "minute")

# DuckDB database file. A single file holds the whole analytical dataset —
# no server to install or keep running. Override with DUCKDB_PATH in config/.env
# if a different location is ever needed.
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    os.path.join(PIPELINE_DIR, "outputs", "bingen_greenroof.duckdb"),
)
