# Raw Data

Place raw CSV data files here for the Bingen green roof pipeline. 

## Expected Structure

- **greenroof/**: Raw CSV files from Empower and Kissel green roof sensors
- **parkplatz/**: Raw CSV files from Empower and Kissel parking lot sensors
- **black_globes/**: Raw `.dat` files from the black-globe temperature logger (Campbell Scientific TOA5 format)

## Data Sources

- Empower sensors: Subdirectory `Empower_Greenroof/` and `Empower_Parkplatz_data/`
- Kissel sensors: Subdirectory `Kissel_GreenRoof_Data/` and `Kissel_Parkplatz_data/`
- Black-globe logger: files land directly in `black_globes/` (no subfolder), named `<station>_<table>_<export-timestamp>.dat`

The pipeline expects greenroof/parkplatz files with timestamps in ISO format and columns for radiation measurements (SR1, SR2, IR1, IR2, etc.). Black-globe files are TOA5 exports with 4 fixed header rows.

Ingest is incremental: re-running the pipeline only processes files that are new or have changed (by size/modified-time) since the last run — existing files are never re-parsed unnecessarily, and nothing needs to be removed before adding more.
