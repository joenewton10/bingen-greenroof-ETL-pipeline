"""
Generates fabricated sensor data in the exact raw file formats the pipeline's
ingest modules expect (Empower greenroof, Kissel/MXmini greenroof, Parkplatz,
Campbell Scientific TOA5 black-globes), so this showcase copy can be run
end-to-end without any real lab data.

All values are synthetic (diurnal/seasonal sine curves + noise) - no real
measurements from the lab are used or referenced anywhere in this script.

Run once from the project root:
    .venv\\Scripts\\python.exe scripts\\generate_synthetic_data.py
"""
import os
import csv
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

RNG = np.random.default_rng(42)

YEARS = [2021, 2022, 2023, 2024, 2025]
SEASON_START = {
    "winter": (1, 10),
    "spring": (4, 10),
    "summer": (7, 10),
    "autumn": (10, 10),
}
SEASON_BASE_TEMP = {"winter": 2.0, "spring": 11.0, "summer": 22.0, "autumn": 12.0}
SEASON_RAD_PEAK = {"winter": 280.0, "spring": 550.0, "summer": 820.0, "autumn": 400.0}
SEASON_COOLING_STRENGTH = {"winter": 0.3, "spring": 0.9, "summer": 1.4, "autumn": 0.6}
FREQ = "5min"
DAYS_PER_CHUNK = 7


def hour_frac(idx):
    return idx.hour + idx.minute / 60.0


def build_base_dataframe():
    """One shared physical dataset (5-min resolution, 20 week-long chunks
    across 5 years x 4 seasons) that every raw-file writer below samples
    from, so greenroof/parkplatz/black-globe exports stay mutually
    consistent (same timestamps, correlated signals)."""
    frames = []
    for year in YEARS:
        for season, (month, day) in SEASON_START.items():
            start = pd.Timestamp(year=year, month=month, day=day)
            idx = pd.date_range(start, start + pd.Timedelta(days=DAYS_PER_CHUNK), freq=FREQ, inclusive="left")
            n = len(idx)
            h = hour_frac(idx)

            base_temp = SEASON_BASE_TEMP[season]
            amp = 6.0 if season in ("summer", "spring") else 4.0
            pp_temp_1 = base_temp + amp * np.cos(2 * np.pi * (h - 15) / 24) + RNG.normal(0, 0.3, n)
            pp_temp_2 = pp_temp_1 - 0.3 + RNG.normal(0, 0.2, n)

            daytime = (h >= 7) & (h <= 19)
            strength = SEASON_COOLING_STRENGTH[season]
            cooling = np.where(daytime, -strength * np.sin(np.pi * np.clip(h - 7, 0, 12) / 12), 0.15)
            cooling += RNG.normal(0, 0.15, n)

            gr_temp_1 = pp_temp_1 + cooling
            gr_temp_2 = gr_temp_1 + 0.2 + RNG.normal(0, 0.15, n)

            rad_peak = SEASON_RAD_PEAK[season]
            solar = rad_peak * np.clip(np.sin(np.pi * np.clip(h - 6, 0, 14) / 14), 0, None)
            sr1_sky = np.clip(solar + RNG.normal(0, 15, n), 0, None)
            gr_sr1 = np.clip(sr1_sky + RNG.normal(0, 5, n), 0, None)

            frame = pd.DataFrame({
                "timestamp": idx,
                "pp_temp_1": pp_temp_1, "pp_temp_2": pp_temp_2,
                "pp_rh_1": np.clip(90 - 1.5 * (pp_temp_1 - 10) + RNG.normal(0, 4, n), 25, 100),
                "pp_rh_2": np.clip(88 - 1.5 * (pp_temp_2 - 10) + RNG.normal(0, 4, n), 25, 100),
                "pp_soil_moisture": np.clip(10 + RNG.normal(0, 2, n), 2, 25),
                "pp_soil_temp_1": pp_temp_1 + RNG.normal(0, 1, n),
                "pp_soil_temp_2": pp_temp_1 + RNG.normal(0, 1, n),
                "pp_wind_speed": np.clip(RNG.normal(2.5, 1.2, n), 0, 12),
                "pp_wind_dir": RNG.uniform(0, 360, n),
                # Kept comfortably under 1000 mbar median: the real ingest's
                # normalize_scales() treats any column with median > 1000 as a
                # known logger 1000x-scale quirk and divides it down, which
                # would wreck a realistic ~1008 mbar synthetic value here.
                "pp_pressure": 990 + RNG.normal(0, 3, n),
                "pp_sr1": sr1_sky,
                "pp_sr2": np.clip(sr1_sky * 0.12 + RNG.normal(0, 4, n), -10, None),
                "pp_ir1": -50 + RNG.normal(0, 8, n),
                "pp_ir2": -80 + RNG.normal(0, 8, n),

                "gr_temp_1": gr_temp_1, "gr_temp_2": gr_temp_2,
                "gr_rh_1": np.clip(94 - 1.4 * (gr_temp_1 - 10) + RNG.normal(0, 4, n), 30, 100),
                "gr_rh_2": np.clip(90 - 1.4 * (gr_temp_2 - 10) + RNG.normal(0, 4, n), 30, 100),
                "gr_soil_moisture": np.clip(28 + RNG.normal(0, 3, n), 10, 45),
                "gr_soil_temp_1": gr_temp_1 + RNG.normal(0, 1, n),
                "gr_soil_temp_2": gr_temp_1 + RNG.normal(0, 1, n),
                "gr_wind_speed": np.clip(RNG.normal(1.8, 1.0, n), 0, 10),
                "gr_wind_dir": RNG.uniform(0, 360, n),
                "gr_sr1": gr_sr1,
                "gr_sr2": np.clip(gr_sr1 * 0.2 + RNG.normal(0, 4, n), -10, None),
                "gr_ir1": -55 + RNG.normal(0, 8, n),
                "gr_ir2": -85 + RNG.normal(0, 8, n),

                "globe_gruendach": gr_temp_1 + 3 + RNG.normal(0, 1.5, n),
                "globe_parkplatz": pp_temp_1 + 5 + RNG.normal(0, 1.5, n),
                "globe_mobiga_nord": pp_temp_1 + 2 + RNG.normal(0, 1.5, n),
                "globe_mobiga_sued": pp_temp_1 + 2.5 + RNG.normal(0, 1.5, n),
            })
            frames.append(frame)
    return pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)


def de(x, decimals=1):
    """German-locale decimal formatting (comma separator) used by the real exports."""
    return f"{x:.{decimals}f}".replace(".", ",")


def iso(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def write_empower_greenroof(df, path):
    lines = [
        "Station Name;BINGEN_DEMO_GR1\n",
        "Station ID;9001\n",
        "Seriennummer;DEMO-GR-EMP-01\n",
        "Timezone;UTC+1\n",
        "\n",
        "SensorNames;synthetic demo station\n",
        "Units;see PIPELINE_GUIDE.md\n",
    ]
    for row in df.itertuples():
        fields = ["0"] * 26
        fields[0] = iso(row.timestamp)
        fields[12] = de(row.gr_temp_2)          # PyrTemp
        fields[13] = de(row.gr_soil_temp_1)      # BodenTemp 1cm
        fields[14] = de(row.gr_soil_temp_2)      # BodenTemp 6cm
        fields[16] = de(row.gr_temp_2)           # LuftTemp oben (2.0m)
        fields[17] = de(row.gr_rh_2)             # RH oben (2.0m)
        fields[18] = de(row.gr_temp_1)           # LuftTemp unten (0.5m)
        fields[19] = de(row.gr_rh_1)             # RH unten (0.5m)
        fields[20] = de(row.gr_wind_speed, 2)
        fields[21] = de(row.gr_wind_dir, 0)
        fields[22] = de(row.gr_sr1)               # SR sky
        fields[23] = de(row.gr_sr2)               # SR ground
        fields[24] = de(row.gr_ir1)               # IR sky
        fields[25] = de(row.gr_ir2)               # IR ground
        lines.append(";".join(fields) + "\n")
    with open(path, "w", encoding="latin-1") as f:
        f.writelines(lines)


def write_kissel_greenroof(df, path):
    lines = [
        "Station Name;BINGEN_DEMO_GR2\n",
        "Station ID;9002\n",
        "Serial No;DEMO-GR-KIS-01\n",
        "Timezone;UTC+1\n",
        "\n",
        "Timestamp;MXmini.Spannungsversorgung Vin 4.1 (V);MXmini.Niederschlag 4.1 (mm);"
        "Arco SDI-12.Windstaerke AVG (m/s);Arco SDI-12.Windstaerke MAX (m/s);Arco SDI-12.Windstaerke MIN (m/s);"
        "Arco SDI-12.Windrichtung (deg);Arco SDI-12.Windrichtung (deg);Arco SDI-12.Windrichtung (deg);"
        "Lambrecht 1.4.Lufttemperatur (C);Lambrecht 1.4.Lufttemperatur MAX (C);Lambrecht 1.4.Lufttemperatur MIN (C);"
        "Lambrecht 1.4.Relative Feuchte (%);Lambrecht 1.4.Relative Feuchte MAX (%);Lambrecht 1.4.Relative Feuchte MIN (%);"
        "Lambrecht 1.10.Globalstrahlung (W/m2);Lambrecht 1.10.Globalstrahlung MAX (W/m2);Lambrecht 1.10.Globalstrahlung MIN (W/m2);"
        "SMT100.2.1.Bodentemperatur (C);SMT100.2.1.Bodenfeuchte (vol%);SMT100.2.1.Permittivitaet;"
        "Virtual.Bodenfeuchte_korregiert (vol%)\n",
        "Units;V;mm;m/s;m/s;m/s;deg;deg;deg;C;C;C;%;%;%;W/m2;W/m2;W/m2;C;vol%;-;vol%\n",
    ]
    for row in df.itertuples():
        fields = ["0"] * 22
        fields[0] = iso(row.timestamp)
        fields[3] = de(row.gr_wind_speed, 2)
        fields[6] = de(row.gr_wind_dir, 0)
        fields[9] = de(row.gr_temp_1)
        fields[12] = de(row.gr_rh_1)
        fields[15] = de(row.gr_sr1)
        fields[18] = de(row.gr_soil_temp_1)
        fields[19] = de(row.gr_soil_moisture)
        lines.append(";".join(fields) + "\n")
    with open(path, "w", encoding="latin-1") as f:
        f.writelines(lines)


PARKPLATZ_HEADER = (
    "Nr.;Datum / Uhrzeit;IR1 [W/m2];SR1 [W/m2];IR2 [W/m2];SR2 [W/m2];Temp [degC];"
    "Luftdruck [mbar];Bodenfeucht [vol%];Luftfeu-1 [%RH];Lufttemp-1 [degC];"
    "Luftfeu-2 [%RH];Lufttemp-2 [degC];Windgeschw [m/s];Windricht [grad];"
    "Bodentemp-1 [degC];Bodentemp-2 [degC];Seriennummer\n"
)


def write_parkplatz(df, path, serial):
    lines = [PARKPLATZ_HEADER]
    for i, row in enumerate(df.itertuples(), start=1):
        fields = [
            str(i),
            iso(row.timestamp),
            de(row.pp_ir1), de(row.pp_sr1), de(row.pp_ir2), de(row.pp_sr2),
            de(row.pp_temp_1),
            de(row.pp_pressure),
            de(row.pp_soil_moisture),
            de(row.pp_rh_1), de(row.pp_temp_1),
            de(row.pp_rh_2), de(row.pp_temp_2),
            de(row.pp_wind_speed, 2), de(row.pp_wind_dir, 0),
            de(row.pp_soil_temp_1), de(row.pp_soil_temp_2),
            serial,
        ]
        lines.append(";".join(fields) + "\n")
    with open(path, "w", encoding="latin-1") as f:
        f.writelines(lines)


def write_black_globes(df, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["TOA5", "BINGEN_BG_DEMO", "CR350", "1234", "CR350.Std.08.01", "CPU:BlackGlobes.CR350", "1", "BlackGlobes"])
        w.writerow(["TIMESTAMP", "RECORD", "MoBiGaNord_Avg", "MoBiGaSued_Avg", "Gruendach_Avg", "Parkplatz_Avg"])
        w.writerow(["TS", "RN", "Deg C", "Deg C", "Deg C", "Deg C"])
        w.writerow(["", "", "Avg", "Avg", "Avg", "Avg"])
        for i, row in enumerate(df.itertuples(), start=1):
            w.writerow([
                iso(row.timestamp), i,
                f"{row.globe_mobiga_nord:.2f}", f"{row.globe_mobiga_sued:.2f}",
                f"{row.globe_gruendach:.2f}", f"{row.globe_parkplatz:.2f}",
            ])


def main():
    df = build_base_dataframe()
    print(f"Generated {len(df):,} synthetic 5-minute timestamps across {len(YEARS)} years.")

    greenroof_dir = os.path.join(RAW_DIR, "greenroof")
    parkplatz_dir = os.path.join(RAW_DIR, "parkplatz")
    black_globes_dir = os.path.join(RAW_DIR, "black_globes")

    write_empower_greenroof(df, os.path.join(greenroof_dir, "Empower_Greenroof", "DEMO_081000387_synthetic.csv"))
    write_kissel_greenroof(df, os.path.join(greenroof_dir, "Kissel_GreenRoof_Data", "DEMO_081000388_synthetic.csv"))
    write_parkplatz(df, os.path.join(parkplatz_dir, "Empower_Parkplatz_data", "DEMO_parkplatz_empower_synthetic.csv"), "DEMO-PP-EMP-01")
    write_parkplatz(df, os.path.join(parkplatz_dir, "Kissel_Parkplatz_data", "DEMO_parkplatz_kissel_synthetic.csv"), "DEMO-PP-KIS-01")
    write_black_globes(df, os.path.join(black_globes_dir, "DEMO_4081_Minute_synthetic.dat"))

    print("Synthetic raw files written. Run the pipeline next:")
    print(r"  .venv\Scripts\python.exe scripts\run_pipeline.py --yes")


if __name__ == "__main__":
    main()
