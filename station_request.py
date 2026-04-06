from datetime import datetime, timedelta
import pandas as pd
import requests
import holidays
import os

# ============================
# KONFIGURATION
# ============================
API_KEY = "f1a82c03-ddcf-487d-a77c-171b4e2b044c"
USER_NAME = "regenbogen24_mein.gmx"
STATION_ID = "686b1552-ded0-4295-ae9c-30a03b3bfef0"

CSV_FILE = "hem_prices.csv"
COLUMNS = ["date", "diesel", "e10", "e5", "weekday", "is_holiday"]

# ============================
# 1. Bestehende Datei laden
# ============================
if os.path.exists(CSV_FILE):
    df_existing = pd.read_csv(CSV_FILE)

    # Datentypen normalisieren
    df_existing["date"] = pd.to_datetime(df_existing["date"], errors="coerce")
    df_existing["diesel"] = df_existing["diesel"].astype(float)
    df_existing["e10"] = df_existing["e10"].astype(float)
    df_existing["e5"] = df_existing["e5"].astype(float)
    df_existing["weekday"] = df_existing["weekday"].astype(int)
    df_existing["is_holiday"] = df_existing["is_holiday"].astype(int)

    # Spaltenreihenfolge erzwingen
    df_existing = df_existing[COLUMNS]

else:
    df_existing = pd.DataFrame(columns=COLUMNS)

# ============================
# 2. Letzte 7 Tage bestimmen (ohne heute)
# ============================
today = datetime.now().date()
days_to_check = [(today - timedelta(days=i)) for i in range(1, 8)]

# print("Letzte 7 Tage:", days_to_check)

# ============================
# 3. Feiertage vorbereiten
# ============================
years = list({d.year for d in days_to_check})
nrw_holidays = holidays.Germany(years=years, subdiv="NW")

# ============================
# 5. Letzte 7 Tage laden
# ============================
df_new = pd.DataFrame(columns=COLUMNS)

for day in days_to_check:
    YEAR = day.strftime("%Y")
    MONTH = day.strftime("%m")
    DATE_PREFIX = day.strftime("%Y-%m-%d")

    url = (
        f"https://{USER_NAME}:{API_KEY}@data.tankerkoenig.de/"
        f"tankerkoenig-organization/tankerkoenig-data/raw/branch/master/"
        f"prices/{YEAR}/{MONTH}/{DATE_PREFIX}-prices.csv"
    )

    print("Hole:", DATE_PREFIX)

    response = requests.get(url)

    if response.status_code != 200:
        print("Fehler beim Laden:", response.status_code)
        continue

    with open("temp.csv", "wb") as f:
        f.write(response.content)

    df = pd.read_csv("temp.csv")

    # Datum sauber konvertieren
    df["date"] = (
        pd.to_datetime(df["date"], utc=True)
        .dt.tz_convert("Europe/Berlin")
        .dt.tz_localize(None)
    )

    df_filtered = df[df["station_uuid"] == STATION_ID]
    if df_filtered.empty:
        print("Keine Daten für", day)
        continue

    df_result = df_filtered[["date", "diesel", "e10", "e5"]].copy()
    df_result["weekday"] = df_result["date"].dt.weekday
    df_result["is_holiday"] = df_result["date"].dt.date.apply(lambda d: 1 if d in nrw_holidays else 0)

    # Spaltenreihenfolge erzwingen
    df_result = df_result[COLUMNS]

    if df_new.empty:
        df_new = df_result.copy()
    else:
        df_new = pd.concat([df_new,df_result], ignore_index=True)

# ============================
# 6. Zusammenführen
# ============================
if not df_existing.empty and not df_new.empty:
    df_all = pd.concat([df_new,df_existing], ignore_index=True)
else:
    if not df_new.empty:
        df_all = df_new.copy()
    else:
        df_all = df_existing.copy()

# ============================
# 7. Clean Up
# ============================
# Grenze: heute minus 8 Tage
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = df_all[df_all["date"].dt.date >= (today - timedelta(days=8))]

# Duplikate entfernen
df_all = df_all.drop_duplicates()

# ============================
# 8. Speichern
# ============================
df_all = df_all.sort_values("date")

df_all.to_csv(CSV_FILE, index=False)
print("Aktualisiert und gespeichert.")
