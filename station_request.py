from datetime import datetime, timedelta
import pandas as pd
import requests
import holidays
import os

API_KEY = "f1a82c03-ddcf-487d-a77c-171b4e2b044c"
USER_NAME = "regenbogen24_mein.gmx"
STATION_ID = "686b1552-ded0-4295-ae9c-30a03b3bfef0"

# ============================
# 1. Bestehende Datei laden
# ============================
if os.path.exists("hem_prices.csv"):
    df_existing = pd.read_csv("hem_prices.csv", parse_dates=["date"])
else:
    df_existing = pd.DataFrame(columns=["date", "diesel", "e10", "e5", "weekday", "is_holiday", "is_vacation", "holiday"])

# ============================
# 2. Letzte 7 Tage bestimmen
# ============================
today = datetime.now().date()
days_to_check = [(today - timedelta(days=i)) for i in range(1, 8)]  # gestern bis -7 Tage

# Welche Tage fehlen?
existing_days = set(df_existing["date"].dt.date.unique())
missing_days = [d for d in days_to_check if d not in existing_days]

print("Fehlende Tage:", missing_days)

# ============================
# 3. NRW-Feiertage vorbereiten
# ============================
years = list({d.year for d in days_to_check})
nrw_holidays = holidays.Germany(years=years, subdiv="NW")

# ============================
# 4. Ferien laden (robust)
# ============================
ferien_url = "https://ferien-api.de/api/v1/holidays/NW"
try:
    ferien_response = requests.get(ferien_url)
    ferien = ferien_response.json()
except:
    print("Warnung: Ferien-API liefert keine gültige Antwort.")
    ferien = []

ferien_ranges = [
    (pd.to_datetime(f["start"]).date(), pd.to_datetime(f["end"]).date())
    for f in ferien if "start" in f and "end" in f
]

def is_ferien(date):
    d = date.date()
    return any(start <= d <= end for start, end in ferien_ranges)

# ============================
# 5. Fehlende Tage nachladen
# ============================
rows_to_append = []

for day in missing_days:
    YEAR = day.strftime("%Y")
    MONTH = day.strftime("%m")
    DATE_PREFIX = day.strftime("%Y-%m-%d")

    url = f"https://{USER_NAME}:{API_KEY}@data.tankerkoenig.de/tankerkoenig-organization/tankerkoenig-data/raw/branch/master/prices/{YEAR}/{MONTH}/{DATE_PREFIX}-prices.csv"
    print("Hole:", DATE_PREFIX)

    response = requests.get(url)
    if response.status_code != 200:
        print("Fehler beim Laden:", response.status_code)
        continue

    with open("temp.csv", "wb") as f:
        f.write(response.content)

    df = pd.read_csv("temp.csv")
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Europe/Berlin").dt.tz_localize(None)

    df_filtered = df[df["station_uuid"] == STATION_ID]

    if df_filtered.empty:
        print("Keine Daten für", day)
        continue

    df_result = df_filtered[["date", "diesel", "e10", "e5"]].copy()
    df_result["weekday"] = df_result["date"].dt.weekday
    df_result["is_holiday"] = df_result["date"].dt.date.apply(lambda d: 1 if d in nrw_holidays else 0)
    df_result["is_vacation"] = df_result["date"].apply(is_ferien)
    df_result["holiday"] = df_result[["is_holiday", "is_vacation"]].max(axis=1)

    rows_to_append.append(df_result)

# ============================
# 6. Neue Daten anhängen
# ============================
if rows_to_append:
    df_new = pd.concat(rows_to_append)
    df_all = pd.concat([df_existing, df_new])
else:
    df_all = df_existing

# ============================
# 7. Nur letzte 7 Tage behalten
# ============================
df_all = df_all[df_all["date"].dt.date >= (today - timedelta(days=7))]

# ============================
# 8. Speichern
# ============================
df_all.to_csv("hem_prices.csv", index=False)
print("Aktualisiert und gespeichert.")
