from datetime import datetime, timedelta
import pandas as pd
import requests
import holidays

# ============================
# KONFIGURATION
# ============================
API_KEY = "f1a82c03-ddcf-487d-a77c-171b4e2b044c"
USER_NAME = "regenbogen24_mein.gmx"
STATION_ID = "686b1552-ded0-4295-ae9c-30a03b3bfef0"

DATE_PREFIX = "2026-01-27"
# gestern
yesterday = datetime.now() - timedelta(days=1)
YEAR = yesterday.strftime("%Y")
MONTH = yesterday.strftime("%m")
DATE_PREFIX = yesterday.strftime("%Y-%m-%d")


URL = f"https://{USER_NAME}:{API_KEY}@data.tankerkoenig.de/tankerkoenig-organization/tankerkoenig-data/raw/branch/master/prices/{YEAR}/{MONTH}/{DATE_PREFIX}-prices.csv"

print(yesterday)
response = requests.get(URL)

if response.status_code == 200:
    with open("temp.csv", "wb") as f:
        f.write(response.content)
    print("CSV erfolgreich heruntergeladen.")
else:
    print("Fehler:", response.status_code, response.text)


df = pd.read_csv("temp.csv", parse_dates=["date"])

# ============================
# Filtern nach Station + Tag
# ============================
df_filtered = df[
    (df["station_uuid"] == STATION_ID) &
    (df["date"].dt.date == yesterday.date())
]
# ============================
# Nur gewünschte Spalten behalten
# ============================
df_result = df_filtered[["date", "diesel", "e10", "e5"]].copy()
# ============================
# Wochentag hinzufügen (0=Montag ... 6=Sonntag)
# ============================
df_result.loc[:, "weekday"] = df_result["date"].dt.weekday
# ============================
# 3. NRW-Feiertage laden
# ============================
nrw_holidays = holidays.Germany(years=df_result["date"].dt.year.unique(), subdiv="NW")

# Feiertag = 1, sonst 0
df_result.loc[:, "is_holiday"] = df_result["date"].dt.date.apply(lambda d: 1 if d in nrw_holidays else 0)
# ============================
# 4. NRW-Ferien über API laden
# ============================
ferien_url = "https://ferien-api.de/api/v1/holidays/NW"
ferien = requests.get(ferien_url).json()

# Ferienintervalle in Python-Daten umwandeln
ferien_ranges = [
    (pd.to_datetime(f["start"]).date(), pd.to_datetime(f["end"]).date())
    for f in ferien
]

def is_ferien(date):
    d = date.date()
    for start, end in ferien_ranges:
        if start <= d <= end:
            return 1
    return 0

df_result.loc[:, "is_vacation"] = df_result["date"].apply(is_ferien)
# ============================
# 5. Feiertag ODER Ferien?
# ============================
df_result.loc[:, "holiday"] = df_result[["is_holiday", "is_vacation"]].max(axis=1)
# ============================
# Ergebnis speichern
# ============================
df_result.to_csv(
    "hem_prices.csv",
    mode="a",          # anhängen statt überschreiben
    header=False,      # keine Kopfzeile erneut schreiben
    index=False
)
print("Fertig! Datei gespeichert.")
