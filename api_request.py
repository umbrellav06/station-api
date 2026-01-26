import requests
import pandas as pd
from datetime import datetime
import holidays

API_KEY = 'f1a82c03-ddcf-487d-a77c-171b4e2b044c'

STATION_ID = '686b1552-ded0-4295-ae9c-30a03b3bfef0'

# Deutsche Feiertage in NRW
feiertage = holidays.Germany(state="NW")

url = f"https://creativecommons.tankerkoenig.de/json/prices.php?ids={STATION_ID}&apikey={API_KEY}"

try:
    response = requests.get(url).json()
except Exception as e:
    raise Exception(e)

if response.get("ok"):
    prices = response["prices"][STATION_ID]

    now = datetime.now()

    # DataFrame mit einer einzigen Zeile
    df = pd.DataFrame([{
        "year": now.year, 
        "month": now.month, 
        "day": now.day,
        "hour": now.hour, 
        "minute": now.minute,
        "weekday": now.weekday(), # Montag=0, Sonntag=6 
        "holiday": 1 if now.date() in feiertage else 0,
        "e10": prices.get("e10"),
        "e5": prices.get("e5"),
        "diesel": prices.get("diesel")
    }])

    # CSV anhängen (Header nur beim ersten Mal)
    df.to_csv(
        "preise.csv", 
        mode="a", 
        header=not pd.io.common.file_exists("preise.csv"), 
        index=False
    )

    print("Daten gespeichert.")
else:
    print("Fehler:", response.get("message"))
