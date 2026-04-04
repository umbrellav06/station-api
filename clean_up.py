import pandas as pd
from datetime import datetime, timedelta

# CSV laden
df = pd.read_csv("hem_prices.csv", parse_dates=["date"])

# Duplikate entfernen
df = df.drop_duplicates()

# cutoff als tz-naive erzeugen
cutoff = datetime.now().replace(tzinfo=None)

# Grenze: heute minus 22 Tage
df = df[df["date"] >= cutoff - timedelta(days=22)]

# Zurückschreiben
df.to_csv("hem_prices.csv", index=False)
