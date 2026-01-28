import pandas as pd
from datetime import datetime, timedelta, timezone

# CSV laden
df = pd.read_csv("hem_prices.csv", parse_dates=["date"])

# Duplikate entfernen
df = df.drop_duplicates()

# cutoff als tz-aware erzeugen
cutoff = datetime.now(timezone.utc).astimezone(df["date"].dt.tz)

# Grenze: heute minus 22 Tage
# Nur Zeilen behalten, deren Datum >= cutoff ist
df = df[df["date"] >= cutoff - timedelta(days=22)]

# Zurückschreiben
df.to_csv("hem_prices.csv", index=False)
