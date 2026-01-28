import pandas as pd

df = pd.read_csv("hem_prices.csv")
df = df.drop_duplicates()
df.to_csv("hem_prices.csv", index=False)
