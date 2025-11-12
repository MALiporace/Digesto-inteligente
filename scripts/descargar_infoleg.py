import requests, pandas as pd

url_ckan = "https://datos.gob.ar/api/3/action/package_search?q=infoleg"
data = requests.get(url_ckan).json()
infoleg = data["result"]["results"][0]

csv_url = infoleg["resources"][0]["url"]
print("Descargando:", csv_url)
df = pd.read_csv(csv_url)
df.to_csv("data/infoleg_raw.csv", index=False)
