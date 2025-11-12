import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# === Configuración general ===
argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")

# Carpeta de destino
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# URL base del catálogo CKAN
url_ckan = "https://datos.gob.ar/api/3/action/package_search?q=infoleg"

print(f"🔍 Consultando catálogo CKAN ({url_ckan}) ...")
response = requests.get(url_ckan)
response.raise_for_status()
data = response.json()

datasets = data["result"]["results"]
total_descargados = 0

for ds in datasets:
    title = ds["title"]
    if "Infoleg" not in title:
        continue

    print(f"\n📦 Dataset detectado: {title}")
    for resource in ds["resources"]:
        if resource["format"].lower() == "csv":
            csv_url = resource["url"]
            nombre = resource["name"].strip().replace(" ", "_").lower()
            nombre_archivo = f"{nombre}.csv"
            destino = os.path.join(DATA_DIR, nombre_archivo)

            print(f"⬇️  Descargando: {csv_url}")
            try:
                df = pd.read_csv(csv_url)
                df.to_csv(destino, index=False)
                total_descargados += len(df)
                print(f"✅ Guardado en {destino} ({len(df):,} filas)")
            except Exception as e:
                print(f"⚠️ Error descargando {csv_url}: {e}")

print(f"\n🧾 {timestamp} - Descarga completada. Total filas acumuladas: {total_descargados:,}")
