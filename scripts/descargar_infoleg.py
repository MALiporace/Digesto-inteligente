import os
import zipfile
import io
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# === 1. Configuración general ===
argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# === 2. URLs oficiales (ZIP) ===
resources = {
    "infoleg_normativa": "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/bf0ec116-ad4e-4572-a476-e57167a84403/download/base-infoleg-normativa-nacional.zip",
    "infoleg_modificadas": "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/0c4fdafe-f4e8-4ac2-bc2e-acf50c27066d/download/base-complementaria-infoleg-normas-modificadas.zip",
    "infoleg_modificatorias": "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/dea3c247-5a5d-408f-a224-39ae0f8eb371/download/base-complementaria-infoleg-normas-modificatorias.zip",
}

total_descargados = 0

print("🔍 Iniciando descarga de datasets oficiales de Infoleg...\n")

for nombre, url in resources.items():
    print(f"⬇️  Descargando {nombre} desde:\n   {url}")

    try:
        # Descargar el ZIP en memoria
        r = requests.get(url)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))

        # Buscar el CSV dentro del ZIP
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            print(f"⚠️ No se encontró CSV dentro del ZIP de {nombre}")
            continue

        csv_name = csv_files[0]
        print(f"📄 Extrayendo {csv_name}...")

        with z.open(csv_name) as f:
            df = pd.read_csv(f, low_memory=False)

        destino = os.path.join(DATA_DIR, f"{nombre}.csv")
        df.to_csv(destino, index=False)

        total_descargados += len(df)
        print(f"✅ Guardado en {destino} ({len(df):,} filas)\n")

    except Exception as e:
        print(f"⚠️ Error procesando {nombre}: {e}\n")

print(f"🧾 {timestamp} - Descarga completada. Total filas acumuladas: {total_descargados:,}")
