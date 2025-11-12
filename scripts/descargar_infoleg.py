import os
import zipfile
import io
import requests
import pandas as pd
import json
from datetime import datetime, timedelta, timezone


# === 1. Configuración general ===
argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")

def subir_a_dropbox(ruta_local, ruta_remota):
    """Sube un archivo a Dropbox usando el token temporal."""
    with open(ruta_local, "rb") as f:
        data = f.read()

    headers = {
        "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
        "Content-Type": "application/octet-stream",
        "Dropbox-API-Arg": json.dumps({
            "path": ruta_remota,
            "mode": "overwrite",
            "autorename": False,
            "mute": False
        })
    }

    r = requests.post("https://content.dropboxapi.com/2/files/upload", headers=headers, data=data)
    if r.status_code == 200:
        print(f"☁️  Subido correctamente a Dropbox: {ruta_remota}")
    else:
        print(f"⚠️ Error subiendo {ruta_local}: {r.text}")



# === 2. Credenciales Dropbox ===
DROPBOX_ACCESS_TOKEN = "sl.u.AGGZpr6agcDMcUoKuSVHU7FJxKFQdQizJougXwz3Lk7hhz6m7KY0GlfeNWiag9BGXbdgI0_ZiAr7yQ12mDIihkhKGhk3yyw_ILydZhIyyLjLqk94BWgNrBUx4RlikFTC5xYTR2Bu9qdP9-CcgZYxW_ONMt1PbzR6Ls4AYGmd_8aYEuVSpuRGSjjocwD3HqMQbyKd3qfRQOTH_G-EocKumwkUGj8cZf_if3Fw64-JDcBhhb9m0eZtVNRpRIOREH1LMLpo3ejoaoS4Hyq8gwzaMhbUdCGnb-JBg9gHYOeGFU96medyeq5C_TT6CxbPYOf9hO7ACaKWDYQ-ryTaPZYxXMeSnBLfJsoCKGBcqWlbmVlSZ5AXN8f5ltzJhPmMWuoU3-iKRFgU7j3lkXmpJMrXTPHPflN4-XxWOp5jKEVbJ_Cdu2cspocHIShdG5I1qY9A7_6QPpIWb6eKqSwODb8qYmhUWwTU9cYicBPm-bj0eiQvtKQvsD7H0BqHSfLaepS_q-LFUWALD07EPDsHb5ftDz4Q0XJMoo-XjNY2H5W5ZOjQjoQCTs5TYjzEIjxmY8hT_kNwH0pioHWC1bkzBWumATiTxi6xpLURZpQtFvGqCiByIYF7m11xDFSZ6M1Jg9qqnj_n-nbnZQIbeucgEMZYUMVUqo3oY9WGxCQPEJpFPt4A3_JeiKYZ6PKwJn1BU-_rhyBUiI8vpG-ISoV9aQUDa_tyoIwO0cn5wU_M7Lb20sd9jf_gDL_i8HvKW7z0Q2c9GHcf-sz9fGCRX7llWRmckHiGZOWtb2fmn214Psx64Sjf_YG0ZPnQsQt1blvN5dOR1lgH3hm5rwS5cEiQu_fY2YwcnNed8VA8ikiSDzXLNMJ05QYYyNOAblz_VJfKxxMw4iOqQU7BTyabCuetcZnrJ7nnNb7DJrDTe3RhwEFLJLiugicHYyiWnwufOTpLocAMr9aIHpgE462RXzI-cdHS4YKwWMpDJvG_Lq0WBzrDSDUHpuwW05DoEFAYhFewGgGr5MwUoz83SejWyAYy7Br81ZDwsq8DQqckxHl7EMvDIo36hbKDtYqwIPpijy73YnIPLpgTgCdA2sh1z9l5WitQK7_aL0vrTmygkxeKmXoNWpl3rQRFfLc3zzgZJN34x9hC4TmqWWOGDuFB3YKXn3ex-bdG-bp52tyaBO6vg1bCSgZ4s4HSz9LFTCLkx1vl5a-pP87LAyikTw1SsF1M6qg9Yiy4FzWu0e6o_LkavuKkMUb5KRnOJi7SUml7vODWJjAhu8LDAVpB_3Vjh7ISBxu0m7oVtcwsdPDXvHANqFXjP7PlrQ"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# === 3. URLs oficiales (ZIP) ===
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

# === 4. Subir a Dropbox ===
for nombre in resources.keys():
    archivo_local = os.path.join(DATA_DIR, f"{nombre}.csv")
    archivo_remoto = f"/data/{nombre}.csv"
    subir_a_dropbox(archivo_local, archivo_remoto)



print(f"🧾 {timestamp} - Descarga completada. Total filas acumuladas: {total_descargados:,}")
