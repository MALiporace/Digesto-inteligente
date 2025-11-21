import os
import zipfile
import io
import requests
import pandas as pd
import json
import chardet
from datetime import datetime, timedelta, timezone


# === 1. Configuración general ===
argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")


# === 2. Credenciales Dropbox (tus nombres de secrets) ===
DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")            # antes: DROPBOX_CLIENT_ID
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")     # antes: DROPBOX_CLIENT_SECRET
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")  # antes: DROPBOX_REFRESH_TOKEN


def obtener_access_token():
    """Genera un access_token nuevo usando TU refresh_token."""
    url = "https://api.dropbox.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def subir_a_dropbox(ruta_local, ruta_remota):
    """Sube un archivo a Dropbox renovando token automáticamente."""
    access_token = obtener_access_token()

    with open(ruta_local, "rb") as f:
        data = f.read()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream",
        "Dropbox-API-Arg": json.dumps({
            "path": ruta_remota,
            "mode": "overwrite",
            "autorename": False,
            "mute": False
        })
    }

    r = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers=headers,
        data=data
    )

    if r.status_code == 200:
        print(f"☁️  Subido correctamente a Dropbox: {ruta_remota}")
    else:
        print(f"⚠️ Error subiendo {ruta_local}: {r.text}")


# === 3. Carpeta local ===
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


# === 4. URLs oficiales (ZIP) ===
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
        r = requests.get(url)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))

        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            print(f"⚠️ No se encontró CSV dentro del ZIP de {nombre}")
            continue

        csv_name = csv_files[0]
        print(f"📄 Extrayendo {csv_name}...")

        with z.open(csv_name) as f:
            raw = f.read()
            enc = chardet.detect(raw)["encoding"] or "latin1"
            df = pd.read_csv(io.BytesIO(raw), low_memory=False, encoding=enc)


        destino = os.path.join(DATA_DIR, f"{nombre}.csv")
        df.to_csv(destino, index=False)

        total_descargados += len(df)
        print(f"✅ Guardado en {destino} ({len(df):,} filas)\n")

    except Exception as e:
        print(f"⚠️ Error procesando {nombre}: {e}\n")


# === 5. Subir a Dropbox (con eliminación previa) ===

def borrar_en_dropbox(path, token):
    """Elimina el archivo remoto si existe antes de subir el nuevo."""
    url = "https://api.dropboxapi.com/2/files/delete_v2"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = json.dumps({"path": path})
    r = requests.post(url, headers=headers, data=data)

    # 409 = file not found → no es error, se ignora
    if r.status_code == 200:
        print(f"🗑️ Eliminado en Dropbox: {path}")
    elif r.status_code == 409:
        print(f"ℹ️ No existía en Dropbox (bien): {path}")
    else:
        print(f"⚠️ Error eliminando {path}: {r.text}")


# Generar token una sola vez
access_token = obtener_access_token()

# Subir archivos de /data
for nombre in resources.keys():
    archivo_local = os.path.join(DATA_DIR, f"{nombre}.csv")
    archivo_remoto = f"/data/{nombre}.csv"

    borrar_en_dropbox(archivo_remoto, access_token)
    subir_a_dropbox(archivo_local, archivo_remoto)

print("☁️ Archivos /data actualizados en Dropbox.")
