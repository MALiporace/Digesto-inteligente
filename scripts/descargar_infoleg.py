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

# === 2. Credenciales Dropbox (vienen desde GitHub Secrets) ===
DROPBOX_REFRESH_TOKEN = os.environ.get("DROPBOX_REFRESH_TOKEN")
DROPBOX_CLIENT_ID = os.environ.get("DROPBOX_CLIENT_ID")
DROPBOX_CLIENT_SECRET = os.environ.get("DROPBOX_CLIENT_SECRET")


def obtener_access_token():
    """Genera un nuevo access_token válido usando el refresh_token."""
    url = "https://api.dropbox.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json()["access_token"]
    return token


def subir_a_dropbox(ruta_local, ruta_remota):
    """Sube un archivo a Dropbox con un access_token recién generado."""
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

    r = requests.post("https://content.dropboxapi.com/2/files/upload",
                      headers=headers,
                      data=data)

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
            print(f"⚠️ No se encont
