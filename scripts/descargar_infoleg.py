# -*- coding: utf-8 -*-

import os
import zipfile
import io
import requests
import pandas as pd
import json
from datetime import datetime, timedelta, timezone

# ==========================================================
# Configuración general
# ==========================================================

argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")

DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ==========================================================
# URLs oficiales Infoleg
# ==========================================================

resources = {
    "infoleg_normativa":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/"
        "bf0ec116-ad4e-4572-a476-e57167a84403/download/base-infoleg-normativa-nacional.zip",

    "infoleg_modificadas":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/"
        "0c4fdafe-f4e8-4ac2-bc2e-acf50c27066d/download/base-complementaria-infoleg-normas-modificadas.zip",

    "infoleg_modificatorias":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/"
        "dea3c247-5a5d-408f-a224-39ae0f8eb371/download/base-complementaria-infoleg-normas-modificatorias.zip",
}

# ==========================================================
# Dropbox
# ==========================================================

def obtener_access_token():
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET
    }
    r = requests.post("https://api.dropbox.com/oauth2/token", data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def borrar_en_dropbox(path, token):
    url = "https://api.dropboxapi.com/2/files/delete_v2"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = json.dumps({"path": path})
    r = requests.post(url, headers=headers, data=body)

    if r.status_code == 200:
        print(f"🗑️ Eliminado remoto: {path}")
    elif r.status_code == 409:
        print(f"ℹ️ No existía (ok): {path}")
    else:
        print(f"⚠️ Error al borrar {path}: {r.text}")


def subir_a_dropbox(local_path, remote_path, token):
    with open(local_path, "rb") as f:
        data = f.read()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Dropbox-API-Arg": json.dumps({
            "path": remote_path,
            "mode": "overwrite",
            "autorename": False,
            "mute": False
        })
    }

    r = requests.post("https://content.dropboxapi.com/2/files/upload",
                      headers=headers, data=data)

    print(f"{remote_path} → {r.status_code}")

# ==========================================================
# Fix mojibake
# ==========================================================

def arreglar_mojibake(texto):
    if "Ã" in texto or "Â" in texto:
        try:
            texto = texto.encode("latin1", errors="ignore").decode("utf8", errors="ignore")
        except:
            pass
    return texto

# ==========================================================
# Descarga y extracción
# ==========================================================

print("🔍 Iniciando descarga Infoleg...\n")

for nombre, url in resources.items():
    print(f"⬇️ Descargando {nombre}\n   {url}")

    try:
        r = requests.get(url)
        r.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]

        if not csv_files:
            print(f"⚠️ No se encontró CSV en {nombre}")
            continue

        csv_name = csv_files[0]
        print(f"📄 Extrayendo {csv_name}...")

        # LECTURA DEL CSV COMO TEXTO
        with z.open(csv_name) as f:
            raw = f.read()

        # Detectamos encoding sin chardet
        for enc in ["utf-8", "latin1", "windows-1252", "cp1252"]:
            try:
                texto = raw.decode(enc)
                break
            except:
                texto = None

        if texto is None:
            texto = raw.decode("latin1", errors="replace")

        texto = arreglar_mojibake(texto)

        df = pd.read_csv(io.StringIO(texto), low_memory=False)

        destino = os.path.join(DATA_DIR, f"{nombre}.csv")
        df.to_csv(destino, index=False, encoding="utf-8")

        print(f"✅ Guardado: {destino} ({len(df):,} filas)\n")

    except Exception as e:
        print(f"❌ Error procesando {nombre}: {e}\n")

# ==========================================================
# Subir a Dropbox con eliminación previa
# ==========================================================

print("☁️ Subiendo a Dropbox...")

token = obtener_access_token()

for nombre in resources.keys():
    archivo_local = os.path.join(DATA_DIR, f"{nombre}.csv")
    archivo_remoto = f"/data/{nombre}.csv"

    borrar_en_dropbox(archivo_remoto, token)
    subir_a_dropbox(archivo_local, archivo_remoto, token)

print("✔ Finalizado correctamente.")


