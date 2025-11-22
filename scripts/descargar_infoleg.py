import os
import zipfile
import io
import requests
import pandas as pd
import json
from datetime import datetime, timedelta, timezone
import chardet


# ===============================================================
# 1. CONFIG GENERAL
# ===============================================================
argentina_tz = timezone(timedelta(hours=-3))
timestamp = datetime.now(argentina_tz).strftime("%Y-%m-%d %H:%M:%S")


# ===============================================================
# 2. CREDENCIALES DESDE SECRETS DE GITHUB
# ===============================================================
DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")


def obtener_access_token():
    """Renueva Access Token usando tu Refresh Token."""
    url = "https://api.dropbox.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET,
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def subir_a_dropbox(ruta_local, ruta_remota, access_token):
    """Sube archivo usando upload API."""
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

    print(f"{ruta_remota} → {r.status_code}")


def borrar_en_dropbox(path, token):
    """Elimina archivo remoto antes de subir nuevo."""
    url = "https://api.dropboxapi.com/2/files/delete_v2"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    data = json.dumps({"path": path})
    r = requests.post(url, headers=headers, data=data)

    if r.status_code == 200:
        print(f"🗑️ Eliminado en Dropbox: {path}")
    elif r.status_code == 409:
        print(f"ℹ️ No existía (OK): {path}")
    else:
        print(f"⚠️ Error eliminando {path}: {r.text}")


# ===============================================================
# 3. RUTA LOCAL
# ===============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


# ===============================================================
# 4. URLs OFICIALES (COMPLETOS)
# ===============================================================
resources = {
    "infoleg_normativa":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/bf0ec116-ad4e-4572-a476-e57167a84403/download/base-infoleg-normativa-nacional.zip",

    "infoleg_modificadas":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/0c4fdafe-f4e8-4ac2-bc2e-acf50c27066d/download/base-complementaria-infoleg-normas-modificadas.zip",

    "infoleg_modificatorias":
        "https://datos.jus.gob.ar/dataset/d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23/resource/dea3c247-5a5d-408f-a224-39ae0f8eb371/download/base-complementaria-infoleg-normas-modificatorias.zip",
}


print("🔍 Iniciando descarga de datasets oficiales de Infoleg...\n")


# ===============================================================
# 5. DESCARGA + LECTURA + FIX ENCODING
# ===============================================================
total_descargados = 0

for nombre, url in resources.items():
    print(f"⬇️  Descargando {nombre} desde:\n   {url}")

    try:
        r = requests.get(url)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))

        # buscar CSV dentro del ZIP
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            print(f"⚠️ No se encontró CSV dentro del ZIP de {nombre}")
            continue

        csv_name = csv_files[0]
        print(f"📄 Extrayendo {csv_name}...")

# =============================
# LECTURA + FIX MOJIBAKE REAL
# =============================
with z.open(csv_name) as f:
    raw = f.read()

# Detectar encoding real (aunque venga mal declarado)
det = chardet.detect(raw)
encoding_detectada = det["encoding"] or "latin1"

# 1) decodificar según lo detectado
texto = raw.decode(encoding_detectada, errors="replace")

# 2) aplicar fix de mojibake si el texto tiene patrones “Ã”
if "Ã" in texto or "Â" in texto:
    try:
        # re-intento de decodificación doble (UTF8 leído como latin1)
        texto = texto.encode("latin1", errors="ignore").decode("utf8", errors="ignore")
    except:
        pass

# 3) cargar finalmente en pandas
df = pd.read_csv(io.StringIO(texto), low_memory=False)


        # guardar UTF-8 limpio
        destino = os.path.join(DATA_DIR, f"{nombre}.csv")
        df.to_csv(destino, index=False, encoding="utf-8")

        total_descargados += len(df)
        print(f"✅ Guardado en {destino} ({len(df):,} filas)\n")

    except Exception as e:
        print(f"⚠️ Error procesando {nombre}: {e}\n")


# ===============================================================
# 6. SUBIR A DROPBOX
# ===============================================================
token = obtener_access_token()

for nombre in resources.keys():
    archivo_local = os.path.join(DATA_DIR, f"{nombre}.csv")
    archivo_remoto = f"/data/{nombre}.csv"

    borrar_en_dropbox(archivo_remoto, token)
    subir_a_dropbox(archivo_local, archivo_remoto, token)

print("☁️ Archivos /data actualizados en Dropbox.")
print(f"🧾 {timestamp} - Descarga completada. Total filas acumuladas: {total_descargados:,}")

