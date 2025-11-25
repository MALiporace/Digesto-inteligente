# -*- coding: utf-8 -*-

import os
import json
import requests
import pandas as pd

# ============================================
# CONFIG (USANDO TUS SECRETS)
# ============================================

DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

DROPBOX_FOLDER_HTML = "/fichas_html"
DROPBOX_FOLDER_JSON = "/fichas_json"

# ============================================
# TOKEN DROPBOX
# ============================================

def dropbox_get_access_token():
    data = {
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET,
    }
    r = requests.post("https://api.dropbox.com/oauth2/token", data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# ============================================
# LISTAR ARCHIVOS (con paginación completa)
# ============================================

def dropbox_list_folder(path):
    token = dropbox_get_access_token()

    url = "https://api.dropboxapi.com/2/files/list_folder"
    continue_url = "https://api.dropboxapi.com/2/files/list_folder/continue"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # primera llamada
    r = requests.post(url, headers=headers, json={"path": path})
    if r.status_code != 200:
        print(f"No se pudo listar carpeta: {path}")
        return []

    data = r.json()
    entries = data.get("entries", [])

    # hacer paginación
    while data.get("has_more"):
        r = requests.post(continue_url, headers=headers, json={"cursor": data["cursor"]})
        r.raise_for_status()
        data = r.json()
        entries.extend(data.get("entries", []))

    # solo archivos
    return [e["name"] for e in entries if e[".tag"] == "file"]

# ============================================
# SUBIR ARCHIVO A DROPBOX
# ============================================

def dropbox_upload(path, content_bytes):
    token = dropbox_get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Dropbox-API-Arg": json.dumps({
            "path": path,
            "mode": "overwrite",
            "autorename": False
        })
    }

    r = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers=headers,
        data=content_bytes
    )
    if r.status_code not in (200, 409):
        raise Exception(f"Error subiendo a Dropbox: {r.text}")

# ============================================
# DELETE A DROPBOX
# ============================================

def dropbox_delete(path):
    token = dropbox_get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    requests.post(
        "https://api.dropboxapi.com/2/files/delete_v2",
        headers=headers,
        json={"path": path}
    )

# ============================================
# PRINCIPAL
# ============================================

if __name__ == "__main__":

    print("📌 Leyendo digesto_normas.csv local...")
    df = pd.read_csv("data_procesada/digesto_normas.csv", dtype=str)

    print("📌 Listando HTML en Dropbox (con paginación)...")
    archivos_html = dropbox_list_folder(DROPBOX_FOLDER_HTML)
    ids_html = {fname.replace(".html", "") for fname in archivos_html}

    print("📌 Listando JSON en Dropbox (con paginación)...")
    archivos_json = dropbox_list_folder(DROPBOX_FOLDER_JSON)
    ids_json = {fname.replace(".json", "") for fname in archivos_json}

    print(f"✔ HTML encontrados: {len(ids_html)}")
    print(f"✔ JSON encontrados: {len(ids_json)}")

    print("📌 Reset de columnas...")
    df["ficha_descargada"] = False
    df["ficha_parseada"] = False

    print("📌 Calculando columnas...")
    df["ficha_descargada"] = df["id_norma"].apply(lambda x: x in ids_html)
    df["ficha_parseada"] = df["id_norma"].apply(lambda x: x in ids_json)

    print("📌 Guardando CSV actualizado localmente...")
    df.to_csv("data_procesada/digesto_normas.csv", index=False, encoding="utf-8")

    print("📌 Subiendo digesto_normas.csv a Dropbox...")
    with open("data_procesada/digesto_normas.csv", "rb") as f:
        dropbox_upload("/data_procesada/digesto_normas.csv", f.read())

    # --- digesto_relaciones.csv ---
    rel_path = "data_procesada/digesto_relaciones.csv"
    if os.path.exists(rel_path):
        print("📌 Eliminando remoto: /data_procesada/digesto_relaciones.csv")
        dropbox_delete("/data_procesada/digesto_relaciones.csv")

        print("📌 Subiendo nuevo digesto_relaciones.csv a Dropbox (delete + recreate)...")
        with open(rel_path, "rb") as f:
            dropbox_upload("/data_procesada/digesto_relaciones.csv", f.read())
    else:
        print("⚠️ Aviso: data_procesada/digesto_relaciones.csv no existe en este run.")

    print("✔ Sincronización de fichas completada.")
