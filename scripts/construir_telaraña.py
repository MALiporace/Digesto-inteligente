# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import requests

# ================================
# SECRETS
# ================================

DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

DROPBOX_JSON_FOLDER = "/fichas_json"

# ================================
# DROPBOX OPERATIONS
# ================================

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


def dropbox_list_json():
    """Lista TODOS los JSON con paginación completa."""
    token = dropbox_get_access_token()
    url = "https://api.dropboxapi.com/2/files/list_folder"
    continue_url = "https://api.dropboxapi.com/2/files/list_folder/continue"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    r = requests.post(url, headers=headers, json={"path": DROPBOX_JSON_FOLDER})
    r.raise_for_status()

    data = r.json()
    entries = data.get("entries", [])

    # paginación
    while data.get("has_more"):
        r = requests.post(continue_url, headers=headers, json={"cursor": data["cursor"]})
        r.raise_for_status()
        data = r.json()
        entries.extend(data.get("entries", []))

    archivos = [
        e["name"] for e in entries
        if e[".tag"] == "file" and e["name"].endswith(".json")
    ]

    return archivos


def dropbox_download(path):
    token = dropbox_get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": json.dumps({"path": path})
    }
    r = requests.post("https://content.dropboxapi.com/2/files/download",
                      headers=headers)
    if r.status_code == 200:
        return r.content
    return None


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
    requests.post("https://content.dropboxapi.com/2/files/upload",
                  headers=headers,
                  data=content_bytes)


def dropbox_delete(path):
    token = dropbox_get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    requests.post("https://api.dropboxapi.com/2/files/delete_v2",
                  headers=headers,
                  json={"path": path})


# ================================
# EXTRACCIÓN DE RELACIONES
# ================================

def extraer_relaciones_json(json_data):
    id_origen = json_data["id_norma"]
    rels = []

    # 1) Relaciones oficiales presentes en la ficha
    for tipo, lista in json_data["relaciones"].items():
        for txt, url in lista:
            if "id=" in url:
                id_dest = url.split("id=")[-1]
                rels.append((id_origen, id_dest, tipo, "infoleg_ficha"))

    # 2) Relaciones embebidas por ID
    for dest in json_data["deep"].get("normas_mencionadas", []):
        rels.append((id_origen, dest, "menciona", "embebida_link"))

    # 3) Relaciones por texto libre
    for txt in json_data["deep"].get("normas_mencionadas_texto", []):
        # Ejemplo: "Ley 1173"
        numero = txt.split(" ")[-1]
        rels.append((id_origen, numero, "menciona", "texto_plano"))

    return rels


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Leyendo CSV oficiales...")
    df_oficial = pd.read_csv("data_procesada/digesto_relaciones.csv", dtype=str)

    print("Listando JSON en Dropbox...")
    archivos = dropbox_list_json()
    print(f"✔ JSON detectados: {len(archivos)}")

    relaciones_expandidas = []

    for file in archivos:
        path = f"{DROPBOX_JSON_FOLDER}/{file}"
        contenido = dropbox_download(path)

        if not contenido:
            print(f"⚠ No se pudo descargar: {file}")
            continue

        data = json.loads(contenido.decode("utf-8"))
        relaciones_expandidas.extend(extraer_relaciones_json(data))

    # dataframe extra
    df_extra = pd.DataFrame(
        relaciones_expandidas,
        columns=["id_origen", "id_destino", "tipo_relacion", "fuente"]
    )

    # dataframe oficial
    df_oficial2 = df_oficial.copy()
    df_oficial2["fuente"] = "infoleg_csv"

    # unión final
    df_final = pd.concat([df_oficial2, df_extra], ignore_index=True)

    df_final = df_final.dropna(subset=["id_origen", "id_destino"])
    df_final = df_final.drop_duplicates()

    out_path = "data_procesada/digesto_relaciones_expandido.csv"
    df_final.to_csv(out_path, index=False, encoding="utf-8")

    # ====== DELETE + RECREATE como pediste ======
    print("📌 Eliminando remoto expandido previo...")
    dropbox_delete("/data_procesada/digesto_relaciones_expandido.csv")

    print("📌 Subiendo nuevo expandido...")
    with open(out_path, "rb") as f:
        dropbox_upload("/data_procesada/digesto_relaciones_expandido.csv", f.read())

    print("✔ Telaraña jurídica generada y actualizada en Dropbox.")

