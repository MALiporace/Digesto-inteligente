# -*- coding: utf-8 -*-

import os
import json
import time
import requests
from bs4 import BeautifulSoup

# ============================================
# CONFIG (USANDO TUS SECRETS)
# ============================================

DROPBOX_CLIENT_ID = os.environ.get("APP_KEY")
DROPBOX_CLIENT_SECRET = os.environ.get("APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

BASE_URL = "https://servicios.infoleg.gob.ar/infolegInternet/verNorma.do?id="

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
# SUBIR A DROPBOX
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
# DESCARGAR DESDE DROPBOX
# ============================================

def dropbox_download(path):
    token = dropbox_get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": json.dumps({"path": path})
    }

    r = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers=headers
    )

    if r.status_code == 200:
        return r.content

    return None


# ============================================
# SCRAPE INFOLEG
# ============================================

def descargar_html_infoleg(id_norma):
    url = BASE_URL + str(id_norma)

    for intento in range(3):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.text
        except:
            pass
        time.sleep(1.5)

    return None


# ============================================
# PARSEOS
# ============================================

def clean(x):
    if not x:
        return None
    return " ".join(x.split()).strip()


def extraer_relaciones(soup):
    relaciones = {
        "modifica": [],
        "es_modificada_por": [],
        "cita": [],
        "es_citada_por": [],
        "complementa": [],
        "es_complementada_por": [],
        "reglamenta": [],
        "es_reglamentada_por": []
    }

    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True).lower()
        links = [(a.get_text(strip=True), a.get("href"))
                 for a in p.find_all("a")]

        if "modifica a" in txt:
            relaciones["modifica"].extend(links)
        if "modificada por" in txt:
            relaciones["es_modificada_por"].extend(links)
        if "complementa a" in txt:
            relaciones["complementa"].extend(links)
        if "complementada por" in txt:
            relaciones["es_complementada_por"].extend(links)
        if "reglamenta a" in txt:
            relaciones["reglamenta"].extend(links)
        if "reglamentada por" in txt:
            relaciones["es_reglamentada_por"].extend(links)
        if "cita a" in txt:
            relaciones["cita"].extend(links)
        if "citada por" in txt:
            relaciones["es_citada_por"].extend(links)

    return relaciones


def parsear_html(id_norma, html):
    soup = BeautifulSoup(html, "html.parser")

    box = soup.find("div", {"id": "Textos_Completos"})
    if not box:
        return None

    titulo_block = box.find("p")
    titulo = clean(titulo_block.get_text()) if titulo_block else None

    destacado = box.find("span", {"class": "destacado"})
    extracto = clean(destacado.get_text()) if destacado else None

    h1 = clean(box.find("h1").get_text()) if box.find("h1") else None

    resumen = None
    for strong in box.find_all("strong"):
        if "resumen" in strong.get_text(strip=True).lower():
            nxt = strong.find_next("p")
            if nxt:
                resumen = clean(nxt.get_text())

    fecha_bo = None
    nro_bo = None
    for a in box.find_all("a"):
        href = a.get("href") or ""
        if "page_id=216" in href:
            fecha_bo = clean(a.get_text())
            nro_bo = fecha_bo

    anexos = []
    for a in soup.find_all("a"):
        if a.get("href") and ("anexos" in a.get("href") or "adjuntos" in a.get("href")):
            anexos.append(a.get("href"))

    relaciones = extraer_relaciones(soup)

    return {
        "id_norma": str(id_norma),
        "titulo": titulo,
        "extracto": extracto,
        "h1": h1,
        "resumen": resumen,
        "publicacion_bo": {
            "fecha": fecha_bo,
            "numero": nro_bo
        },
        "relaciones": relaciones,
        "anexos": anexos
    }


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def obtener_ficha(id_norma):
    json_path = f"{DROPBOX_FOLDER_JSON}/{id_norma}.json"
    html_path = f"{DROPBOX_FOLDER_HTML}/{id_norma}.html"

    contenido_json = dropbox_download(json_path)
    if contenido_json:
        return json.loads(contenido_json.decode("utf-8"))

    contenido_html = dropbox_download(html_path)
    if contenido_html:
        html = contenido_html.decode("utf-8")
        data = parsear_html(id_norma, html)
        if data:
            dropbox_upload(
                json_path,
                json.dumps(data, ensure_ascii=False).encode("utf-8")
            )
        return data

    html = descargar_html_infoleg(id_norma)
    if not html:
        return None

    dropbox_upload(html_path, html.encode("utf-8"))

    data = parsear_html(id_norma, html)
    if data:
        dropbox_upload(
            json_path,
            json.dumps(data, ensure_ascii=False).encode("utf-8")
        )

    return data


# ============================================
# TEST
# ============================================

if __name__ == "__main__":
    print(obtener_ficha(283855))

