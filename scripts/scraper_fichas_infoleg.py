# -*- coding: utf-8 -*-

import os
import time
import json
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://servicios.infoleg.gob.ar/infolegInternet/verNorma.do?id="

BASE_FICHAS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data_procesada",
    "fichas"
)

BASE_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data_procesada",
    "fichas_parseadas"
)

os.makedirs(BASE_FICHAS, exist_ok=True)
os.makedirs(BASE_JSON, exist_ok=True)


# ============================================
# Descargar HTML con reintentos
# ============================================

def descargar_html(id_norma, sleep=1.0):
    url = BASE_URL + str(id_norma)

    r = None
    intentos = 0
    while intentos < 3:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.text
        except:
            pass

        intentos += 1
        time.sleep(1.5)

    return None


# ============================================
# Extraer texto limpio
# ============================================

def clean(text):
    if not text:
        return None
    return " ".join(text.split()).strip()


# ============================================
# Parsear relaciones del HTML
# ============================================

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

    bloques = soup.find_all("p")

    for p in bloques:
        txt = p.get_text(strip=True).lower()

        enlaces = [
            (a.get_text(strip=True), a.get("href"))
            for a in p.find_all("a")
        ]

        # Detectamos por palabras clave (Infoleg es estable)
        if "modifica a" in txt or "modifica a:" in txt:
            relaciones["modifica"].extend(enlaces)

        if "modificada por" in txt or "modificada por:" in txt:
            relaciones["es_modificada_por"].extend(enlaces)

        if "complementa a" in txt:
            relaciones["complementa"].extend(enlaces)

        if "complementada por" in txt:
            relaciones["es_complementada_por"].extend(enlaces)

        if "reglamenta a" in txt:
            relaciones["reglamenta"].extend(enlaces)

        if "reglamentada por" in txt:
            relaciones["es_reglamentada_por"].extend(enlaces)

        if "cita a" in txt:
            relaciones["cita"].extend(enlaces)

        if "citada por" in txt:
            relaciones["es_citada_por"].extend(enlaces)

    return relaciones


# ============================================
# Parsear ficha completa
# ============================================

def parsear_ficha(id_norma):
    ruta_json = os.path.join(BASE_JSON, f"{id_norma}.json")
    ruta_html = os.path.join(BASE_FICHAS, f"{id_norma}.html")

    # ----------------------------------------
    # CACHÉ
    # ----------------------------------------
    if os.path.exists(ruta_json):
        with open(ruta_json, "r", encoding="utf-8") as f:
            return json.load(f)

    # Si existe HTML pero no JSON, lo parseamos igual
    if os.path.exists(ruta_html):
        with open(ruta_html, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        html = descargar_html(id_norma)
        if not html:
            return None
        with open(ruta_html, "w", encoding="utf-8") as f:
            f.write(html)

    soup = BeautifulSoup(html, "html.parser")

    # ----------------------------------------
    # Extracción
    # ----------------------------------------
    box = soup.find("div", {"id": "Textos_Completos"})
    if not box:
        return None

    # Título
    titulo_block = box.find("p")
    titulo = clean(titulo_block.get_text()) if titulo_block else None

    # Elementos principales
    strongs = box.find_all("strong")
    resumen = None
    if strongs:
        for s in strongs:
            if "resumen" in s.text.lower():
                resumen = clean(s.find_next("p").get_text())

    # H1
    h1 = clean(box.find("h1").get_text()) if box.find("h1") else None

    # Extracto
    destacado = box.find("span", {"class": "destacado"})
    extracto = clean(destacado.get_text()) if destacado else None

    # Fecha publicación
    publicados = box.find_all("a")
    fecha_bo = None
    numero_bo = None
    for a in publicados:
        if "page_id=216" in (a.get("href") or ""):
            fecha_bo = clean(a.get_text())
            numero_bo = fecha_bo  # Infoleg repite número/fecha igual

    # Relaciones
    relaciones = extraer_relaciones(soup)

    # Anexos
    anexos = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and ("anexos" in href or "adjuntos" in href):
            anexos.append(href)

    # ----------------------------------------
    # JSON final
    # ----------------------------------------
    data = {
        "id_norma": str(id_norma),
        "titulo": titulo,
        "extracto": extracto,
        "h1": h1,
        "resumen": resumen,
        "publicacion_bo": {
            "boletin_infoleg": fecha_bo,
            "numero": numero_bo
        },
        "relaciones": relaciones,
        "anexos": anexos
    }

    # Guardar JSON
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data


# ============================================
# USO
# ============================================
if __name__ == "__main__":
    test_id = 283855
    print(parsear_ficha(test_id))
