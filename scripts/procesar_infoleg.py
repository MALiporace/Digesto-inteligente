# -*- coding: utf-8 -*-

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ==================================================
# Rutas fijas para entorno de GitHub Actions
# ==================================================

BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data"
)

BASE_PROCESADA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data_procesada"
)

os.makedirs(BASE_PROCESADA, exist_ok=True)

# ==================================================
# Helpers generales
# ==================================================

def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")


def reparar_mojibake_texto(x: str) -> str:
    """
    Repara mojibake típico de Infoleg:
    - ResoluciÃ³n -> Resolución
    - PequeÃ±a    -> Pequeña
    - nÂº         -> nº
    """
    if not isinstance(x, str):
        return x

    if "Ã" not in x and "Â" not in x and "Ð" not in x:
        return x

    try:
        return x.encode("latin1").decode("utf-8")
    except Exception:
        return x


def reparar_mojibake_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la reparación de mojibake a todas las celdas string del DF.
    """
    return df.applymap(reparar_mojibake_texto)


def leer_csv_reforzado(path: str) -> pd.DataFrame:
    """
    Lector robusto para los CSV de Infoleg:
    - prueba utf-8
    - prueba utf-8-sig si hace falta
    - repara mojibake a nivel DF
    """
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except Exception:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    df = reparar_mojibake_df(df)
    return df

# ==================================================
# Reconstrucción de URL y scraping de Resumen
# ==================================================

def reconstruir_url_infoleg(id_norma):
    """
    Para normas sin texto original cargado en Infoleg,
    la URL 'real' que existe es la ficha dinámica:
    https://servicios.infoleg.gob.ar/infolegInternet/verNorma.do?id={id_norma}
    """
    return f"https://servicios.infoleg.gob.ar/infolegInternet/verNorma.do?id={id_norma}"


def obtener_resumen_infoleg(id_norma):
    """
    Scrapea la página verNorma.do?id={id_norma} y devuelve el Resumen.

    Estructura HTML típica (ejemplo real que pasaste):

    <p>
      <strong> Resumen:</strong><br>
      SE DECLARA PROCEDENTE ...
    </p>

    Estrategia:
    - Buscar <strong> que contenga 'Resumen'
    - Tomar el texto del <p> padre
    - Quitar la etiqueta 'Resumen:'
    """
    url = reconstruir_url_infoleg(id_norma)

    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None

        # El HTML declara charset ISO-8859-1
        r.encoding = "ISO-8859-1"
        soup = BeautifulSoup(r.text, "html.parser")

        # Buscar <strong> con la palabra 'Resumen'
        strong = soup.find("strong", string=lambda s: s and "Resumen" in s)
        if not strong:
            return None

        p_tag = strong.parent
        if not p_tag:
            return None

        # Extraer texto del <p>, limpiando el "Resumen:"
        texto = p_tag.get_text(separator=" ", strip=True)
        texto = texto.replace("Resumen:", "").replace("RESUMEN:", "").strip()

        return texto if texto else None

    except Exception:
        return None

# ==================================================
# Procesamiento principal
# ==================================================

print("Procesando Infoleg...")

# 1) Cargar datasets crudos
df_norm = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_normativa.csv"))
df_modif = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificadas.csv"))
df_modifatorias = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificatorias.csv"))

# ==================================================
# 2. Crear maestro de normas (digesto_normas)
# ==================================================

print("Generando digesto_normas.csv...")

df_digesto_normas = pd.DataFrame({
    "id_norma": df_norm["id_norma"],
    "tipo_norma": df_norm["tipo_norma"],
    "numero_norma": df_norm["numero_norma"],
    "fecha_sancion": normalizar_fecha(df_norm["fecha_sancion"]),
    "organismo": df_norm["organismo_origen"],
    "titulo_resumido": df_norm["titulo_resumido"],
    "titulo_sumario": df_norm["titulo_sumario"],
    "fecha_publicacion": normalizar_fecha(df_norm["fecha_boletin"]),
    "estado": "",
    "fuente": "Infoleg",
    "url_texto_original": df_norm["texto_original"],
    "url_texto_actualizado": df_norm["texto_actualizado"],
})

# Reparación final de mojibake
df_digesto_normas = reparar_mojibake_df(df_digesto_normas)

# Normalizar columna URL original (tratar vacío como faltante)
df_digesto_normas["url_texto_original"] = (
    df_digesto_normas["url_texto_original"]
    .astype("string")
    .replace({"": pd.NA})
)

# Identificar normas sin texto original cargado
mask_sin_texto_original = df_digesto_normas["url_texto_original"].isna()

# Reconstruir URL dinámica verNorma.do para esas normas
df_digesto_normas.loc[mask_sin_texto_original, "url_texto_original"] = (
    df_digesto_normas.loc[mask_sin_texto_original, "id_norma"]
    .apply(reconstruir_url_infoleg)
)

# Crear columna para guardar el Resumen de Infoleg
df_digesto_normas["resumen_infoleg"] = pd.NA

# Scraping de resumen SOLO para normas sin texto original "real"
print("Recuperando 'Resumen' de Infoleg para normas sin texto original completo...")

indices_a_procesar = df_digesto_normas[mask_sin_texto_original].index

for idx in indices_a_procesar:
    id_norma = df_digesto_normas.at[idx, "id_norma"]
    resumen = obtener_resumen_infoleg(id_norma)
    df_digesto_normas.at[idx, "resumen_infoleg"] = resumen

# Guardar archivo maestro
df_digesto_normas.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas.csv"),
    index=False,
    encoding="utf-8-sig"
)

print("digesto_normas.csv generado correctamente.")

# ==================================================
# 3. Crear tabla de relaciones (digesto_relaciones)
# ==================================================

print("Generando digesto_relaciones.csv...")

df_rel_modifica = pd.DataFrame({
    "id_origen": df_modifatorias["id_norma_modificatoria"],
    "id_destino": df_modifatorias["id_norma_modificada"],
    "tipo_relacion": "modifica"
})

df_rel_modificada_por = pd.DataFrame({
    "id_origen": df_modif["id_norma_modificada"],
    "id_destino": df_modif["id_norma_modificatoria"],
    "tipo_relacion": "es_modificada_por"
})

df_digesto_rel = pd.concat(
    [df_rel_modifica, df_rel_modificada_por],
    ignore_index=True
)

df_digesto_rel = reparar_mojibake_df(df_digesto_rel)

df_digesto_rel.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_relaciones.csv"),
    index=False,
    encoding="utf-8-sig"
)

print("digesto_relaciones.csv generado correctamente.")
print("Digesto listo.")




