# -*- coding: utf-8 -*-

import os
import csv
import pandas as pd

# ==================================================
# Paths
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
# Helpers
# ==================================================

def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")


def reparar_mojibake_texto(x):
    if not isinstance(x, str):
        return x
    if "Ã" not in x and "Â" not in x and "Ð" not in x:
        return x
    try:
        return x.encode("latin1").decode("utf-8")
    except:
        return x


def reparar_mojibake_df(df):
    return df.applymap(reparar_mojibake_texto)


def leer_csv_reforzado(path):
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    return reparar_mojibake_df(df)

# ==================================================
# URL dinámica
# ==================================================

def reconstruir_url_infoleg(id_norma):
    return f"https://servicios.infoleg.gob.ar/infolegInternet/verNorma.do?id={id_norma}"

# ==================================================
# Procesamiento
# ==================================================

print("Procesando Infoleg...")

df_norm = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_normativa.csv"))
df_modif = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificadas.csv"))
df_modifatorias = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificatorias.csv"))

# ==================================================
# Crear digesto_normas
# ==================================================

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

df_digesto_normas = reparar_mojibake_df(df_digesto_normas)

# ==================================================
# NORMALIZACIÓN DE id_norma (CLAVE)
# ==================================================
# Elimina ".0", convierte todo a string, quita espacios, elimina NaN

df_digesto_normas["id_norma"] = (
    df_digesto_normas["id_norma"]
    .astype(str)                     # convierte 594.0 → "594.0"
    .str.replace(r"\.0$", "", regex=True)   # elimina ".0"
    .str.strip()                     # quita espacios invisibles
    .replace({"nan": pd.NA})         # convierte "nan" real a NA
)

# ==================================================
# Normalizar url_texto_original
# ==================================================

def limpiar_url(x):
    if not isinstance(x, str):
        return pd.NA
    xs = x.strip()
    if xs in ("", "nan", "None", "0"):
        return pd.NA
    return xs

df_digesto_normas["url_texto_original"] = (
    df_digesto_normas["url_texto_original"]
    .astype("string")
    .apply(limpiar_url)
)

# ==================================================
# Construir SIEMPRE la alternativa
# ==================================================

df_digesto_normas["texto_original_alternativo"] = (
    df_digesto_normas["id_norma"].apply(lambda x: reconstruir_url_infoleg(x) if pd.notna(x) else pd.NA)
)

# NO rellenamos url_texto_original
df_digesto_normas["resumen_infoleg"] = pd.NA

# ==================================================
# Guardar CSV con QUOTE_ALL para que Excel NO corte URLs
# ==================================================

df_digesto_normas.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas.csv"),
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL,
    escapechar="\\"
)

print("digesto_normas.csv generado correctamente.")

# ==================================================
# Crear digesto_relaciones
# ==================================================

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

df_digesto_rel = pd.concat([df_rel_modifica, df_rel_modificada_por], ignore_index=True)
df_digesto_rel = reparar_mojibake_df(df_digesto_rel)

df_digesto_rel.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_relaciones.csv"),
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL
)

print("digesto_relaciones.csv generado correctamente.")
print("Digesto listo.")
