
# -*- coding: utf-8 -*-

import os
import re
import unicodedata
import pandas as pd

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
# Helpers
# ==================================================

def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")


def reparar_mojibake_texto(x: str) -> str:
    """
    Intenta reparar mojibake tipico:
    'ResoluciÃ³n' -> 'Resolución'
    'PequeÃ±a'   -> 'Pequeña'
    'nÂº'        -> 'nº'
    """
    if not isinstance(x, str):
        return x
    # si no hay rastro de mojibake, devolvemos tal cual
    if "Ã" not in x and "Â" not in x and "Ð" not in x:
        return x
    try:
        return x.encode("latin1").decode("utf-8")
    except Exception:
        return x


def reparar_mojibake_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la reparación de mojibake a TODAS las celdas string del DF.
    """
    return df.applymap(reparar_mojibake_texto)


def a_ascii_basico_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todo texto del DF a ASCII "seguro":
    - Quita tildes (ó -> o, ñ -> n, etc.)
    - Reemplaza º y ª por 'o'/'a' razonables
    - Elimina cualquier cosa fuera del rango imprimible simple

    Resultado: sin caracteres "especiales" para lo que sea que venga después.
    """
    def to_ascii(x):
        if not isinstance(x, str):
            return x

        # normalizamos y removemos diacríticos
        x_norm = unicodedata.normalize("NFKD", x)
        x_sin_acentos = "".join(
            c for c in x_norm
            if not unicodedata.combining(c)
        )

        # reemplazos puntuales
        x_sin_acentos = (
            x_sin_acentos
            .replace("º", "o")
            .replace("ª", "a")
            .replace("·", ".")
        )

        # quedarnos solo con ASCII imprimible "tranquilo"
        x_sin_acentos = re.sub(r"[^\x20-\x7E]", "", x_sin_acentos)

        return x_sin_acentos

    return df.applymap(to_ascii)


def leer_csv_reforzado(path: str) -> pd.DataFrame:
    """
    Lector robusto:
    - prueba utf-8
    - si explota, prueba utf-8-sig
    - luego repara mojibake a nivel DF
    """
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except Exception:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    df = reparar_mojibake_df(df)
    return df


print("Procesando Infoleg...")

# ==================================================
# 1. Cargar datasets crudos con lector reforzado
# ==================================================

df_norm = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_normativa.csv"))
df_modif = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificadas.csv"))
df_modifatorias = leer_csv_reforzado(os.path.join(BASE_DIR, "infoleg_modificatorias.csv"))

# ==================================================
# 2. Crear maestro de normas
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

# 2.b) Segunda pasada de reparación (por si se coló algo en derivadas)
df_digesto_normas = reparar_mojibake_df(df_digesto_normas)

# 2.c) Versión SIN caracteres especiales (ASCII "amigable")
df_digesto_normas_ascii = a_ascii_basico_df(df_digesto_normas)

# Guarda el archivo PRINCIPAL ya normalizado a ASCII
# UTF-8 con BOM para que Excel no rompa más las tildes
df_digesto_normas_ascii.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas.csv"),
    index=False,
    encoding="utf-8-sig"
)

# (Opcional) si querés conservar también la versión con acentos bien puestos:
df_digesto_normas.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas_utf8_completo.csv"),
    index=False,
    encoding="utf-8-sig"
)

print("digesto_normas.csv (ASCII) generado correctamente.")
print("digesto_normas_utf8_completo.csv generado correctamente.")

# ==================================================
# 3. Crear tabla de relaciones
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

# Mojibake fuera y ASCII también acá por las dudas
df_digesto_rel = reparar_mojibake_df(df_digesto_rel)
df_digesto_rel_ascii = a_ascii_basico_df(df_digesto_rel)

df_digesto_rel_ascii.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_relaciones.csv"),
    index=False,
    encoding="utf-8-sig"
)

print("digesto_relaciones.csv generado correctamente.")
print("Digesto listo.")




