# -*- coding: utf-8 -*-

import os
import pandas as pd

# ==================================================
# Rutas fijas (GitHub Actions)
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
    if not isinstance(x, str):
        return x
    if "Ã" not in x and "Â" not in x and "Ð" not in x:
        return x
    try:
        return x.encode("latin1").decode("utf-8")
    except Exception:
        return x


def reparar_mojibake_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(reparar_mojibake_texto)


def leer_csv_reforzado(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except Exception:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df = reparar_mojibake_df(df)
    return df

# ==================================================
# URL dinámica SIEMPRE disponible
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

df_digesto_normas = reparar_mojibake_df(df_digesto_normas)

# --------------------------------------------------
# Normalización AGRESIVA de valores vacíos
# --------------------------------------------------

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

# --------------------------------------------------
# Campo alternativo SIEMPRE presente
# --------------------------------------------------

df_digesto_normas["texto_original_alternativo"] = (
    df_digesto_normas["id_norma"].apply(reconstruir_url_infoleg)
)

# --------------------------------------------------
# NO completar url_texto_original con alternativa
# (queda NA si no hay texto real)
# --------------------------------------------------

# --------------------------------------------------
# Campo resumen_infoleg vacío (bajo demanda)
# --------------------------------------------------

df_digesto_normas["resumen_infoleg"] = pd.NA

# --------------------------------------------------
# Guardar archivo maestro
# --------------------------------------------------

df_digesto_normas.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas.csv"),
    index=False,
    encoding="utf-8-sig"
)

print("digesto_normas.csv generado correctamente.")

# ==================================================
# Relaciones
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



