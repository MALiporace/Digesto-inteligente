# -*- coding: utf-8 -*-

import os
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

# Crear carpeta si no existe (GitHub la crea sobre la marcha)
os.makedirs(BASE_PROCESADA, exist_ok=True)


# ==================================================
# Función para normalizar fechas
# ==================================================

def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")


print("Procesando Infoleg...")

# ==================================================
# 1. Cargar datasets crudos
# ==================================================

df_norm = pd.read_csv(
    os.path.join(BASE_DIR, "infoleg_normativa.csv"),
    low_memory=False,
    encoding="latin1"
)

df_modif = pd.read_csv(
    os.path.join(BASE_DIR, "infoleg_modificadas.csv"),
    low_memory=False,
    encoding="latin1"
)

df_modifatorias = pd.read_csv(
    os.path.join(BASE_DIR, "infoleg_modificatorias.csv"),
    low_memory=False,
    encoding="latin1"
)

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

df_digesto_normas.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_normas.csv"),
    index=False,
    encoding="utf-8"
)

print("digesto_normas.csv generado correctamente.")

# ==================================================
# 3. Crear tabla de relaciones
# ==================================================

print("Generando digesto_relaciones.csv...")

# Caso A: X modifica a Y (infoleg_modificatorias)
df_rel_modifica = pd.DataFrame({
    "id_origen": df_modifatorias["id_norma_modificatoria"],
    "id_destino": df_modifatorias["id_norma_modificada"],
    "tipo_relacion": "modifica"
})

# Caso B: X es modificada por Y (infoleg_modificadas)
df_rel_modificada_por = pd.DataFrame({
    "id_origen": df_modif["id_norma_modificada"],        # X
    "id_destino": df_modif["id_norma_modificatoria"],    # Y
    "tipo_relacion": "es_modificada_por"
})

df_digesto_rel = pd.concat(
    [df_rel_modifica, df_rel_modificada_por],
    ignore_index=True
)

df_digesto_rel.to_csv(
    os.path.join(BASE_PROCESADA, "digesto_relaciones.csv"),
    index=False,
    encoding="utf-8"
)

print("digesto_relaciones.csv generado correctamente.")

print("Digesto listo.")
