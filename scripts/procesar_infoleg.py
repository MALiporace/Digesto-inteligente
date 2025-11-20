import os
import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# === Función para normalizar fechas ===
def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")

print("🔍 Procesando Infoleg → Digesto…\n")

# === 1. Cargar datasets crudos ===
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

# ================================
# === 2. Maestro de Normas    ===
# ================================

print("📚 Generando digesto_normas.csv…")

df_digesto_normas = pd.DataFrame({
    "id_norma": df_norm["id_norma"],
    "tipo_norma": df_norm["tipo_norma"],
    "numero_norma": df_norm["numero_norma"],
    "fecha_sancion": normalizar_fecha(df_norm["Fecha_sancion"]),
    "organismo": df_norm["organismo_origen"],
    "titulo_resumido": df_norm["titulo_resumido"],
    "titulo_sumario": df_norm["titulo_sumario"],
    "fecha_publicacion": normalizar_fecha(df_norm["fecha_boletin"]),
    "estado": "",  # Infoleg no lo provee
    "fuente": "Infoleg",
    "url_texto_original": df_norm["texto_original"],
    "url_texto_actualizado": df_norm["texto_actualizado"],
})

df_digesto_normas.to_csv(
    os.path.join(BASE_DIR, "digesto_normas.csv"),
    index=False,
    encoding="utf-8"
)

print("✅ digesto_normas.csv generado correctamente.\n")

# ===================================
# === 3. Relaciones normativas    ===
# ===================================

print("🔗 Generando digesto_relaciones.csv…")

# A. Normas que X modifica (infoleg_modificatorias)
df_rel_modifica = pd.DataFrame({
    "id_origen": df_modifatorias["id_norma_modificatoria"],   # X
    "id_destino": df_modifatorias["id_norma_modificada"],     # Y
    "tipo_relacion": "modifica"
})

# B. Normas que modifican a X (infoleg_modificadas)
df_rel_modificada_por = pd.DataFrame({
    "id_origen": df_modif["id_norma_modificatoria"],          # Z
    "id_destino": df_modif["id_norma_modificada"],            # X
    "tipo_relacion": "es_modificada_por"
})

# Unir ambas
df_digesto_rel = pd.concat(
    [df_rel_modifica, df_rel_modificada_por],
    ignore_index=True
)

df_digesto_rel.to_csv(
    os.path.join(BASE_DIR, "digesto_relaciones.csv"),
    index=False,
    encoding="utf-8"
)

print("✅ digesto_relaciones.csv generado correctamente.\n")

print("🎉 Digesto procesado con éxito.")
