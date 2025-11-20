import os
import pandas as pd

# ===============================================
# 1. Selección automática de la carpeta "data"
# ===============================================

# A. Ruta de Dropbox local
DROPBOX_DATA = os.path.join(
    os.path.expanduser("~"),
    "Dropbox",
    "Aplicaciones",
    "Digesto_Inteligente",
    "data"
)

# B. Ruta del repo local o GitHub Actions
GITHUB_DATA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "data"
)

# Elegimos automáticamente la ruta que existe
if os.path.exists(DROPBOX_DATA):
    BASE_DIR = DROPBOX_DATA
    print(f"📂 Usando carpeta de Dropbox:\n{BASE_DIR}\n")
else:
    BASE_DIR = os.path.abspath(GITHUB_DATA)
    print(f"📂 Usando carpeta del repositorio:\n{BASE_DIR}\n")

# ===============================================
# Función para normalizar fechas
# ===============================================

def normalizar_fecha(serie):
    return pd.to_datetime(serie, errors="coerce").dt.strftime("%Y-%m-%d")

print("🔍 Procesando Infoleg → Digesto…\n")

# ===============================================
# 2. Cargar datasets crudos
# ===============================================

df_norm = pd.read_csv(os.path.join(BASE_DIR, "infoleg_normativa.csv"), 
                      low_memory=False, encoding="latin1")

df_modif = pd.read_csv(os.path.join(BASE_DIR, "infoleg_modificadas.csv"), 
                       low_memory=False, encoding="latin1")

df_modifatorias = pd.read_csv(os.path.join(BASE_DIR, "infoleg_modificatorias.csv"), 
                              low_memory=False, encoding="latin1")

# ===============================================
# 3. Maestro de Normas
# ===============================================

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
    "estado": "",
    "fuente": "Infoleg",
    "url_texto_original": df_norm["texto_original"],
    "url_texto_actualizado": df_norm["texto_actualizado"],
})

df_digesto_normas.to_csv(os.path.join(BASE_DIR, "digesto_normas.csv"),
                         index=False, encoding="utf-8")

print("✅ digesto_normas.csv generado.\n")

# ===============================================
# 4. Relaciones
# ===============================================

print("🔗 Generando digesto_relaciones.csv…")

df_rel_modifica = pd.DataFrame({
    "id_origen": df_modifatorias["id_norma_modificatoria"],
    "id_destino": df_modifatorias["id_norma_modificada"],
    "tipo_relacion": "modifica"
})

df_rel_modificada_por = pd.DataFrame({
    "id_origen": df_modif["id_norma_modificatoria"],
    "id_destino": df_modif["id_norma_modificada"],
    "tipo_relacion": "es_modificada_por"
})

df_digesto_rel = pd.concat([df_rel_modifica, df_rel_modificada_por], ignore_index=True)

df_digesto_rel.to_csv(os.path.join(BASE_DIR, "digesto_relaciones.csv"),
                      index=False, encoding="utf-8")

print("✅ digesto_relaciones.csv generado.\n")

print("🎉 Digesto procesado con éxito.")
