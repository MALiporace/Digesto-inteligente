import requests
import json

# ============================================================
# 1. CONFIGURAR TUS DATOS
# ============================================================
APP_KEY = "j4guek08ued14ve"
APP_SECRET = "vhm6kb1km427w7u"
REFRESH_TOKEN = "cDn3Zy6-UQEAAAAAAAAAAbTU3WKn71f5cPSOxaUK3fEzl51qkrB2f9iEEGz5NdcM"


# ============================================================
# 2. OBTENER ACCESS TOKEN USANDO EL REFRESH TOKEN
# ============================================================
def obtener_access_token():
    url = "https://api.dropbox.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": APP_KEY,
        "client_secret": APP_SECRET
    }
    print("🔑 Generando access token…")
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json()["access_token"]
    print("✔ Access token obtenido correctamente.\n")
    return token


# ============================================================
# 3. LISTAR ARCHIVOS DE UNA CARPETA
# ============================================================
def listar(token, path):
    print(f"📂 Listando carpeta: '{path or '(root)'}'")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {"path": path}
    r = requests.post(
        "https://api.dropboxapi.com/2/files/list_folder",
        headers=headers,
        json=data
    )
    r.raise_for_status()
    contenido = r.json()

    print(json.dumps(contenido, indent=2, ensure_ascii=False))
    print("\n")
    return contenido


# ============================================================
# 4. EJECUCIÓN PRINCIPAL
# ============================================================
if __name__ == "__main__":
    token = obtener_access_token()

    print("=====================================")
    print(" 🔍 DIAGNÓSTICO DEL APP FOLDER REAL ")
    print("=====================================\n")

    # Carpeta raíz del App Folder real
    root = listar(token, "")

    # Carpeta /data
    data = listar(token, "/data")

    # Carpeta /data_procesada
    data_proc = listar(token, "/data_procesada")

    print("=====================================")
    print("     🔎 ANALIZAR RESULTADOS MANUALMENTE")
    print("=====================================")
    print("""
→ Si ves archivos con fechas NUEVAS en estas respuestas, pero NO en la UI:
     ✔ Estás viendo una carpeta fantasma en Dropbox Web/Desktop.

→ Si ves una estructura distinta en la API que en la web:
     ✔ Dropbox UI está mostrando otro App Folder.

→ Si el nombre del folder en la API no coincide con el de la UI:
     ✔ Tenés dos App Folders distintos (uno real y uno zombie).

Mandame la salida y lo analizo con vos.
""")
