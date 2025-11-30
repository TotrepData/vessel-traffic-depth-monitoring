"""
01_bronze.py
--------------
Script para cargar los datos crudos a la capa BRONZE.

- AIS: muchos archivos en el volumen, un archivo por día (ais-YYYY-MM-DD.csv.zst).
- NOAA: carpeta con archivos de profundidad.

Idea básica:
  1. Cargar TODOS los datos de NOAA y guardar en una tabla Bronze (overwrite).
  2. Buscar todos los archivos AIS en la carpeta.
  3. Ordenarlos por nombre (que equivale a ordenarlos por fecha).
  4. Dependiendo del modo:
       - MODO_DEMO = True  -> solo procesar los primeros N archivos.
       - MODO_DEMO = False -> procesar TODOS los archivos disponibles.
  5. Cada archivo AIS se escribe en la tabla Bronze AIS acumulando datos.

Nota:
  - Si se quiere cambiar cuántos archivos AIS procesar en una corrida,
    solo tocar las variables MODO_DEMO y MAX_AIS_FILES_PER_RUN de abajo.
"""

# Cargar la configuración común del proyecto (spark, rutas, nombres de tablas, etc.)
exec(open("/Workspace/Users/odl_user_1905255@databrickslabs.com/vessel-traffic-depth-monitoring/00_config.py").read())

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------------
# Parámetros según el escenario
# ---------------------------------------------------------------------------

# Si MODO_DEMO = True, solo proceso unos pocos archivos AIS (para pruebas).
# Si MODO_DEMO = False, proceso TODOS los archivos AIS que encuentre.
MODO_DEMO = True

# En modo demo, este es el número máximo de archivos AIS
# que se quiere procesar en una sola corrida del workflow.
MAX_AIS_FILES_PER_RUN = 2


# ---------------------------------------------------------------------------
# 1. Cargar NOAA completo a Bronze (overwrite)
# ---------------------------------------------------------------------------

# Nota: por ahora se asume que NOAA cabe bien en memoria y que no es gigante.
# Si en algún momento NOAA crece mucho, habría que repensar esta parte.

noaa_df = (
    spark.read
    .csv(
        NOAA_PATH,   # esta ruta viene de 00_config.py
        header=True,
        inferSchema=True
    )
)

noaa_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(BRONZE_NOAA_TABLE)


# ---------------------------------------------------------------------------
# 2. Listar archivos AIS del volumen
# ---------------------------------------------------------------------------

# Revisar qué archivos existen en la carpeta AIS_BASE_PATH.
# Cada entrada tiene:
#   - .path -> ruta completa
#   - .name -> nombre del archivo (ej: 'ais-2025-01-01.csv.zst')

files = dbutils.fs.ls(AIS_BASE_PATH)

# Quedan solo con los que siguen el patrón esperado.
ais_files = [
    f for f in files
    if f.name.startswith("ais-") and f.name.endswith(".csv.zst")
]

# Se ordenan por nombre; como la fecha está en el nombre, quedan ordenados por fecha.
ais_files_sorted = sorted(ais_files, key=lambda f: f.name)

# Según el modo, decidir cuántos archivos procesar:
if MODO_DEMO:
    # Modo demo: solo los primeros N archivos.
    ais_files_to_process = ais_files_sorted[:MAX_AIS_FILES_PER_RUN]
else:
    # Modo “real”: proceso todos los archivos que haya.
    ais_files_to_process = ais_files_sorted


# ---------------------------------------------------------------------------
# 3. Escribir AIS en Bronze de forma acumulativa
# ---------------------------------------------------------------------------

# Primero verificar si la tabla Bronze AIS ya existe.
try:
    spark.read.table(BRONZE_AIS_TABLE)
    ais_table_exists = True
except AnalysisException:
    ais_table_exists = False

for idx, f in enumerate(ais_files_to_process, start=1):
    # Ejemplo de nombre: 'ais-2025-01-01.csv.zst'
    file_name = f.name
    # Aquí sacar la fecha del nombre: quedarse con la parte YYYY-MM-DD
    date_part = file_name.replace("ais-", "").replace(".csv.zst", "")

    ais_path = f.path  # ruta completa del archivo en el volumen

    ais_df = (
        spark.read
        .csv(
            ais_path,
            header=True,
            inferSchema=True
        )
        # Guardar la fecha del archivo en una columna, para saber de qué día es cada registro.
        .withColumn("process_date", F.lit(date_part))
    )

    # Si la tabla aún no existe y es el primer archivo -> overwrite (se crea).
    # En cualquier otro caso -> append (ir acumulando días).
    if not ais_table_exists and idx == 1:
        write_mode = "overwrite"
    else:
        write_mode = "append"

    ais_df.write \
        .format("delta") \
        .mode(write_mode) \
        .saveAsTable(BRONZE_AIS_TABLE)

    # A partir del primer archivo se puede considerar que la tabla existe.
    ais_table_exists = True
