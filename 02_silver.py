"""
02_silver.py
--------------
Script de capa SILVER incremental por process_date.

Idea general:
  - Bronze AIS ya tiene muchos días y una columna process_date.
  - Cada vez que se corra este script, se quiere procesar SOLO los días
    que todavía no estén en Silver AIS (para ahorrar cómputo).

Pasos:
  1. Cargar la configuración común (spark, rutas, nombres de tablas, H3_RESOLUTION).
  2. Leer tablas Bronze AIS y Bronze NOAA.
  3. Calcular qué días (process_date) existen en Bronze y cuáles ya
     están en Silver AIS. Quedarse solo con los días pendientes.
  4. Limpiar NOAA una sola vez y preparar una tabla agregada por H3 + ventana.
  5. Para cada día pendiente:
       - filtrar Bronze AIS a ese día,
       - limpiar AIS,
       - agregar H3,
       - hacer join con NOAA,
       - hacer APPEND en Silver AIS y Silver Enriched.
  6. Silver NOAA se escribe siempre en overwrite (dimensión casi estática).
"""

# ---------------------------------------------------------------------------
# 0. Cargar configuración común (Spark, rutas, tablas, H3_RESOLUTION)
# ---------------------------------------------------------------------------

exec(open("/Workspace/Users/odl_user_1905255@databrickslabs.com/vessel-traffic-depth-monitoring/00_config.py").read())

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException
from h3 import latlng_to_cell

# ---------------------------------------------------------------------------
# 1. Leer tablas Bronze
# ---------------------------------------------------------------------------

bronze_ais = spark.table(BRONZE_AIS_TABLE)
bronze_noaa = spark.table(BRONZE_NOAA_TABLE)

# ---------------------------------------------------------------------------
# 2. Calcular días a procesar (process_date) de forma incremental
# ---------------------------------------------------------------------------

# Días disponibles en Bronze (process_date distintos)
bronze_days_rows = (
    bronze_ais
    .select("process_date")
    .distinct()
    .collect()
)

bronze_days = [row["process_date"] for row in bronze_days_rows]
bronze_days_set = set(bronze_days)

# Días ya procesados en Silver (si la tabla existe)
try:
    silver_ais_existing = spark.read.table(SILVER_AIS_TABLE)

    silver_days_rows = (
        silver_ais_existing
        .select("process_date")
        .distinct()
        .collect()
    )

    silver_days = [row["process_date"] for row in silver_days_rows]
    silver_days_set = set(silver_days)
    silver_ais_exists = True

except AnalysisException:
    silver_days_set = set()
    silver_ais_exists = False

# Días pendientes: están en Bronze pero no en Silver
pending_days = sorted(list(bronze_days_set - silver_days_set))

# Si no hay días pendientes, no hay nada que hacer
if not pending_days:
    pending_days = []  # por si acaso, pero ya sé que no hay trabajo.


# ---------------------------------------------------------------------------
# 3. Limpieza y preparación de NOAA (una sola vez)
# ---------------------------------------------------------------------------

silver_noaa = (
    bronze_noaa
    .filter(
        (F.col("LAT") >= -90) & (F.col("LAT") <= 90) &
        (F.col("LON") >= -180) & (F.col("LON") <= 180) &
        (F.col("DEPTH") > 0)
    )
    .select(
        F.col("UNIQUE_ID").alias("unique_id"),
        F.col("FILE_UUID").alias("file_uuid"),
        F.col("LON").alias("lon"),
        F.col("LAT").alias("lat"),
        F.col("DEPTH").alias("depth"),
        F.col("TIME").alias("time"),
        F.col("PLATFORM_NAME").alias("platform_name"),
        F.col("PROVIDER").alias("provider")
    )
)

# UDF de H3 con la resolución definida en config
h3_udf = F.udf(lambda lat, lon: latlng_to_cell(lat, lon, H3_RESOLUTION), StringType())

# Agregar celda H3 a NOAA
silver_noaa_h3 = silver_noaa.withColumn(
    "h3_cell",
    h3_udf(F.col("lat"), F.col("lon"))
)

# NOAA agregado por celda H3 + ventana de 30 minutos
noaa_aggregated = (
    silver_noaa_h3
    .groupBy(
        F.col("h3_cell"),
        F.window(F.col("time"), "30 minutes").alias("time_window")
    )
    .agg(
        F.round(F.avg(F.col("depth")), 2).alias("avg_depth_m"),
        F.round(F.min(F.col("depth")), 2).alias("min_depth_m"),
        F.round(F.max(F.col("depth")), 2).alias("max_depth_m"),
        F.count(F.col("depth")).alias("depth_samples")
    )
    .select(
        F.col("h3_cell"),
        F.col("time_window.start").alias("time_start"),
        F.col("time_window.end").alias("time_end"),
        "avg_depth_m",
        "min_depth_m",
        "max_depth_m",
        "depth_samples"
    )
)

# Silver NOAA se recalcula completo cada vez
silver_noaa_h3.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(SILVER_NOAA_TABLE)

# ---------------------------------------------------------------------------
# 4. Procesar AIS solo para los días pendientes
# ---------------------------------------------------------------------------

# Revisar si Silver Enriched ya existe para decidir overwrite/append
try:
    spark.read.table(SILVER_ENRICHED_TABLE)
    silver_enriched_exists = True
except AnalysisException:
    silver_enriched_exists = False

for idx, day in enumerate(pending_days, start=1):
    # Filtrar Bronze AIS al día que se quiere procesar
    ais_day = bronze_ais.filter(F.col("process_date") == day)

    # Limpieza AIS para ese día
    silver_ais_day = (
        ais_day
        .filter(
            (F.col("mmsi").isNotNull()) &
            (F.col("latitude").isNotNull()) &
            (F.col("longitude").isNotNull()) &
            (F.col("base_date_time").isNotNull()) &
            (F.col("latitude") >= -90) & (F.col("latitude") <= 90) &
            (F.col("longitude") >= -180) & (F.col("longitude") <= 180)
        )
        .withColumn(
            "vessel_type",
            F.when(F.col("vessel_type").isNull(), F.lit("UNKNOWN")).otherwise(F.col("vessel_type"))
        )
        .withColumn(
            "cargo",
            F.when(F.col("cargo").isNull(), F.lit("UNKNOWN")).otherwise(F.col("cargo"))
        )
        # process_date ya viene de Bronze, no tocar
    )

    # Agregar celda H3 a AIS del día
    silver_ais_day_h3 = silver_ais_day.withColumn(
        "h3_cell",
        h3_udf(F.col("latitude"), F.col("longitude"))
    )

    # Ventana de 30 minutos para AIS
    ais_with_window = silver_ais_day_h3.withColumn(
        "time_window",
        F.window(F.col("base_date_time"), "30 minutes")
    )

    # Join AIS + NOAA
    silver_enriched_day = (
        ais_with_window.join(
            noaa_aggregated,
            (ais_with_window.h3_cell == noaa_aggregated.h3_cell) &
            (ais_with_window.time_window.start <= noaa_aggregated.time_end) &
            (ais_with_window.time_window.end >= noaa_aggregated.time_start),
            "left"
        )
        .select(
            ais_with_window.mmsi,
            ais_with_window.vessel_name,
            ais_with_window.vessel_type,
            ais_with_window.cargo,
            ais_with_window.base_date_time,
            ais_with_window.latitude,
            ais_with_window.longitude,
            ais_with_window.process_date,
            ais_with_window.h3_cell,
            noaa_aggregated.avg_depth_m.alias("nearby_depth_m"),
            noaa_aggregated.depth_samples
        )
    )

    # Escribir Silver AIS:
    #   - si la tabla no existe y es el primer día -> overwrite
    #   - si no -> append
    if not silver_ais_exists and idx == 1:
        ais_write_mode = "overwrite"
    else:
        ais_write_mode = "append"

    silver_ais_day.write \
        .format("delta") \
        .mode(ais_write_mode) \
        .saveAsTable(SILVER_AIS_TABLE)

    silver_ais_exists = True

    # Escribir Silver Enriched, mismo criterio
    if not silver_enriched_exists and idx == 1:
        enriched_write_mode = "overwrite"
    else:
        enriched_write_mode = "append"

    silver_enriched_day.write \
        .format("delta") \
        .mode(enriched_write_mode) \
        .saveAsTable(SILVER_ENRICHED_TABLE)

    silver_enriched_exists = True
