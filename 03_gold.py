"""
03_gold.py
--------------
Script de capa GOLD incremental por process_date.

Idea general:
  - Se parte de la tabla Silver Enriched (SILVER_ENRICHED_TABLE), que tiene:
      * base_date_time
      * process_date (string 'YYYY-MM-DD')
      * h3_cell
      * vessel_type
      * cargo
      * mmsi
      * nearby_depth_m
      * depth_samples
  - Construir la tabla Gold (GOLD_ANALYTICS_TABLE) con este esquema:

      event_date    (DATE)  -> día del evento (a partir de base_date_time)
      h3_cell
      vessel_type
      cargo
      vessel_count          -> número de mmsi distintos
      latitude              -> latitud representativa de la celda
      longitude             -> longitud representativa de la celda
      avg_depth_m
      min_depth_m
      max_depth_m
      avg_samples

Comportamiento incremental:
  - Ver qué días (process_date) existen en Silver Enriched.
  - Ver qué días ya están en Gold (usando event_date).
  - Solo procesar los días que están en Silver y no en Gold,
    y hacer un APPEND de esos días a la tabla Gold.
"""

# ---------------------------------------------------------------------------
# 0. Cargar configuración común (Spark, tablas, etc.)
# ---------------------------------------------------------------------------

exec(open("/Workspace/Users/odl_user_1905255@databrickslabs.com/vessel-traffic-depth-monitoring/00_config.py").read())

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------------
# 1. Leer tabla Silver Enriched
# ---------------------------------------------------------------------------

silver_enriched_all = spark.table(SILVER_ENRICHED_TABLE)

# ---------------------------------------------------------------------------
# 2. Calcular qué días tengo en Silver y en Gold
# ---------------------------------------------------------------------------

# Días disponibles en Silver (usar process_date, que viene de Bronze/Silver)
silver_days_rows = (
    silver_enriched_all
    .select("process_date")
    .distinct()
    .collect()
)
silver_days = [row["process_date"] for row in silver_days_rows]
silver_days_set = set(silver_days)

# Días que ya existen en Gold (si la tabla existe)
try:
    gold_existing = spark.read.table(GOLD_ANALYTICS_TABLE)

    # event_date es de tipo DATE; pasar a string 'YYYY-MM-DD' para comparar
    gold_days_rows = (
        gold_existing
        .select(F.date_format("event_date", "yyyy-MM-dd").alias("event_date_str"))
        .distinct()
        .collect()
    )

    gold_days = [row["event_date_str"] for row in gold_days_rows]
    gold_days_set = set(gold_days)
    gold_exists = True

except AnalysisException:
    gold_days_set = set()
    gold_exists = False

# Días pendientes: están en Silver pero no en Gold
pending_days = sorted(list(silver_days_set - gold_days_set))

if not pending_days:
    # Si no hay días pendientes, no hay nada nuevo que agregar a Gold.
    pending_days = []

# ---------------------------------------------------------------------------
# 3. Procesar solo los días pendientes y hacer agregación GOLD
# ---------------------------------------------------------------------------

for idx, day in enumerate(pending_days, start=1):
    # Filtro Silver Enriched al día que se quiere procesar
    # y creo la columna event_date a partir de base_date_time
    silver_enriched = (
        silver_enriched_all
        .withColumn("event_date", F.to_date(F.col("base_date_time")))
        .filter(F.col("process_date") == day)
    )

    # Agregación
    gold_analytics = (
        silver_enriched.groupBy(
            F.col("event_date"),
            F.col("h3_cell"),
            F.col("vessel_type"),
            F.col("cargo")
        )
        .agg(
            F.countDistinct(F.col("mmsi")).alias("vessel_count"),
            F.first(F.col("latitude")).alias("latitude"),
            F.first(F.col("longitude")).alias("longitude"),
            F.round(F.avg(F.col("nearby_depth_m")), 2).alias("avg_depth_m"),
            F.round(F.min(F.col("nearby_depth_m")), 2).alias("min_depth_m"),
            F.round(F.max(F.col("nearby_depth_m")), 2).alias("max_depth_m"),
            F.round(F.avg(F.col("depth_samples")), 0).alias("avg_samples")
        )
        .filter(F.col("h3_cell").isNotNull())
        .filter(F.col("vessel_count") > 0)
    )

    # Modo de escritura:
    #   - si la tabla no existe y es el primer día -> overwrite
    #   - en cualquier otro caso -> append
    if (not gold_exists) and (idx == 1):
        gold_write_mode = "overwrite"
    else:
        gold_write_mode = "append"

    gold_analytics.write \
        .format("delta") \
        .mode(gold_write_mode) \
        .saveAsTable(GOLD_ANALYTICS_TABLE)

    gold_exists = True  # a partir del primer día ya existe seguro

# ---------------------------------------------------------------------------
# 4. OPTIMIZE + VACUUM (igual que antes, opcional)
# ---------------------------------------------------------------------------

if gold_exists:
    try:
        spark.sql(f"""
            OPTIMIZE {GOLD_ANALYTICS_TABLE}
            ZORDER BY (event_date, h3_cell, latitude, longitude)
        """)
    except Exception as e:
        print(f"⚠ Advertencia durante OPTIMIZE: {e}")

    try:
        spark.sql(f"VACUUM {GOLD_ANALYTICS_TABLE} RETAIN 30 DAYS")
    except Exception as e:
        print(f"⚠ Advertencia durante VACUUM: {e}")

# ---------------------------------------------------------------------------
# 5. Resumen
# ---------------------------------------------------------------------------

if gold_exists:
    gold_df = spark.table(GOLD_ANALYTICS_TABLE)
    total_rows = gold_df.count()
    total_days = gold_df.select("event_date").distinct().count()
    total_h3 = gold_df.select("h3_cell").distinct().count()

    print("Resumen GOLD:")
    print(f"  Registros totales       : {total_rows}")
    print(f"  Días (event_date) únicos: {total_days}")
    print(f"  Celdas H3 únicas        : {total_h3}")
    print(f"  Días procesados en esta corrida: {pending_days}")
else:
    print("Gold no tiene datos todavía (no había días pendientes que procesar).")
