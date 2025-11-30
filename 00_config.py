"""
00_config.py
Archivo de configuración del proyecto.

Notas:
      * la sesión de Spark,
      * el catálogo y esquema de Unity Catalog,
      * las rutas base de los datos crudos,
      * los nombres de las tablas en cada capa: Bronze, Silver y Gold,
      * la resolución H3 que voy a usar en Silver/Gold.
"""

import sys
import subprocess

# Librería h3 instalada en el entorno de ejecución.
subprocess.check_call([sys.executable, "-m", "pip", "install", "h3"])

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Sesión de Spark
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("vessel-traffic-depth-monitoring")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Catálogo y esquema en Unity Catalog
# ---------------------------------------------------------------------------

CATALOG = "labs_56754_cs713b"
SCHEMA = "vessel_traffic_monitoring"

# ---------------------------------------------------------------------------
# Rutas en Volumes para datos crudos
# ---------------------------------------------------------------------------

AIS_BASE_PATH = "/Volumes/proyecto/default/raw_data/marine-cadastre/"
NOAA_PATH     = "/Volumes/labs_56754_cs713b/vessel_traffic_monitoring/noaa_raw_data/"

# ---------------------------------------------------------------------------
# Tablas Bronze
# ---------------------------------------------------------------------------

BRONZE_AIS_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_ais"
BRONZE_NOAA_TABLE = f"{CATALOG}.{SCHEMA}.bronze_noaa"

# ---------------------------------------------------------------------------
# Tablas Silver
# ---------------------------------------------------------------------------

SILVER_AIS_TABLE      = f"{CATALOG}.{SCHEMA}.silver_ais"
SILVER_NOAA_TABLE     = f"{CATALOG}.{SCHEMA}.silver_noaa"
SILVER_ENRICHED_TABLE = f"{CATALOG}.{SCHEMA}.silver_enriched"

# ---------------------------------------------------------------------------
# Tablas Gold
# ---------------------------------------------------------------------------

GOLD_ANALYTICS_TABLE = f"{CATALOG}.{SCHEMA}.gold_analytics"

# ---------------------------------------------------------------------------
# Configuración H3
# ---------------------------------------------------------------------------

H3_RESOLUTION = 5
