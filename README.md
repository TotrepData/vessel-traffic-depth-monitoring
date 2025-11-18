# Vessel Traffic Depth Monitoring

Pipeline de Big Data para monitorear profundidades oceanográficas e identificar zonas de riesgo marítimo.

## Descripción del Proyecto

Este proyecto implementa una arquitectura Medallion completa que integra datos de tráfico marítimo (AIS de Marine Cadastre) con mediciones oceanográficas (NOAA Bathymetry). El objetivo es monitorear profundidades en rutas de navegación, identificar zonas de riesgo, visualizar patrones de tráfico marítimo global y realizar análisis geoespacial usando indexación H3 hexagonal.

El resultado es un dashboard interactivo que mapea 9,805 zonas oceanográficas con métricas de profundidad y tráfico marítimo.

## Arquitectura de Datos

El pipeline sigue tres capas (Bronze → Silver → Gold):

Bronze: Datos crudos sin transformación
- bronze_ais: 7,337,208 registros de Marine Cadastre
- bronze_noaa: 1,133,563 mediciones de NOAA

Silver: Datos limpios y enriquecidos
- silver_ais: AIS validado sin nulos críticos
- silver_noaa: Profundidades validadas
- silver_enriched: AIS + NOAA cruzados con índices H3

Gold: Datos agregados y optimizados
- gold_analytics: 9,805 zonas H3 con métricas de profundidad y tráfico

## Tecnologías

Databricks Workspace (plataforma unificada), Apache Spark (ETL distribuido), Delta Lake (almacenamiento con ACID y versionado), H3 Uber (indexación geográfica hexagonal), Databricks Dashboard (visualización), CloudLabs AWS (infraestructura).

## Datasets

### Marine Cadastre AIS

Proveedor: NOAA Marine Cadastre
Período: 2025-01-01
Registros: 7,337,208 posiciones de barcos
Campos: mmsi (ID único), latitude/longitude (posición), vessel_type (tipo de embarcación), cargo (tipo de carga), base_date_time (timestamp)

### NOAA Crowdsourced Bathymetry

Proveedor: NOAA Crowdsourced Bathymetry Program
Período: 2025-01-01 (252 archivos)
Registros: 1,133,563 mediciones de profundidad
Tamaño: 381 MB
Campos: LAT/LON (coordenadas), DEPTH (profundidad en metros), TIME (timestamp), PLATFORM_NAME (dispositivo de medición)

## Pipeline ETL

### Fase 1: Ingesta de Datos

Descargar NOAA desde S3 público:

```bash
brew install awscli
aws s3 cp s3://noaa-dcdb-bathymetry-pds/csb/csv/2025/01/01/ \
  ~/noaa_data/ --recursive --no-sign-request
```

Crear infraestructura en Databricks:

```sql
CREATE SCHEMA IF NOT EXISTS labs_56754_cs713b.vessel_traffic_monitoring;
CREATE VOLUME IF NOT EXISTS 
  labs_56754_cs713b.vessel_traffic_monitoring.noaa_raw_data;
```

### Fase 2: Bronze (Datos Crudos)

Almacenar datos originales sin transformación para auditoría, reproducibilidad y reversibilidad en caso de errores.

### Fase 3: Silver (Datos Limpios)

Validación AIS: Filtrar registros sin mmsi/latitude/longitude, validar rangos (lat [-90,90], lon [-180,180]), rellenar nulos con "UNKNOWN".

Validación NOAA: Validar profundidad > 0, validar rangos de coordenadas, sin nulos críticos.

Enriquecimiento H3: Convertir lat/lon a celdas hexagonales H3 nivel 5 (~1200 km² por celda), permite joins eficientes.

Cruce AIS-NOAA: JOIN por H3 cell + ventana temporal (30 min), LEFT JOIN mantiene todos los barcos, agrega profundidad promedio cercana.

### Fase 4: Gold (Datos Agregados)

Agregación por h3_cell, vessel_type y cargo. Métricas: vessel_count (total barcos), avg_depth_m (profundidad promedio), min_depth_m, max_depth_m. Resultado: 9,805 zonas H3 listas para análisis.

## Dashboard

### KPIs Principales

Total de Barcos: 8,292,498
Profundidad Promedio: 5.93 metros
Zonas Navegadas: 9,805

### Visualizaciones

Mapa Geográfico: Puntos azules representan zonas con mediciones de profundidad. Azul oscuro = aguas profundas (>30m), Azul claro = aguas someras (<10m). Incluye tooltips con estadísticas.

Top 15 Tipos de Embarcación: Tipo 37 domina con 53.2%, seguido de tipo 31 (40.6%) y tipo 52 (38.6%).

Top 15 Tipos de Carga: UNKNOWN predomina. Muestra distribución y correlación carga-profundidad.

Zonas de Riesgo: Identifica aguas someras con tráfico alto. Clasificación CRÍTICO (profundidad < 5m Y tráfico > 100), ALTO, MEDIO, BAJO.

Filtros: Tipo de embarcación, tipo de carga, rango de profundidad, nivel de riesgo.

## Optimizaciones Implementadas

Delta Lake: Transacciones ACID, versionado automático, time-travel para auditoría.

H3 Indexing: Reduce 8 billones comparaciones a 10 millones, celdas hexagonales uniformes, join por string.

Agregación en Gold: Reduce 7.3M registros a 9,805 zonas, queries < 1 segundo.

## Requisitos

Python 3.8+, Databricks Workspace, AWS CLI, Git, pyspark>=3.0, h3>=3.7.0.

## Instalación y Ejecución

Clonar repositorio:

```bash
git clone https://github.com/TotrepData/vessel-traffic-depth-monitoring.git
cd vessel-traffic-depth-monitoring
```

Descargar NOAA:

```bash
brew install awscli
aws s3 cp s3://noaa-dcdb-bathymetry-pds/csb/csv/2025/01/01/ \
  ~/noaa_data/ --recursive --no-sign-request
```

En Databricks: Crear schema vessel_traffic_monitoring, crear volumen noaa_raw_data, subir 252 archivos CSV, ejecutar notebook en orden (fases 1-6).

## Resultados Clave

Volumen de datos: Bronze AIS 7.3M (2GB), Bronze NOAA 1.1M (381MB), Silver AIS 7.3M (2GB), Silver NOAA 1.1M (381MB), Silver Enriched 7.3M (2.5GB), Gold 9,805 zonas (5MB).

Reducción de escala: Entrada 7.3M posiciones, salida 9,805 zonas H3, factor 744x más pequeño.

## Casos de Uso

Navegación Segura: Alertas en aguas someras, rutas optimizadas.
Monitoreo Ambiental: Análisis de ecosistemas marinos.
Planificación Logística: Optimización de rutas comerciales.
Investigación Oceanográfica: Validación de modelos batimétricos.

## Licencia

MIT License

## Autor

TotrépData - Data Engineering | Big Data | Databricks

Última actualización: Noviembre 2025
Versión: 1.0.0
