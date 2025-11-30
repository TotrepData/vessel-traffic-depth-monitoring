# Vessel Traffic Depth Monitoring – Pipeline

## 1. Descripción general

Este proyecto implementa un pipeline completo para procesar, limpiar, enriquecer y analizar datos de tráfico marítimo usando:

- **AIS (Marine Cadastre)** – posiciones y atributos de embarcaciones.  
- **NOAA CSB (Crowdsourced Bathymetry)** – profundidades oceánicas.

El objetivo principal es construir una vista integrada que permita analizar **zonas navegadas**, **tipos de embarcación**, y **profundidad cercana**, asegurando un procesamiento **incremental y eficiente** sobre Databricks.

El pipeline sigue una arquitectura **Medallion** con tres capas:

- **Bronze:** ingestión cruda e incremental.  
- **Silver:** limpieza, validación, H3 indexing y enriquecimiento AIS–NOAA.  
- **Gold:** agregación final para análisis y dashboards.  

Todo se ejecuta mediante un **Databricks Workflow Job**.

---

## 2. Datasets

### 2.1 AIS – Marine Cadastre
- Archivos diarios `.csv.zst`.
- Atributos principales:
  - MMSI  
  - Latitud / longitud  
  - Fecha/hora  
  - Velocidad / rumbo  
  - Tipo de embarcación  
  - Tipo de carga  

### 2.2 NOAA – Crowdsourced Bathymetry
- Archivos CSV con mediciones de profundidad.
- Incluye:
  - Latitud / longitud  
  - Profundidad  
  - Timestamp  
  - Plataforma que mide  

---

## 3. Arquitectura del pipeline

```
AIS Raw Files       NOAA Raw Files
      |                    |
      v                    v
+-------------------------------+
|            BRONZE            |
+-------------------------------+
| AIS incremental (append)     |
| NOAA overwrite               |
+-------------------------------+
                |
                v
+-------------------------------+
|            SILVER            |
+-------------------------------+
| Limpieza AIS/NOAA            |
| H3 indexing                  |
| Ventana 30 mins              |
| Enriquecimiento AIS–NOAA     |
+-------------------------------+
                |
                v
+-------------------------------+
|             GOLD             |
+-------------------------------+
| Agregación final por:        |
|   event_date                 |
|   h3_cell                    |
|   vessel_type                |
|   cargo                      |
| Métricas de profundidad      |
| Conteo de barcos únicos      |
+-------------------------------+
```

---

## 4. Estructura del repositorio

```
vessel-traffic-depth-monitoring/
│
├── 00_config.py        # Configuración global (Spark, rutas, tablas)
├── 01_bronze.py        # Ingesta incremental AIS + NOAA
├── 02_silver.py        # Limpieza, H3 y enriquecimiento incremental
├── 03_gold.py          # Agregación final incremental
└── README.md
```

---

## 5. Capa Bronze

### Responsabilidades
- Leer archivos AIS en orden.
- Procesar solo una cantidad controlada por corrida:
```
MODO_DEMO = True
MAX_AIS_FILES_PER_RUN = 2
```
- Extraer `process_date` desde el nombre del archivo.
- Escribir AIS con **append**.
- Escribir NOAA con **overwrite**.

### Salidas
- `bronze_ais`  
- `bronze_noaa`

---

## 6. Capa Silver

### Responsabilidades
- Detectar días faltantes entre Bronze y Silver.
- Limpieza de AIS y NOAA.
- Generación de celdas H3.
- Ventanas temporales de 30 minutos.
- Enriquecimiento AIS–NOAA.

### Salidas
- `silver_ais`  
- `silver_noaa`  
- `silver_enriched`

---

## 7. Capa Gold

### Responsabilidades
- Detectar días faltantes entre Silver y Gold.
- Agregar por:
  - `event_date`
  - `h3_cell`
  - `vessel_type`
  - `cargo`

### Métricas generadas
- `vessel_count`
- `avg_depth_m`
- `min_depth_m`
- `max_depth_m`
- `avg_samples`
- `latitude`, `longitude`

### Salida
- `gold_analytics`

---

## 8. Incrementalidad

| Capa   | Detecta        | Procesa          | Modo    |
|--------|----------------|------------------|---------|
| Bronze | Nuevos archivos| N primeros       | append  |
| Silver | Nuevos días    | Diferencia       | append  |
| Gold   | Nuevos días    | Diferencia       | append  |

---

## 9. Workflow Job

Pipeline en 3 tareas:

```
bronze → silver → gold
```

Recomendaciones:
- Desarrollo: **Existing cluster**
- Producción: **Job cluster**
- Instalar `h3` como library del cluster

---

## 10. Dashboard

La tabla principal utilizada es:

```
catalog.schema.gold_analytics
```

---

## 11. Consultas SQL del Dashboard

### 11.1. Métricas generales

```sql
SELECT 
    COUNT(DISTINCT h3_cell)      AS zonas_navegadas,
    SUM(vessel_count)            AS total_barcos,
    ROUND(AVG(avg_depth_m), 2)   AS profundidad_promedio,
    ROUND(MIN(min_depth_m), 2)   AS profundidad_minima,
    ROUND(MAX(max_depth_m), 2)   AS profundidad_maxima
FROM catalog.schema.gold_analytics;
```

---

### 11.2. Actividad por celda H3

```sql
SELECT 
    h3_cell,
    ROUND(latitude, 4)  AS lat,
    ROUND(longitude, 4) AS lon,
    SUM(vessel_count)   AS total_vessels,
    ROUND(AVG(avg_depth_m), 2) AS depth_avg,
    ROUND(MIN(min_depth_m), 2) AS depth_min,
    ROUND(MAX(max_depth_m), 2) AS depth_max
FROM catalog.schema.gold_analytics
WHERE 
    avg_depth_m IS NOT NULL
GROUP BY h3_cell, latitude, longitude
ORDER BY total_vessels DESC;
```

---

### 11.3. Actividad por tipo de embarcación

```sql
SELECT 
    vessel_type,
    SUM(vessel_count)          AS total_barcos,
    ROUND(AVG(avg_depth_m), 2) AS profundidad_promedio
FROM catalog.schema.gold_analytics
WHERE 
    vessel_type IS NOT NULL
GROUP BY vessel_type
ORDER BY total_barcos DESC
LIMIT 15;
```

---

### 11.4. Actividad por tipo de carga

```sql
SELECT 
    cargo,
    SUM(vessel_count)          AS total_barcos,
    ROUND(AVG(avg_depth_m), 2) AS profundidad_promedio
FROM catalog.schema.gold_analytics
WHERE 
    cargo IS NOT NULL
GROUP BY cargo
ORDER BY total_barcos DESC
LIMIT 15;
```

---

## 12. Cómo agregar nuevos datos

### 12.1. Nuevos AIS
1. Copiar archivos nuevos.  
2. Ejecutar el Workflow.  
3. El pipeline actualizará Bronze → Silver → Gold.

### 12.2. Nuevos NOAA
- NOAA se recarga por completo en Bronze.
- Silver recalcula enriquecimiento solo para días nuevos.

---

## 13. Notas finales
- Configuración centralizada en `00_config.py`.
- Pipeline modular, escalable y reproducible.
- Salidas Gold compatibles con dashboards existentes.
