# Data Engineering Challenge — Castor

**Candidato:** David Arenas  
**Empresa:** Castor  
**Fecha:** Junio 2026  
**Plataforma de ejecución:** Databricks Community Edition

---

## Índice

1. [Arquitectura](#arquitectura)
2. [Stack tecnológico](#stack-tecnológico)
3. [Estructura del repositorio](#estructura-del-repositorio)
4. [Catálogo de tablas](#catálogo-de-tablas)
5. [Ejecución en Databricks](#ejecución-en-databricks)
6. [Fase 1 — Arquitectura del Data Lake](#fase-1--arquitectura-del-data-lake)
7. [Fase 2 — Pipeline ETL](#fase-2--pipeline-etl)
8. [Fase 3 — Visualización e Insights](#fase-3--visualización-e-insights)
9. [Fase 4 — Liderazgo Técnico y Mentoring](#fase-4--liderazgo-técnico-y-mentoring)
10. [Decisiones de diseño](#decisiones-de-diseño)

---

## Arquitectura

El proyecto implementa la **Arquitectura Medallón** (Bronze → Silver → Gold) sobre Databricks Community Edition con tablas gestionadas en Unity Catalog (Hive Metastore compatible).

```
Fuentes de datos
      │
      ▼
┌─────────────────────────────────────────────┐
│  BRONZE                                     │
│  Ingesta sin transformaciones               │
│  • brz_fact_transactions                    │
│  • brz_dim_users                            │
│  Formato: Delta (Managed Table)             │
└────────────────────┬────────────────────────┘
                     │ Limpieza + validación + tipado
                     ▼
┌─────────────────────────────────────────────┐
│  SILVER                                     │
│  Datos limpios, tipados y deduplicados      │
│  • slv_fact_transactions                    │
│  • slv_dim_users                            │
│  Formato: Delta (Managed Table)             │
└────────────────────┬────────────────────────┘
                     │ Modelado dimensional + análisis
                     ▼
┌─────────────────────────────────────────────┐
│  GOLD                                       │
│  Datamarts analíticos listos para BI        │
│  • gld_customer_360   (Customer Intelligence│
│  • gld_cohort_retention (Retention Analysis)│
│  Formato: Delta (Managed Table)             │
└─────────────────────────────────────────────┘
```

---

## Stack tecnológico

| Componente | Tecnología | Justificación |
|---|---|---|
| Motor de procesamiento | PySpark (Databricks CE) | Procesamiento distribuido; mismo código escala a millones de registros sin modificación |
| Formato de almacenamiento | Delta Lake | ACID transactions, time-travel y schema evolution nativo sobre todas las capas |
| Modelado | Kimball (Fact + Dim) | Nomenclatura y separación de responsabilidades estándar de la industria |
| Catálogo | Unity Catalog / Hive Metastore | Gobernanza, control de accesos y descubrimiento de datos integrado en Databricks |
| Control de versiones | Git + GitHub | Integración nativa con Databricks Repos para CI/CD y trabajo en ramas |
| Particionamiento | `cohort_month` en Retention | Optimiza las consultas analíticas más frecuentes eliminando particiones irrelevantes |

---

## Estructura del repositorio

```
data_engineering_challenge_castor/
│
├── README.md
├── requirements.txt
│
├── data/
│   ├── transacciones_raw.csv        ← Fuente transaccional (500 registros)
│   └── usuarios_raw.json            ← Fuente API de terceros (134 registros)
│
├── docs/
│   ├── fase1_arquitectura.md        ← Respuestas Fase 1: Data Lake design
│   ├── fase3_dashboard_design.md    ← Respuestas Fase 3: Dashboard & Tableau
│   └── fase4_mentoring.md          ← Respuestas Fase 4: Liderazgo técnico
│
└── notebooks/
    ├── utils/
    │   └── unity_catalog_config.ipynb   ← Setup inicial del catálogo (ejecutar primero)
    │
    ├── bronze/
    │   ├── brz_fact_transactions.ipynb  ← Ingesta CSV → castor.bronze.brz_fact_transactions
    │   └── brz_dim_users.ipynb          ← Ingesta JSON → castor.bronze.brz_dim_users
    │
    ├── silver/
    │   ├── slv_fact_transactions.ipynb  ← Limpieza de transacciones → castor.silver.slv_fact_transactions
    │   └── slv_dim_users.ipynb          ← Limpieza y deduplicación de usuarios → castor.silver.slv_dim_users
    │
    └── gold/
        ├── gld_customer_360.ipynb       ← Customer Intelligence + RFM + Anomaly Detection
        └── gld_cohort_retention.ipynb   ← Cohort Analysis + Retention Rate por periodo
```

---

## Catálogo de tablas

### Bronze (`castor.bronze`)

| Tabla | Fuente | Descripción |
|---|---|---|
| `brz_fact_transactions` | `transacciones_raw.csv` | Transacciones sin transformar. Columnas originales + `fecha_ingesta` |
| `brz_dim_users` | `usuarios_raw.json` | Usuarios sin transformar. Columnas originales + `fecha_ingesta` |

### Silver (`castor.silver`)

| Tabla | Transformaciones aplicadas |
|---|---|
| `slv_fact_transactions` | `monto` limpiado de símbolos y casteado a `Double`. `fecha_transaccion` derivada de `timestamp_unix`. Nulos en `categoria` imputados con `'Sin categoría'` |
| `slv_dim_users` | `fecha_registro` normalizada (3 formatos) con `try_to_date`. Deduplicación por `id_usuario` vía `row_number()`. Nulos en `pais` imputados con `'Desconocido'`. Nulos en `edad` imputados con mediana por país |

### Gold (`castor.gold`)

| Tabla | Descripción | Caso de uso en BI |
|---|---|---|
| `gld_customer_360` | Snapshot analítico por usuario. Incluye métricas RFM, segmentación de edad, antigüedad, categoría favorita y perfil de riesgo | Dashboard C-Level: top customers, revenue por segmento, mapa geográfico |
| `gld_cohort_retention` | Tasa de retención por cohorte de primera compra y periodo (month index). Particionada por `cohort_month` | Retention Grid / Heatmap para equipos de Producto y Marketing |

#### Columnas clave de `gld_customer_360`

| Columna | Tipo | Descripción |
|---|---|---|
| `recency_dias` | Integer | Días desde la última compra hasta la fecha máxima del dataset |
| `frequency_txs` | Long | Número total de transacciones del usuario |
| `monetary_total` | Double | Gasto acumulado histórico |
| `ticket_promedio` | Double | Gasto promedio por transacción |
| `categoria_favorita` | String | Categoría donde el usuario concentra mayor gasto |
| `rango_edad` | String | Segmento etario: `18-24`, `25-35`, `36-50`, `51+` |
| `antiguedad_dias` | Integer | Días desde el registro del usuario |
| `txs_anomalas` | Long | Transacciones que superan `avg + 3 * stddev` del propio usuario |
| `perfil_riesgo` | String | Clasificación: `Normal`, `Bajo`, `Medio`, `Alto` |

#### Columnas clave de `gld_cohort_retention`

| Columna | Tipo | Descripción |
|---|---|---|
| `cohort_month` | String | Mes de la primera compra (formato `yyyy-MM`) |
| `period_index` | Integer | 0 = mes de entrada, 1 = mes siguiente, N = N meses después |
| `cohort_size` | Long | Usuarios únicos que entraron en esa cohorte |
| `active_users` | Long | Usuarios de la cohorte que compraron en ese periodo |
| `retention_rate` | Double | `active_users / cohort_size` (0.0 a 1.0) |
| `revenue_cohort` | Double | Ingresos generados por la cohorte en ese periodo |

---

## Ejecución en Databricks

**Requisitos previos:**
- Databricks Community Edition activo
- Repositorio clonado en Databricks Repos (Workspace → Repos → Add Repo)
- Cluster con Databricks Runtime 13.3 LTS o superior activo

**Orden de ejecución:**

```
1. notebooks/utils/unity_catalog_config.ipynb   ← Crear catálogo y schemas
2. notebooks/bronze/brz_fact_transactions.ipynb
3. notebooks/bronze/brz_dim_users.ipynb
4. notebooks/silver/slv_fact_transactions.ipynb
5. notebooks/silver/slv_dim_users.ipynb
6. notebooks/gold/gld_customer_360.ipynb
7. notebooks/gold/gld_cohort_retention.ipynb    ← Independiente de Customer 360
```

**Nota:** Los notebooks 2 y 3 son independientes entre sí, al igual que 4 y 5, y 6 y 7. Pueden ejecutarse en paralelo si se dispone de múltiples clusters.

---

## Fase 1 — Arquitectura del Data Lake

Respuesta detallada en [`docs/fase1_arquitectura.md`](docs/fase1_arquitectura.md)

---

## Fase 2 — Pipeline ETL

### Diagnóstico de los datos de entrada

| Dataset | Registros | Problemas identificados |
|---|---|---|
| `usuarios_raw.json` | 134 | 14 duplicados por `id_usuario`, 18 nulos en `pais`, 10 nulos en `edad`, 3 formatos de fecha distintos |
| `transacciones_raw.csv` | 500 | `monto` como string con `$`, `,` y espacios; 99 nulos en `categoria`; `timestamp_unix` requiere conversión |

### Criterio de manejo de nulos

| Campo | Acción | Justificación |
|---|---|---|
| `pais` (18 nulos) | Imputar con `'Desconocido'` | Eliminar el 13% de usuarios sesgaría análisis geográficos |
| `edad` (10 nulos) | Imputar con mediana por `pais` | La mediana es robusta a outliers; se preserva el contexto geográfico |
| `categoria` (99 nulos) | Imputar con `'Sin categoría'` | El monto y la fecha son válidos; excluirlos sesgaría el total de gasto por usuario |

### Detección de anomalías (capa Gold)

Se aplica la regla estadística de 3-sigma sobre el historial de cada usuario:

```
umbral = avg(monto_usuario) + 3 * stddev(monto_usuario)
is_anomalous = monto > umbral
```

Los usuarios se clasifican por `perfil_riesgo` según el número acumulado de transacciones anómalas.

---

## Fase 3 — Visualización e Insights

Respuesta detallada en [`docs/fase3_dashboard_design.md`](docs/fase3_dashboard_design.md)

### Tablas Gold recomendadas para consumo BI

| Tabla Gold | Visualización sugerida |
|---|---|
| `gld_customer_360` | KPIs de cabecera, top customers, treemap de categorías, mapa geográfico, semáforo de riesgo |
| `gld_cohort_retention` | Retention heatmap, curvas de retención por cohorte, revenue por cohorte |

---

## Fase 4 — Liderazgo Técnico y Mentoring

Respuesta detallada en [`docs/fase4_mentoring.md`](docs/fase4_mentoring.md)

---

## Decisiones de diseño

| Decisión | Alternativa descartada | Razón |
|---|---|---|
| Tablas gestionadas en Unity Catalog sobre rutas DBFS | `dbfs:/FileStore/...` o rutas del Workspace | El DBFS raíz está deshabilitado en versiones modernas de Databricks; el catálogo ofrece gobernanza, control de acceso y descubrimiento de datos nativo |
| `saveAsTable` en todas las capas | `df.write.parquet(path)` | Desacopla el almacenamiento físico de la capa de acceso; las tablas son descubribles sin conocer rutas físicas |
| Nomenclatura `fact_` / `dim_` en Kimball | Nomenclatura genérica plana | Facilita el entendimiento del modelo dimensional a cualquier ingeniero o analista que llegue al proyecto |
| Sobrescritura de columnas en Silver (sin sufijos `_clean` o `_raw`) | Columnas adicionales `monto_clean`, `fecha_registro_raw` | Silver representa la versión canónica y limpia; si se necesita la versión cruda, existe Bronze. Los sufijos enturbian el esquema sin aportar valor |
| `try_to_date` para parsing de fechas | `to_date` con modo PERMISSIVE | El modo ANSI de Databricks hace que `to_date` lance excepción ante formato incorrecto; `try_to_date` devuelve `NULL` y permite el fallback con `coalesce` |
| LEFT JOIN en Gold | INNER JOIN | Preserva transacciones de usuarios con datos incompletos; se investigan en lugar de silenciarse |
| Análisis de cohortes particionado por `cohort_month` | Sin particionamiento | Las queries de retención siempre filtran por cohorte; el partition pruning reduce el escaneo de datos drásticamente |
