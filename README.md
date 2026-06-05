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
│  Particionado por: fecha_ingesta            │
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
│  • gld_customer_360        (Vista 360 + RFM + Anomalías)    │
│  • gld_cohort_retention    (Análisis de Retención)          │
│  • gld_customer_monthly_spending (Evolución mensual)        │
│  Formato: Delta (Managed Table)             │
└─────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│  BI / VISUALIZACIÓN                         │
│  • Tableau (conexión live a Databricks)     │
│  • Dashboard: Clientes Valiosos + Retención │
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
| Particionamiento | `fecha_ingesta` en Bronze, `cohort_month` en Retention | Optimiza las consultas analíticas más frecuentes eliminando particiones irrelevantes |
| Visualización | Tableau (conexión live a Databricks) | Consumo directo de tablas Gold sin transformaciones adicionales |

---

## Estructura del repositorio

```
data_engineering_challenge_castor/
│
├── README.md
├── requirements.txt
├── .gitignore                              ← Excluye .twb, PDFs y archivos de sistema
│
├── data/
│   ├── transacciones_raw.csv              ← Fuente transaccional (500 registros)
│   └── usuarios_raw.json                  ← Fuente API de terceros (134 registros)
│
├── docs/
│   ├── fase1_arquitectura.md              ← Respuestas Fase 1: Data Lake design
│   ├── fase3_dashboard_design.md          ← Respuestas Fase 3: Dashboard & Tableau
│   └── fase4_mentoring.md                 ← Respuestas Fase 4: Liderazgo técnico
│
└── notebooks/
    ├── utils/
    │   └── unity_catalog_config.ipynb     ← Setup inicial del catálogo (ejecutar primero)
    │
    ├── bronze/
    │   ├── brz_fact_transactions.ipynb    ← Ingesta CSV → castor.bronze.brz_fact_transactions
    │   └── brz_dim_users.ipynb            ← Ingesta JSON → castor.bronze.brz_dim_users
    │
    ├── silver/
    │   ├── slv_fact_transactions.ipynb    ← Limpieza de transacciones → castor.silver.slv_fact_transactions
    │   └── slv_dim_users.ipynb            ← Limpieza y deduplicación de usuarios → castor.silver.slv_dim_users
    │
    └── gold/
        ├── gld_customer_360.ipynb          ← Customer Intelligence + RFM + Anomaly Detection
        ├── gld_cohort_retention.ipynb      ← Cohort Analysis + Retention Rate por periodo
        └── gld_customer_monthly_spending.ipynb ← Gasto mensual por usuario (alimenta Tableau)
```

---

## Catálogo de tablas

### Bronze (`castor.bronze`)

| Tabla | Fuente | Transformaciones | Descripción |
|---|---|---|---|
| `brz_fact_transactions` | `transacciones_raw.csv` | `fecha_ingesta` añadida, particionada por `fecha_ingesta` | Transacciones tal como llegan de la fuente |
| `brz_dim_users` | `usuarios_raw.json` | `fecha_ingesta` añadida, lectura con `multiLine=True` | Usuarios sin transformar desde API de terceros |

### Silver (`castor.silver`)

| Tabla | Transformaciones aplicadas |
|---|---|
| `slv_fact_transactions` | `monto` limpiado de `$`, `,` y espacios vía `regexp_replace` y casteado a `Double`. `fecha_transaccion` derivada de `timestamp_unix` con `from_unixtime`. Nulos en `categoria` imputados con `'Sin categoría'` |
| `slv_dim_users` | `fecha_registro` normalizada con `try_to_date` sobre 3 formatos (`yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy-MM-dd HH:mm:ss`) usando `coalesce`. Deduplicación por `id_usuario` vía `row_number()` sobre ventana. Nulos en `pais` → `'Desconocido'`. Nulos en `edad` → mediana calculada con `approxQuantile` |

### Gold (`castor.gold`)

| Tabla | Descripción | Caso de uso en BI |
|---|---|---|
| `gld_customer_360` | Snapshot analítico por usuario. Incluye métricas RFM, segmentación de edad, antigüedad, categoría favorita y perfil de riesgo | Dashboard C-Level: top customers, revenue por segmento, mapa geográfico, semáforo de riesgo |
| `gld_cohort_retention` | Tasa de retención por cohorte de primera compra y periodo (month index). Particionada por `cohort_month` | Retention Grid / Heatmap para equipos de Producto y Marketing |
| `gld_customer_monthly_spending` | Gasto mensual y número de transacciones por usuario y mes. Enriquecida con nombre del usuario | Análisis de evolución mensual de clientes; responde "¿quiénes son los más valiosos y cómo evolucionan?" |

#### Columnas clave de `gld_customer_360`

| Columna | Tipo | Descripción |
|---|---|---|
| `recency_dias` | Integer | Días desde la última compra hasta la fecha máxima del dataset |
| `frequency_txs` | Long | Número total de transacciones del usuario |
| `monetary_total` | Double | Gasto acumulado histórico |
| `ticket_promedio` | Double | Gasto promedio por transacción |
| `categoria_favorita` | String | Categoría donde el usuario concentra mayor gasto |
| `rango_edad` | String | Segmento etario: `18-24`, `25-35`, `36-50`, `51+` |
| `antiguedad_dias` | Integer | Días desde el registro del usuario hasta la fecha máxima del dataset |
| `txs_anomalas` | Long | Transacciones que superan `avg + 3 * stddev` del propio usuario |
| `perfil_riesgo` | String | Clasificación: `Normal`, `Bajo`, `Medio`, `Alto` |
| `umbral_anomalia` | Double | Valor máximo del umbral estadístico calculado por usuario |
| `monto_max_historico` | Double | Transacción de mayor monto registrada por el usuario |

#### Columnas clave de `gld_cohort_retention`

| Columna | Tipo | Descripción |
|---|---|---|
| `cohort_month` | String | Mes de la primera compra (formato `yyyy-MM`) |
| `period_index` | Integer | 0 = mes de entrada, 1 = mes siguiente, N = N meses después |
| `cohort_size` | Long | Usuarios únicos que entraron en esa cohorte |
| `active_users` | Long | Usuarios de la cohorte que compraron en ese periodo |
| `retention_rate` | Double | `active_users / cohort_size` (0.0 a 1.0) |
| `revenue_cohort` | Double | Ingresos generados por la cohorte en ese periodo |

#### Columnas de `gld_customer_monthly_spending`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_usuario` | String | Identificador único del cliente |
| `nombre` | String | Nombre del cliente (join con `slv_dim_users`) |
| `ano_mes` | String | Año-mes de la actividad (formato `yyyy-MM`) |
| `gasto_mensual` | Double | Suma del gasto del cliente en ese mes |
| `transacciones_del_mes` | Long | Número de transacciones realizadas en ese mes |

---

## Ejecución en Databricks

**Requisitos previos:**
- Databricks Community Edition activo
- Repositorio clonado en Databricks Repos (Workspace → Repos → Add Repo → URL del repositorio)
- Cluster con Databricks Runtime 13.3 LTS o superior activo

**Orden de ejecución:**

```
1. notebooks/utils/unity_catalog_config.ipynb          ← Crear catálogo y schemas (solo 1 vez)
2. notebooks/bronze/brz_fact_transactions.ipynb
3. notebooks/bronze/brz_dim_users.ipynb
4. notebooks/silver/slv_fact_transactions.ipynb
5. notebooks/silver/slv_dim_users.ipynb
6. notebooks/gold/gld_customer_360.ipynb
7. notebooks/gold/gld_cohort_retention.ipynb           ← Independiente de Customer 360
8. notebooks/gold/gld_customer_monthly_spending.ipynb  ← Independiente; consume Silver directamente
```

> **Nota:** Los notebooks 2 y 3 son independientes entre sí, al igual que 4 y 5. En Gold, los notebooks 6, 7 y 8 son también independientes entre sí y pueden ejecutarse en paralelo si se dispone de múltiples clusters.

---

## Fase 1 — Arquitectura del Data Lake

Respuesta detallada en [`docs/fase1_arquitectura.md`](docs/fase1_arquitectura.md)

### Principios aplicados

- **Inmutabilidad en Bronze:** los datos de origen se preservan intactos para permitir reprocesamiento sin acudir a la fuente.
- **Canónico en Silver:** la capa Silver representa la única versión de la verdad de cada entidad. No se crean columnas auxiliares con sufijos `_clean`; si se necesita la versión cruda, existe Bronze.
- **Orientado a negocio en Gold:** cada tabla Gold responde una pregunta de negocio específica. Las transformaciones complejas ocurren en el pipeline, no en la herramienta de BI.
- **Menor privilegio:** las capas Bronze y Silver son inaccesibles para perfiles de consumo (analistas, BI). Solo Gold es de lectura pública.

---

## Fase 2 — Pipeline ETL

### Diagnóstico de los datos de entrada

| Dataset | Registros | Problemas identificados |
|---|---|---|
| `usuarios_raw.json` | 134 | 14 duplicados por `id_usuario`, 18 nulos en `pais`, 10 nulos en `edad`, 3 formatos de fecha distintos |
| `transacciones_raw.csv` | 500 | `monto` como string con `$`, `,` y espacios; 99 nulos en `categoria`; `timestamp_unix` requiere conversión a fecha legible |

### Criterio de manejo de nulos

| Campo | Acción | Justificación |
|---|---|---|
| `pais` (18 nulos) | Imputar con `'Desconocido'` | Eliminar el 13% de usuarios sesgaría análisis geográficos |
| `edad` (10 nulos) | Imputar con mediana global (`approxQuantile`) | La mediana es robusta a outliers; se evita sesgar la distribución etaria |
| `categoria` (99 nulos) | Imputar con `'Sin categoría'` | El monto y la fecha son válidos; excluirlos sesgaría el total de gasto por usuario |

### Transformaciones técnicas destacadas

#### Limpieza de `monto` (Silver — Transacciones)
```python
F.regexp_replace(F.col('monto'), r'[\$,\s]', '').cast('double')
```
Elimina el símbolo de moneda, separadores de miles y espacios antes de castear.

#### Normalización de `fecha_registro` (Silver — Usuarios)
```python
F.coalesce(
    F.expr("try_to_date(fecha_registro, 'yyyy-MM-dd')"),
    F.expr("try_to_date(fecha_registro, 'dd/MM/yyyy')"),
    F.expr("try_to_date(fecha_registro, 'yyyy-MM-dd HH:mm:ss')")
)
```
`try_to_date` devuelve `NULL` en lugar de lanzar excepción ante formato incorrecto. `coalesce` prueba los tres formatos en cascada hasta obtener una fecha válida.

#### Deduplicación de usuarios (Silver — Usuarios)
```python
window = Window.partitionBy('id_usuario').orderBy(F.col('fecha_registro').desc())
df_user_clean = df_user.withColumn('rn', F.row_number().over(window)) \
                       .filter(F.col('rn') == 1).drop('rn')
```
Se conserva el registro con la fecha de registro más reciente para cada `id_usuario`.

#### Categoría favorita por usuario (Gold — Customer 360)
```python
window_cat = Window.partitionBy('id_usuario').orderBy(F.col('gasto_categoria').desc())
fav_cat_df = cat_df.withColumn('rn', F.row_number().over(window_cat)) \
                   .filter(F.col('rn') == 1) \
                   .select('id_usuario', F.col('categoria').alias('categoria_favorita'))
```
Agrupa por usuario y categoría, suma el gasto, y extrae la categoría de mayor acumulado.

### Detección de anomalías — Regla 3-Sigma (Gold — Customer 360)

Se aplica la regla estadística de Z-Score simplificada sobre el historial de cada usuario:

```
umbral = avg(monto_usuario) + 3 * stddev(monto_usuario)
is_anomalous = monto > umbral
```

Los usuarios se clasifican por `perfil_riesgo` según el número acumulado de transacciones anómalas:

| `txs_anomalas` | `perfil_riesgo` |
|---|---|
| 0 | Normal |
| 1 | Bajo |
| 2 – 3 | Medio |
| > 3 | Alto |

> **Manejo de edge case:** para usuarios con una sola transacción, `stddev` es `NULL`. Se usa `coalesce(stddev, 0)` para evitar que el umbral sea nulo y toda transacción sea marcada como anómala.

### Análisis de cohortes (Gold — Cohort Retention)

La lógica de cohortes se construye íntegramente en PySpark sin SQL externo:

1. **Cohorte** = mes de la primera compra de cada usuario (`date_format(min(fecha_transaccion), 'yyyy-MM')`).
2. **Period index** = diferencia en meses entre el mes de la transacción y el mes de la cohorte:
   ```python
   (F.year('transaction_month_ts') - F.year('cohort_month_ts')) * 12 +
   (F.month('transaction_month_ts') - F.month('cohort_month_ts'))
   ```
3. **Retention rate** = `active_users / cohort_size` donde `cohort_size` es el número de usuarios únicos en `period_index = 0`.

---

## Fase 3 — Visualización e Insights

Respuesta detallada en [`docs/fase3_dashboard_design.md`](docs/fase3_dashboard_design.md)

### Conexión Tableau → Databricks

El tablero se conecta en modo **live** directamente a la tabla `castor.gold.gld_customer_monthly_spending` en Databricks, usando el conector nativo de Tableau para Databricks (Spark SQL). Esto garantiza que el dashboard siempre refleja el último estado del pipeline sin necesidad de extraer datos manualmente.

### Pregunta respondida: ¿Quiénes son los clientes más valiosos y cómo evolucionan sus compras mes a mes?

La respuesta se construye sobre `gld_customer_monthly_spending`:

| Visualización | Métrica | Insight |
|---|---|---|
| Bar chart horizontal (Top N clientes) | `SUM(gasto_mensual)` por `nombre` | Identifica los clientes que concentran mayor revenue histórico |
| Line chart (evolución temporal) | `SUM(gasto_mensual)` por `ano_mes`, desglosado por `nombre` | Revela patrones: clientes consistentes, estacionales o en declive |
| Mapa de calor (Heat Map) | `gasto_mensual` en matriz `nombre × ano_mes` | Muestra en qué meses cada cliente fue más activo |

**Segmentación de comportamiento resultante:**

- **Clientes consistentes:** gasto estable todos los meses → alta fidelidad → candidatos a programas VIP.
- **Clientes estacionales:** picos en meses específicos → oportunidades de campañas anticipadas.
- **Clientes en declive:** tendencia descendente → candidatos para acciones de reactivación.

### Otras tablas Gold para consumo BI

| Tabla Gold | Visualización sugerida |
|---|---|
| `gld_customer_360` | KPIs de cabecera, top customers, treemap de categorías, semáforo de riesgo |
| `gld_cohort_retention` | Retention heatmap, curvas de retención por cohorte, revenue por cohorte |
| `gld_customer_monthly_spending` | Evolución mensual por cliente, top clientes por periodo |

---

## Fase 4 — Liderazgo Técnico y Mentoring

Respuesta detallada en [`docs/fase4_mentoring.md`](docs/fase4_mentoring.md)

---

## Decisiones de diseño

| Decisión | Alternativa descartada | Razón |
|---|---|---|
| Tablas gestionadas en Unity Catalog | `dbfs:/FileStore/...` o rutas del Workspace | El DBFS raíz está deshabilitado en versiones modernas de Databricks; el catálogo ofrece gobernanza, control de acceso y descubrimiento de datos nativo |
| `saveAsTable` en todas las capas | `df.write.parquet(path)` | Desacopla el almacenamiento físico de la capa de acceso; las tablas son descubribles sin conocer rutas físicas |
| Nomenclatura `fact_` / `dim_` en Kimball | Nomenclatura genérica plana | Facilita el entendimiento del modelo dimensional a cualquier ingeniero o analista que llegue al proyecto |
| Sobrescritura de columnas en Silver (sin sufijos `_clean`) | Columnas adicionales `monto_clean`, `fecha_registro_raw` | Silver representa la versión canónica; si se necesita la versión cruda, existe Bronze. Los sufijos enturbian el esquema sin aportar valor |
| `try_to_date` para parsing de fechas | `to_date` con modo PERMISSIVE | El modo ANSI de Databricks hace que `to_date` lance excepción ante formato incorrecto; `try_to_date` devuelve `NULL` y permite el fallback con `coalesce` |
| LEFT JOIN en Gold | INNER JOIN | Preserva transacciones de usuarios con datos incompletos; se investigan en lugar de silenciarse |
| Análisis de cohortes particionado por `cohort_month` | Sin particionamiento | Las queries de retención siempre filtran por cohorte; el partition pruning reduce el escaneo de datos drásticamente |
| `coalesce(stddev, 0)` en detección de anomalías | Filtrar usuarios con 1 sola transacción | Evita descartar usuarios legítimos con poco historial; el umbral sin desviación es simplemente el propio monto promedio |
| `gld_customer_monthly_spending` como tabla Gold separada | Calcular en tiempo real en Tableau | Materializar el cálculo en Gold desacopla la lógica de negocio del dashboard; Tableau no debe ser un motor de transformación |
| Tableau en conexión live a Databricks | Extracto `.hyper` local | Para este proyecto de prueba la conexión live es suficiente. En producción con millones de filas se recomendaría extracto programático post-pipeline |
