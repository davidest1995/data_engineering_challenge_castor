# Prueba Técnica — Senior Data Engineer + BI
**Candidato:** David Esteban Arenas Ossa
**Empresa:** Castor
**Fecha:** Junio 2026

---

## Índice

1. [Arquitectura general](#arquitectura-general)
2. [Stack tecnológico y justificación](#stack-tecnológico-y-justificación)
3. [Estructura del repositorio](#estructura-del-repositorio)
4. [Fase 1 — Arquitectura del Data Lake](#fase-1--arquitectura-del-data-lake)
5. [Fase 2 — Pipeline ETL](#fase-2--pipeline-etl)
6. [Fase 3 — Visualización e Insights](#fase-3--visualización-e-insights)
7. [Fase 4 — Liderazgo Técnico y Mentoring](#fase-4--liderazgo-técnico-y-mentoring)
8. [Cómo ejecutar el proyecto](#cómo-ejecutar-el-proyecto)
9. [Decisiones de diseño y trade-offs](#decisiones-de-diseño-y-trade-offs)

---

## Arquitectura general

```
Fuentes de datos
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE / RAW                                       │
│  Datos sin transformar, tal como llegan             │
│  • transacciones_raw.csv  (SQL transaccional)       │
│  • usuarios_raw.json      (API terceros)            │
└────────────────────┬────────────────────────────────┘
                     │ Limpieza + validación
                     ▼
┌─────────────────────────────────────────────────────┐
│  SILVER / CLEANED                                   │
│  Datos limpios, tipados y deduplicados              │
│  • usuarios_silver  (Delta / Parquet particionado)  │
│  • transacciones_silver                             │
└────────────────────┬────────────────────────────────┘
                     │ Modelado dimensional / agregación
                     ▼
┌─────────────────────────────────────────────────────┐
│  GOLD / CURATED                                     │
│  Modelo analítico listo para consumo BI             │
│  • customer_summary  (tablón analítico)             │
│  Particionado por year / month                      │
└─────────────────────────────────────────────────────┘
```

---

## Stack tecnológico y justificación

| Componente | Tecnología | Justificación |
|---|---|---|
| Motor de procesamiento | **PySpark** (Databricks CE) | Procesamiento distribuido real; escala a millones de registros sin cambio de código |
| Formato de almacenamiento | **Delta Lake** (capa Silver/Gold) | ACID transactions, time-travel, schema evolution nativo; supera a Parquet puro para pipelines productivos |
| Formato intermedio | **Parquet** (capa Bronze) | Columnar, comprimido, universal; ideal para datos crudos sin necesidad de ACID |
| Orquestación / transformación | **PySpark SQL + DataFrame API** | Vectorizado, no iterativo; misma API en local y en clúster productivo |
| Particionamiento | `year` / `month` sobre fecha de transacción | Optimiza las consultas analíticas más frecuentes (rangos de tiempo) eliminando particiones irrelevantes (partition pruning) |
| Notebook | **Databricks** | UI integrada, cluster gestionado, Git integration, MLflow ready |
| Alternativa Gold en producción | **dbt sobre Databricks SQL** | Para equipos con perfiles analíticos; SQL declarativo, linaje automático, tests de calidad |

### ¿Por qué Delta Lake y no Parquet puro?

Delta Lake ofrece tres ventajas críticas sobre Parquet en un pipeline productivo:

1. **ACID transactions** — escrituras atómicas; nunca un estado corrupto si el job falla a mitad.
2. **Time-travel** — `VERSION AS OF N` permite auditorías y rollbacks sin backups manuales.
3. **Schema evolution** — acepta campos nuevos sin romper lectores downstream (relevante para la Fase 1, pregunta 3 sobre evolución de esquema JSON).

En datasets pequeños la diferencia es imperceptible. En producción con millones de registros y múltiples escritores concurrentes, la diferencia es la que separa un data lake de un data swamp.

---

## Estructura del repositorio

```
castor-data-engineer/
│
├── README.md                        ← Este archivo
├── requirements.txt                 ← Dependencias Python (entorno local)
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb    ← Ingesta raw → capa Bronze
│   ├── 02_silver_cleaning.ipynb     ← Limpieza y deduplicación → Silver
│   ├── 03_gold_aggregation.ipynb    ← Modelado y join → Gold
│   └── 00_exploration.ipynb         ← EDA inicial de los datasets
│
├── data/
│   ├── raw/                         ← Archivos originales (Bronze local)
│   │   ├── transacciones_raw.csv
│   │   └── usuarios_raw.json
│   ├── silver/                      ← Output Silver (Delta / Parquet)
│   └── gold/                        ← Output Gold con particionamiento
│
├── docs/
│   ├── fase1_arquitectura.md        ← Respuestas detalladas Fase 1
│   ├── fase3_dashboard_design.md    ← Diseño del dashboard Tableau
│   └── fase4_mentoring.md          ← Estrategia de mentoring
│
└── tests/
    └── test_transformations.py      ← Tests unitarios de funciones clave
```

---

## Fase 1 — Arquitectura del Data Lake

> Respuesta completa en [`docs/fase1_arquitectura.md`](docs/fase1_arquitectura.md)

### 1. Capas del Data Lake

**Bronze / Raw**
- Datos ingeridos tal como llegan: sin transformaciones, sin filtros.
- Propósito: fuente de verdad inmutable. Ante cualquier error downstream, se puede reprocesar desde aquí.
- Formato: Parquet (columnar, comprimido). Para la base SQL transaccional: Parquet particionado por `fecha_ingesta`.
- Controles: checksum del archivo, log de ingesta (timestamp, tamaño, fuente), alertas de volumen anómalo.

**Silver / Cleaned**
- Transformaciones aplicadas:
  - Deduplicación con regla de negocio (registro más reciente por `id_usuario`).
  - Estandarización de fechas a `YYYY-MM-DD`.
  - Limpieza de `monto`: eliminar `$`, `,` y espacios; castear a `DoubleType`.
  - Conversión de `timestamp_unix` a `TimestampType`.
  - Manejo de nulos (ver criterio en Fase 2).
- Formato: **Delta Lake** — habilita time-travel y schema evolution.
- Controles de calidad (Great Expectations o Spark assertions):
  - `id_usuario` / `id_transaccion` no nulos y únicos.
  - `monto > 0`.
  - `fecha_transaccion` dentro de rango razonable (no futura, no anterior a 2020).

**Gold / Curated**
- Modelo analítico agregado: tablón `customer_summary` con `total_gastado`, `num_transacciones`, `ticket_promedio`.
- Joins optimizados con broadcast hint cuando una tabla es pequeña (usuarios ~10K registros).
- Particionado por `year` / `month` de la transacción.
- Formato: **Delta Lake** con `OPTIMIZE` y `ZORDER BY id_usuario` para acelerar consultas por cliente.
- Acceso: permisos de solo lectura para perfiles de consumo (analistas, Tableau). Las capas Bronze y Silver son inaccesibles para usuarios no técnicos (principio de menor privilegio).

### 2. Estrategia de ingesta y particionamiento

**Ingesta de base transaccional SQL sin afectar producción:**
- **Change Data Capture (CDC)** con Debezium o la funcionalidad nativa del motor SQL, capturando únicamente los deltas desde la última ejecución. Nunca un `SELECT *` completo en producción.
- Horario fuera de pico (batch nocturno) o streaming con Kafka → Spark Structured Streaming para baja latencia.
- Conexión mediante réplica de lectura (Read Replica), nunca sobre el primario.
- Watermark en Spark Structured Streaming para manejar eventos tardíos.

**Particionamiento:**
- Capa Gold: `year` / `month` — alineado con las consultas analíticas más frecuentes (series de tiempo, comparativas mes a mes).
- Evitar particionamiento por columnas de alta cardinalidad (ej. `id_usuario`) en el nivel de archivo; para eso se usa `ZORDER BY` en Delta.
- Tamaño objetivo por partición: 128 MB – 1 GB (evitar el "small file problem").

### 3. Evolución del esquema JSON

Si la API de terceros agrega o elimina campos:

- **Delta Lake con `mergeSchema = true`**: acepta columnas nuevas automáticamente sin romper el pipeline. Los campos eliminados quedan como `null` en los registros nuevos.
- **Schema registry** (Confluent o AWS Glue): versiona el esquema formalmente y permite validar compatibilidad hacia atrás antes de procesar.
- **Estrategia defensiva en código**: lectura con `PERMISSIVE` mode en Spark + columna `_corrupt_record` para capturar registros malformados sin detener el job.
- **Alertas de drift**: comparar el esquema inferido de cada ingesta contra el esquema registrado. Si hay campos críticos faltantes, pausar el pipeline y notificar.

---

## Fase 2 — Pipeline ETL

> Código completo en los notebooks de la carpeta `notebooks/`

### Datos de entrada — diagnóstico

| Dataset | Registros | Problemas identificados |
|---|---|---|
| `usuarios_raw.json` | 134 | 14 duplicados por `id_usuario`, 18 nulos en `pais`, 10 nulos en `edad`, 3 formatos de fecha distintos (`YYYY-MM-DD`, `DD/MM/YYYY`, `YYYY-MM-DD HH:MM:SS`) |
| `transacciones_raw.csv` | 500 | `monto` como string con `$`, `,` y espacios, 99 nulos en `categoria`, `timestamp_unix` requiere conversión |

### Criterio de manejo de nulos

**`pais` (18 nulos):** Imputación con `"Desconocido"`. El campo tiene valor analítico (segmentación geográfica); eliminar 13% de usuarios impacta la representatividad del modelo.

**`edad` (10 nulos):** Imputación con la mediana por `pais`. La mediana es robusta a outliers. Alternativa conservadora: mantener `null` y excluir de análisis de segmentación por edad.

**`categoria` en transacciones (99 nulos = 19.8%):** Imputación con `"Sin categoría"`. No se eliminan porque el monto y la fecha sí son válidos; excluirlos sesgaría el total de gasto por usuario.

### Transformaciones Silver → Gold

- Join entre `transacciones_silver` y `usuarios_silver` por `id_usuario` (`LEFT JOIN` para preservar transacciones de usuarios no encontrados).
- Broadcast hint en la tabla de usuarios por ser la más pequeña.
- Agregaciones: `SUM(monto)`, `COUNT(id_transaccion)`, `AVG(monto)` agrupadas por `id_usuario`.

### Formato de salida

Delta Lake con particionamiento `year` / `month`:
```
gold/customer_summary/
  year=2024/
    month=1/  *.parquet
    month=2/  *.parquet
  year=2025/
    ...
```

---

## Fase 3 — Visualización e Insights

> Diseño completo en [`docs/fase3_dashboard_design.md`](docs/fase3_dashboard_design.md)

### Propuesta de Dashboard (C-Level)

Pregunta de negocio: *"¿Quiénes son nuestros clientes más valiosos y cómo evolucionan sus compras mes a mes?"*

**Componentes:**
1. KPIs de cabecera: Total revenue, Clientes activos, Ticket promedio, Transacciones totales.
2. Top 10 clientes por gasto total (bar chart horizontal).
3. Evolución de revenue mes a mes (line chart con anotaciones de hitos).
4. Distribución de gasto por categoría (treemap).
5. Tabla de detalle de clientes con filtros dinámicos (país, rango de fecha, categoría).

### Conexión Live vs. Extracto en Tableau

**Elección: Extracto (Extract).**

Para un Data Lake con millones de registros, una conexión Live ejecuta cada interacción del usuario como una query directa al motor, generando latencia inaceptable para dashboards de C-Level. Un extracto de Tableau (.hyper) materializa los datos de la capa Gold (ya agregados) en memoria columnar optimizada, con tiempos de respuesta de milisegundos.

El extracto se refresca programáticamente (Tableau Server / Tableau Cloud) cada vez que el pipeline ETL completa su ejecución Gold — desacoplando la disponibilidad del dashboard del rendimiento del Data Lake.

### LOD FIXED vs. Agregación normal

`FIXED` se usa cuando el nivel de detalle del cálculo debe ser independiente de los filtros o dimensiones del view actual. Ejemplo: calcular el porcentaje que representa cada cliente sobre el **total histórico** (no sobre el subconjunto filtrado). Con una agregación normal, el denominador cambia al aplicar filtros; con `FIXED {id_usuario}: SUM(monto)} / FIXED {}: SUM(monto)` el denominador permanece sobre el universo completo.

### Optimización de Workbook lento

1. Migrar a fuente de Extracto si se usa Live.
2. Reducir el número de marks (puntos) en el view — agregar más en la fuente antes de cargar a Tableau.
3. Eliminar calculated fields redundantes; moverlos a la capa Gold del pipeline.
4. Usar context filters para reducir el espacio de datos antes de aplicar otros filtros.
5. Limitar el número de dashboards en un mismo workbook — separar por audiencia.

---

## Fase 4 — Liderazgo Técnico y Mentoring

> Detalle completo en [`docs/fase4_mentoring.md`](docs/fase4_mentoring.md)

### Enfoque de Code Review

La revisión se inicia reconociendo que el código **funciona y cumple el objetivo funcional** — ese es un punto de partida sólido. El problema no es la lógica, sino la escala. La conversación se estructura así:

1. **Primero el elogio genuino**: "El script hace exactamente lo que se pedía y está bien estructurado para 100 registros."
2. **Luego la pregunta, no la sentencia**: "¿Qué crees que pasaría si en vez de 100 registros tuviéramos 10 millones?" — dejar que el Junior llegue al problema por sí mismo.
3. **Analogía para explicar vectorización**: Un bucle `for` es como copiar un libro a mano, letra por letra. Pandas/PySpark es como una fotocopiadora: opera sobre todo el documento en paralelo.
4. **Demo en vivo**: mostrar el mismo resultado con `.str.replace()` + `pd.to_datetime()`, medir el tiempo con `%%timeit`. Los números hablan solos.

### Plan de mentoría (30 días)

| Semana | Acción | Objetivo |
|---|---|---|
| 1 | Pair Programming: refactorizar el script juntos usando Pandas vectorizado | Transferencia práctica inmediata |
| 2 | Lectura guiada: capítulos de "Effective Pandas" (Matt Harrison) | Base conceptual de operaciones vectorizadas |
| 3 | Mini-proyecto: mismo pipeline en PySpark con dataset de 1M registros generado sintéticamente | Exposición real a escala |
| 4 | Code Review inverso: el Junior revisa un PR tuyo y señala oportunidades de mejora | Consolida el aprendizaje y genera ownership |

---

## Cómo ejecutar el proyecto

### En Databricks Community Edition

```bash
# 1. Importar el repositorio vía Git en el workspace de Databricks
# Workspace → Repos → Add Repo → pegar URL del repo

# 2. Crear un cluster (Runtime 13.x LTS con Spark 3.4)
# Compute → Create Cluster → Databricks Runtime 13.3 LTS

# 3. Ejecutar los notebooks en orden:
#    notebooks/00_exploration.ipynb
#    notebooks/01_bronze_ingestion.ipynb
#    notebooks/02_silver_cleaning.ipynb
#    notebooks/03_gold_aggregation.ipynb
```

### En local (solo para desarrollo/exploración)

```bash
# Requisitos: Python 3.10+, Java 11+
pip install -r requirements.txt

# Los notebooks locales usan PySpark con Delta Lake
# El output se escribe en data/silver/ y data/gold/
jupyter notebook notebooks/
```

---

## Decisiones de diseño y trade-offs

| Decisión | Alternativa considerada | Razón de la elección |
|---|---|---|
| Delta Lake sobre Parquet puro | Parquet + Hive metastore | Delta simplifica ACID, time-travel y schema evolution sin infraestructura adicional |
| LEFT JOIN en Gold | INNER JOIN | Preserva transacciones de usuarios con datos incompletos; se investigan en lugar de silenciarse |
| Imputar `categoria` nula | Eliminar registros | 19.8% de nulos es demasiado para descartar sin sesgar el análisis de gasto |
| Partición por `year/month` | Partición por `id_usuario` | Las queries analíticas filtran por tiempo, no por usuario individual |
| Broadcast hint en join | Sort-merge join por defecto | `usuarios_silver` < 10K registros → broadcast es el join más rápido para tablas pequeñas en Spark |
| dbt mencionado pero no implementado | dbt como capa Gold | El time-box de la prueba prioriza el pipeline PySpark; dbt se propone como evolución natural en producción |
