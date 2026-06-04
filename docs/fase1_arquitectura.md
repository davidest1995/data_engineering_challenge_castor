# Fase 1 — Arquitectura del Data Lake

## 1. Capas del Data Lake

### Bronze
Capa de aterrizaje. Los datos llegan tal como los produce la fuente, sin ninguna transformación.

- **Propósito:** fuente de verdad inmutable. Si cualquier proceso downstream falla, se puede reprocesar desde aquí sin acudir a la fuente original.
- **Formato:** Delta Lake (Managed Table en Unity Catalog). Se agrega la columna `fecha_ingesta` para trazabilidad de la carga.
- **Controles mínimos:** log de ingesta (timestamp, fuente, número de registros), alertas por volumen anómalo.

### Silver
Capa de datos limpios, tipados y deduplicados. Representa la versión canónica y confiable de cada entidad de negocio.

Transformaciones aplicadas en este proyecto:

| Campo | Transformación |
|---|---|
| `monto` | Eliminar `$`, `,` y espacios; castear a `DoubleType` |
| `fecha_transaccion` | Derivada de `timestamp_unix` vía `from_unixtime` |
| `fecha_registro` | Normalización de 3 formatos distintos con `try_to_date` + `coalesce` |
| `id_usuario` (usuarios) | Deduplicación por `row_number()` sobre ventana por `id_usuario` |
| Nulos en `pais` | Imputación con `'Desconocido'` |
| Nulos en `edad` | Imputación con mediana calculada sobre el dataset |
| Nulos en `categoria` | Imputación con `'Sin categoría'` |

- **Formato:** Delta Lake. Las columnas se sobrescriben con su versión limpia; no se crean columnas auxiliares con sufijos `_clean` o `_raw`. Si se necesita la versión original, existe Bronze.
- **Controles de calidad:** `id_usuario` e `id_transaccion` no nulos, `monto > 0`, `fecha_transaccion` dentro de rango razonable.

### Gold
Capa analítica. Datamarts orientados a responder preguntas de negocio específicas.

| Tabla | Descripción |
|---|---|
| `gld_customer_360` | Vista 360 por cliente: métricas RFM, segmentación demográfica, categoría favorita y perfil de riesgo por detección de anomalías |
| `gld_cohort_retention` | Análisis de cohortes por mes de primera compra. Tasa de retención y revenue por periodo |

- **Acceso:** solo lectura para perfiles de consumo (analistas, herramientas de BI). Las capas Bronze y Silver son inaccesibles para usuarios no técnicos (principio de menor privilegio).

---

## 2. Estrategia de ingesta

**Para la base transaccional SQL (evitar impacto en producción):**

- Lectura siempre sobre réplica de lectura (Read Replica), nunca sobre el primario.
- Change Data Capture (CDC) con Debezium o equivalente nativo del motor SQL, capturando únicamente los deltas desde la última ejecución. Se elimina el `SELECT *` completo en producción.
- En caso de requerimiento de baja latencia: Kafka + Spark Structured Streaming con watermark para manejar eventos tardíos.
- Horario de ejecución batch fuera de pico cuando la latencia sea tolerada.

**Particionamiento:**

- `gld_cohort_retention` particionada por `cohort_month`: las queries de retención siempre filtran por cohorte, el partition pruning reduce el escaneo de forma significativa.
- Se evita el particionamiento por columnas de alta cardinalidad (ej. `id_usuario`) a nivel de archivo; para eso se usa `ZORDER BY` en Delta.
- Tamaño objetivo por partición: 128 MB – 1 GB para evitar el "small file problem".

---

## 3. Evolución del esquema JSON

Cuando la API de terceros agrega o elimina campos:

- **`mergeSchema = true` en Delta Lake:** acepta columnas nuevas automáticamente. Los campos eliminados quedan como `null` en los nuevos registros sin romper el pipeline.
- **Schema registry** (Confluent o AWS Glue): versiona el esquema formalmente y valida compatibilidad antes de procesar.
- **Modo defensivo en Spark:** lectura con `PERMISSIVE` + columna `_corrupt_record` para capturar registros malformados sin detener el job.
- **Alertas de schema drift:** comparar el esquema inferido de cada ingesta contra el esquema registrado. Si hay campos críticos faltantes, pausar el pipeline y notificar.
