# Fase 1 — Arquitectura del Data Lake

## 1. Capas del Data Lake

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
  - Manejo de nulos.
- Formato: **Delta Lake** — habilita time-travel y schema evolution.
- Controles de calidad:
  - `id_usuario` / `id_transaccion` no nulos y únicos.
  - `monto > 0`.
  - `fecha_transaccion` dentro de rango razonable (no futura, no anterior a 2020).

**Gold / Curated**
- Modelo analítico agregado: tablón `customer_summary` con `total_gastado`, `num_transacciones`, `ticket_promedio`.
- Joins optimizados con broadcast hint cuando una tabla es pequeña (usuarios ~10K registros).
- Particionado por `year` / `month` de la transacción.
- Formato: **Delta Lake** con `OPTIMIZE` y `ZORDER BY id_usuario` para acelerar consultas por cliente.

## 2. Estrategia de ingesta y particionamiento

**Ingesta de base transaccional SQL sin afectar producción:**
- **Change Data Capture (CDC)** con Debezium o la funcionalidad nativa del motor SQL.
- Horario fuera de pico (batch nocturno) o streaming con Kafka → Spark Structured Streaming.
- Conexión mediante réplica de lectura (Read Replica).

**Particionamiento:**
- Capa Gold: `year` / `month` — alineado con las consultas analíticas más frecuentes.
- Evitar particionamiento por columnas de alta cardinalidad en el nivel de archivo (usar `ZORDER BY`).
- Tamaño objetivo por partición: 128 MB – 1 GB.

## 3. Evolución del esquema JSON

Si la API de terceros agrega o elimina campos:
- **Delta Lake con `mergeSchema = true`**: acepta columnas nuevas automáticamente.
- **Schema registry** (Confluent o AWS Glue): versiona el esquema.
- **Estrategia defensiva en código**: lectura con `PERMISSIVE` mode en Spark + columna `_corrupt_record`.
- **Alertas de drift**: comparar esquema inferido vs registrado.
