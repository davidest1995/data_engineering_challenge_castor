# Fase 3 — Visualización e Insights

## Propuesta de Dashboard (C-Level)

Pregunta de negocio: *"¿Quiénes son nuestros clientes más valiosos y cómo evolucionan sus compras mes a mes?"*

**Componentes:**
1. KPIs de cabecera: Total revenue, Clientes activos, Ticket promedio, Transacciones totales.
2. Top 10 clientes por gasto total (bar chart horizontal).
3. Evolución de revenue mes a mes (line chart con anotaciones de hitos).
4. Distribución de gasto por categoría (treemap).
5. Tabla de detalle de clientes con filtros dinámicos (país, rango de fecha, categoría).

## Conexión Live vs. Extracto en Tableau

**Elección: Extracto (Extract).**

Para un Data Lake con millones de registros, una conexión Live ejecuta cada interacción del usuario como una query directa al motor, generando latencia inaceptable para dashboards de C-Level. Un extracto de Tableau (.hyper) materializa los datos de la capa Gold (ya agregados) en memoria columnar optimizada, con tiempos de respuesta de milisegundos.

El extracto se refresca programáticamente (Tableau Server / Tableau Cloud) cada vez que el pipeline ETL completa su ejecución Gold — desacoplando la disponibilidad del dashboard del rendimiento del Data Lake.

## LOD FIXED vs. Agregación normal

`FIXED` se usa cuando el nivel de detalle del cálculo debe ser independiente de los filtros o dimensiones del view actual. Ejemplo: calcular el porcentaje que representa cada cliente sobre el **total histórico** (no sobre el subconjunto filtrado). Con una agregación normal, el denominador cambia al aplicar filtros; con `FIXED {id_usuario}: SUM(monto)} / FIXED {}: SUM(monto)` el denominador permanece sobre el universo completo.

## Optimización de Workbook lento

1. Migrar a fuente de Extracto si se usa Live.
2. Reducir el número de marks (puntos) en el view — agregar más en la fuente antes de cargar a Tableau.
3. Eliminar calculated fields redundantes; moverlos a la capa Gold del pipeline.
4. Usar context filters para reducir el espacio de datos antes de aplicar otros filtros.
5. Limitar el número de dashboards en un mismo workbook — separar por audiencia.
