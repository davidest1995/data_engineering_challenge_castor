# Fase 3 — Visualización e Insights

## Fuentes de datos para el dashboard

Las tablas Gold de este proyecto están diseñadas para ser consumidas directamente por herramientas de BI sin transformaciones adicionales. La lógica de negocio vive en el pipeline ETL, no en el dashboard.

| Tabla Gold | Caso de uso |
|---|---|
| `gld_customer_360` | KPIs ejecutivos, segmentación de clientes, análisis de riesgo, categoría favorita |
| `gld_cohort_retention` | Curvas de retención, heatmap de cohortes, revenue por generación de clientes |
| `gld_customer_monthly_spending` | Evolución mensual del gasto por cliente; responde directamente la pregunta de clientes más valiosos |

---

## Conexión Tableau → Databricks

El tablero utiliza el **conector nativo de Tableau para Databricks** (Spark SQL) en modo **live**, conectado directamente a la tabla `castor.gold.gld_customer_monthly_spending`. Esto garantiza que el dashboard siempre refleja el último estado del pipeline sin necesidad de extraer o exportar datos manualmente.

### Conexión Live vs. Extracto

**Decisión para este proyecto: Conexión Live.**

Para el volumen de datos actual (tabla Gold ya agregada) la conexión live ofrece la ventaja de reflejar inmediatamente cualquier re-ejecución del pipeline. En un entorno de producción con millones de registros, se recomienda migrar a **Extracto (`.hyper`)** programático post-pipeline, lo que materializa los datos en memoria columnar con tiempos de respuesta de milisegundos independientemente del tamaño del Data Lake.

---

## Propuesta de Dashboard (C-Level)

Pregunta de negocio central: *"¿Quiénes son nuestros clientes más valiosos, cómo evolucionan sus compras y cuál es el riesgo operativo asociado?"*

**Componentes propuestos:**

1. **KPIs de cabecera:** Revenue total, clientes activos, ticket promedio, transacciones totales.
2. **Top 10 clientes por gasto total** (bar chart horizontal, filtrable por mes).
3. **Evolución de revenue mes a mes por cliente** (line chart con desglose por `nombre`).
4. **Mapa de calor (Heat Map) cliente × mes:** `nombre` en filas, `ano_mes` en columnas, `gasto_mensual` como color. Permite identificar picos de actividad individuales.
5. **Retention Heatmap:** cuadrícula cohorte × periodo con `retention_rate` como intensidad de color. Identifica en qué mes se produce el mayor drop-off.
6. **Distribución de gasto por categoría** (treemap coloreado por `categoria_favorita`).
7. **Semáforo de perfil de riesgo:** distribución de clientes por `perfil_riesgo` (Normal / Bajo / Medio / Alto).
8. **Tabla de detalle de clientes** con filtros dinámicos por mes y nombre.

---

## Análisis: ¿Quiénes son los clientes más valiosos y cómo evolucionan sus compras mes a mes?

### Fuente de datos
Tabla `castor.gold.gld_customer_monthly_spending`:

| Campo | Uso analítico |
|---|---|
| `nombre` | Identificación del cliente en el eje visual |
| `ano_mes` | Dimensión temporal (eje X en line chart, columnas en heatmap) |
| `gasto_mensual` | Métrica principal de valor |
| `transacciones_del_mes` | Métrica de frecuencia |

### Visualizaciones implementadas en Tableau

**Hoja 1 — Top Clientes por Gasto Total (Bar chart horizontal)**
- Filas: `nombre` | Columnas: `SUM(gasto_mensual)` | Color: `SUM(gasto_mensual)`
- Ordenado descendente. Filtro opcional: Top 10 por `SUM(gasto_mensual)`.

**Hoja 2 — Evolución Mensual (Line chart)**
- Columnas: `ano_mes` | Filas: `SUM(gasto_mensual)` | Color: `nombre`
- Filtro de Top 10 clientes para evitar sobresaturación visual.

**Hoja 3 — Mapa de Calor de Actividad (Square mark)**
- Columnas: `ano_mes` | Filas: `nombre` | Color: `SUM(gasto_mensual)`
- Paleta secuencial azul. Permite identificar qué clientes son activos en qué meses.

### Campos calculados en Tableau

```
Ticket Promedio = SUM([gasto_mensual]) / SUM([transacciones_del_mes])
```

### Segmentación de comportamiento resultante

| Patrón | Señal | Acción recomendada |
|---|---|---|
| **Consistente** | Gasto estable todos los meses | Programa VIP / fidelización |
| **Estacional** | Picos en meses específicos | Campañas anticipadas pre-pico |
| **En declive** | Tendencia descendente sostenida | Acciones de reactivación |
| **Nuevo activo** | Primeros meses con alto gasto | Onboarding premium |

---

## LOD FIXED vs. Agregación estándar en Tableau

`FIXED` se utiliza cuando el nivel de detalle del cálculo debe ser independiente de los filtros o dimensiones activos en el view.

Ejemplo concreto: calcular el porcentaje de revenue que representa cada cliente sobre el **total histórico** (no sobre el subconjunto filtrado). Con una agregación estándar, el denominador cambia al aplicar filtros de fecha. Con `FIXED`:

```
{ FIXED [id_usuario] : SUM([monetary_total]) } / { FIXED : SUM([monetary_total]) }
```

El denominador permanece sobre el universo completo independientemente de los filtros del view.

---

## Optimización de un workbook lento

1. Migrar a Extracto si se usa conexión Live con grandes volúmenes.
2. Reducir el número de marks: agregar más en la fuente (en la capa Gold del pipeline) antes de cargar a Tableau.
3. Mover calculated fields complejos a la capa Gold del pipeline. Tableau no es un motor de transformación.
4. Usar Context Filters para reducir el espacio de datos antes de aplicar otros filtros.
5. Limitar el número de dashboards por workbook. Separar por audiencia (C-Level, Producto, Riesgo).
