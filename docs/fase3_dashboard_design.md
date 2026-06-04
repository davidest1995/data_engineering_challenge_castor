# Fase 3 — Visualización e Insights

## Fuentes de datos para el dashboard

Las tablas Gold de este proyecto están diseñadas para ser consumidas directamente por herramientas de BI sin transformaciones adicionales.

| Tabla Gold | Caso de uso |
|---|---|
| `gld_customer_360` | KPIs ejecutivos, segmentación de clientes, análisis de riesgo |
| `gld_cohort_retention` | Curvas de retención, heatmap de cohortes, revenue por generación de clientes |

---

## Propuesta de Dashboard (C-Level)

Pregunta de negocio central: *"¿Quiénes son nuestros clientes más valiosos, cómo evolucionan sus compras y cuál es el riesgo operativo asociado?"*

**Componentes propuestos:**

1. **KPIs de cabecera:** Revenue total, clientes activos, ticket promedio, transacciones totales.
2. **Top 10 clientes por gasto total** (bar chart horizontal, filtrable por país y categoría).
3. **Evolución de revenue mes a mes** (line chart con desglose por cohorte de adquisición).
4. **Retention Heatmap:** cuadrícula cohorte × periodo con `retention_rate` como intensidad de color. Permite identificar en qué mes se produce el mayor drop-off.
5. **Distribución de gasto por categoría** (treemap coloreado por `categoria_favorita`).
6. **Semáforo de perfil de riesgo:** distribución de clientes por `perfil_riesgo` (Normal / Bajo / Medio / Alto).
7. **Tabla de detalle de clientes** con filtros dinámicos por país, rango de edad y categoría.

---

## Conexión Live vs. Extracto en Tableau

**Decisión: Extracto (Extract).**

Para un Data Lake con millones de registros, una conexión Live ejecuta cada interacción del usuario como una query directa al motor, generando latencia inaceptable en un dashboard ejecutivo. Un extracto `.hyper` materializa los datos de Gold (ya agregados) en memoria columnar, con tiempos de respuesta de milisegundos.

El extracto se refresca de forma programática (Tableau Server / Tableau Cloud) tras la finalización de cada ejecución del pipeline Gold, desacoplando la disponibilidad del dashboard del rendimiento del Data Lake.

---

## LOD FIXED vs. Agregación estándar en Tableau

`FIXED` se utiliza cuando el nivel de detalle del cálculo debe ser independiente de los filtros o dimensiones activos en el view. 

Ejemplo concreto: calcular el porcentaje de revenue que representa cada cliente sobre el **total histórico** (no sobre el subconjunto filtrado). Con una agregación estándar, el denominador cambia al aplicar filtros de fecha o país. Con `FIXED`:

```
{ FIXED [id_usuario] : SUM([monetary_total]) } / { FIXED : SUM([monetary_total]) }
```

El denominador permanece sobre el universo completo independientemente de los filtros del view.

---

## Optimización de un workbook lento

1. Migrar a Extracto si se usa conexión Live.
2. Reducir el número de marks: agregar más en la fuente (en la capa Gold del pipeline) antes de cargar a Tableau.
3. Mover calculated fields complejos a la capa Gold del pipeline. Tableau no es un motor de transformación.
4. Usar Context Filters para reducir el espacio de datos antes de aplicar otros filtros.
5. Limitar el número de dashboards por workbook. Separar por audiencia (C-Level, Producto, Riesgo).
