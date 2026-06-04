# Fase 4 — Liderazgo Técnico y Mentoring

## Contexto

Un ingeniero junior entrega un script Python que procesa 100 registros con un bucle `for` y transformaciones campo a campo. El script funciona correctamente. El problema es que no escala: en producción con millones de registros, el rendimiento sería inaceptable.

---

## Enfoque de Code Review

La revisión parte de reconocer que el código funciona y cumple el objetivo funcional. No se trata de señalar un error, sino de una conversación sobre escala. La estructura es la siguiente:

**1. Reconocimiento genuino del trabajo:**
> "El script hace exactamente lo que se pedía y está bien estructurado para el volumen actual. Es un buen punto de partida."

**2. Pregunta, no sentencia:**
> "¿Qué crees que pasaría si en vez de 100 registros procesáramos 10 millones?"

Se deja que el Junior identifique el problema por sí mismo. Si no llega, se guía con datos concretos.

**3. Analogía para explicar vectorización:**
Un bucle `for` es como copiar un libro a mano, letra por letra. Pandas o PySpark es como una fotocopiadora industrial: opera sobre el documento completo en paralelo. El resultado es el mismo; la diferencia es de minutos frente a horas.

**4. Demostración en vivo:**
Se refactoriza la misma lógica usando `.str.replace()` y `pd.to_datetime()` o la API de DataFrame de PySpark. Se mide la diferencia con `%%timeit`. Los números hacen el argumento por sí solos.

---

## Plan de mentoría (30 días)

| Semana | Acción | Objetivo |
|---|---|---|
| 1 | Pair Programming: refactorizar el script juntos usando operaciones vectorizadas | Transferencia práctica inmediata sin teoría abstracta |
| 2 | Lectura guiada: capítulos de "Effective Pandas" (Matt Harrison) | Base conceptual de operaciones sobre columnas completas |
| 3 | Mini-proyecto: mismo pipeline en PySpark con dataset de 1 millón de registros generado sintéticamente | Exposición real a escala y familiarización con el DAG de Spark |
| 4 | Code Review inverso: el Junior revisa un PR del Senior y señala oportunidades de mejora | Consolida el aprendizaje y genera ownership sobre las buenas prácticas |

---

## Principios que guían el proceso

- El objetivo no es corregir al Junior, sino ampliar su modelo mental sobre el cómputo distribuido.
- Las métricas de rendimiento reales son más efectivas que cualquier argumentación teórica.
- El Code Review inverso en la semana 4 no es opcional: es el mecanismo que convierte el aprendizaje pasivo en criterio propio.
