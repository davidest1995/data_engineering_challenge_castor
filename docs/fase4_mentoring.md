# Fase 4 — Liderazgo Técnico y Mentoring

## Enfoque de Code Review

La revisión se inicia reconociendo que el código **funciona y cumple el objetivo funcional** — ese es un punto de partida sólido. El problema no es la lógica, sino la escala. La conversación se estructura así:

1. **Primero el elogio genuino**: "El script hace exactamente lo que se pedía y está bien estructurado para 100 registros."
2. **Luego la pregunta, no la sentencia**: "¿Qué crees que pasaría si en vez de 100 registros tuviéramos 10 millones?" — dejar que el Junior llegue al problema por sí mismo.
3. **Analogía para explicar vectorización**: Un bucle `for` es como copiar un libro a mano, letra por letra. Pandas/PySpark es como una fotocopiadora: opera sobre todo el documento en paralelo.
4. **Demo en vivo**: mostrar el mismo resultado con `.str.replace()` + `pd.to_datetime()`, medir el tiempo con `%%timeit`. Los números hablan solos.

## Plan de mentoría (30 días)

| Semana | Acción | Objetivo |
|---|---|---|
| 1 | Pair Programming: refactorizar el script juntos usando Pandas vectorizado | Transferencia práctica inmediata |
| 2 | Lectura guiada: capítulos de "Effective Pandas" (Matt Harrison) | Base conceptual de operaciones vectorizadas |
| 3 | Mini-proyecto: mismo pipeline en PySpark con dataset de 1M registros generado sintéticamente | Exposición real a escala |
| 4 | Code Review inverso: el Junior revisa un PR tuyo y señala oportunidades de mejora | Consolida el aprendizaje y genera ownership |
