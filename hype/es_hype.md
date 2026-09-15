# ¿Por qué Necroflow?

La lógica la escribes en Python. El framework se encarga de la caché, del DAG y de la genealogía
de las rutas.

## Un pipeline es un script de Python corriente

```python
@command("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
def process_text(raw_text: RawText, tool_config: ToolConfig):
    processed_text = output(ProcessedText)
    return processed_text

def my_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = import_text(P, path=config["input"])
    P.processed = process_text(P, P.raw, P.tool_config)
    P.summary = summarize(P, P.processed)
```

- Los resultados son variables corrientes y las reglas, funciones corrientes. Añadir un paso =
  añadir una llamada más un pequeño cambio local en el uso de las variables — sin reescribir
  nombres de ficheros.
- Python entero durante la construcción del grafo: bucles, condicionales, funciones, tests con
  pytest. Los subpipelines (`P.subpipeline`) son simples factorías, y los prefijos compartidos se
  calculan una sola vez.
- Se lee como código procedural normal. Eso importa en la revisión: los cambios quedan
  localizados en los puntos de llamada, así que es fácil comprobar si un agente de IA entendió la
  intención — o escribir uno mismo la lógica y encargar a otros la implementación de las reglas.
  Un pipeline es una especie de cabecera de C++ que declara cómo deben funcionar las cosas.

## Las rutas las genera el framework

- Un resultado acaba en `nodes/{regla}/{provenance_hash}/{fichero}`. El hash (fingerprint v4)
  abarca la estructura de la receta, la config, la shell y toda la línea de los padres.
- Un trabajo idéntico converge en un único directorio — también entre pipelines independientes
  dentro del mismo DAG. No diseñas ninguna taxonomía de nombres de fichero ni wildcards.
- Junto al resultado está `.rip/`: `dependencies.toml` (línea + SHA-256 de los padres
  consumidos), `graph.tgf` (DAG de antepasados), `job.log`, `state`, `run.toml` (tiempos,
  tamaños). La genealogía de las rutas viaja con el fichero en lugar de residir en una base de
  datos central.
- Lo que quieras mirar lo recibes como copias en `results/<job>/` con etiquetas legibles, más
  `manifest.toml` (ruta visible, nodo de origen, hash del contenido). Las copias usan reflink/CoW
  allí donde el sistema de ficheros lo permite.

## Cacheamos por contenido, no por fecha

- Reconstruir un padre con bytes idénticos **no** invalida a los consumidores: comparamos
  SHA-256, y la mtime solo sirve para invalidar la vía rápida.
- El estado vive en ficheros de texto, sin base de datos. Un `running` colgado tras un crash
  fuerza una reejecución.
- `necroflow explain job.toml` dice qué se ejecutaría y por qué (por nodo); `doctor` hace
  comprobaciones previas con códigos `NF_*` estables; `gc` limpia el node store;
  `graph --json` / `outputs --json` / `provenance --json` están para herramientas y agentes.

## Resultados tipados

- Un `NodeType` es un tipo de fichero. La jerarquía la decides tú; los subtipos y las uniones
  (alternativas) de tipos funcionan como contratos de formato, y `filename = None` da un contrato
  abstracto, solo de entrada.
- El framework comprueba la composición mientras se construye el pipeline — el error salta antes
  de que arranque nada caro.

## La ejecución

- Un único `job.toml` describe la ejecución; `__grid` expande rejillas de parámetros (ajuste,
  muchos conjuntos de datos) en trabajos separados con etiquetas deterministas.
- Un ejecutor local y paralelo, con topes de recursos (`threads`, `ram`, los tuyos propios), un
  protocolo de planificador (FIFO por defecto), `--dry-run`, `--keep-going`, `repeat=N` y
  `autoclean` para los resultados intermedios.
- Un `RuleCall` es atómico: todas las cosalidas de una llamada se cachean y se ejecutan juntas.
  Un exit 0 con un fichero declarado ausente es un fallo, no un éxito.

## ¿Cómo se sitúa frente a Nextflow (y Snakemake)?

Otra categoría de peso y, sobre todo, otra frontera de abstracción:

- Una sola máquina. No coordinamos un clúster — por ahora.
- Los contenedores son ortogonales: Necroflow no los usa de ninguna manera particular. Puedes
  encerrar el proyecto entero en una imagen, o basar reglas concretas en `docker run` y unir la
  comodidad de Necroflow con la reproducibilidad de un entorno configurado desde fuera.
- Python en vez de Groovy o de un DSL con wildcards: más simple para desarrolladores, para data
  scientists y para la IA. Los tests y los pipelines multiconjunto se escriben con los medios de
  siempre.
- Snakemake es el punto de referencia más cercano y es maduro. La diferencia no es que allí no se
  pueda — es quién mantiene la taxonomía de rutas y las restricciones de wildcards según se
  acumulan las variantes.
- Prefect demuestra que la orquestación dinámica en Python funciona, pero unas tareas genéricas no
  dan ni ficheros de resultado tipados ni rutas derivadas de la línea de procedencia.
- Base de código pequeña: ~5,6 mil líneas en `src/`.

Inconvenientes: esto es nuevo, así que te conviertes automáticamente en early adopter; no hay HPC
ni nube; no hay ecosistema de plugins.

## Planes para el futuro próximo

- Una publicación — la estoy escribiendo ahora.
- Reanimar el gestor de procesos basado en Textual. Ahora mismo el stdout de las reglas va a
  `.rip/job.log` (porque varias reglas corren a la vez); `tail` funciona, pero un gestor de
  ventanas no es lo mismo.
- Reconocimiento de SLURM — tengo un supercomputador cerca de donde vivo, así que comprobaré cómo
  se relaciona con lo que ya hay.
