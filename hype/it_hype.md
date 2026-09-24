# Perché Necroflow?

La logica la scrivi in Python. Il framework si occupa della cache, del DAG e della genealogia
dei percorsi.

## Una pipeline è un normale script Python

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

- I risultati sono normali variabili, le regole normali funzioni. Aggiungere uno step = aggiungere
  una chiamata più una piccola modifica locale nell'uso delle variabili — senza riscrivere i nomi
  dei file.
- Python per intero durante la costruzione del grafo: cicli, condizioni, funzioni, test con
  pytest. Le sotto-pipeline (`P.subpipeline`) sono semplici workflow, e i prefissi condivisi si
  calcolano una volta sola.
- Si legge come normale codice procedurale. Conta in fase di revisione: le modifiche restano
  localizzate nei punti di chiamata, quindi è facile verificare se un agente IA ha capito
  l'intenzione — oppure scrivere da sé la logica e affidare ad altri l'implementazione delle
  regole. Una pipeline è una specie di header C++ che dichiara come le cose devono funzionare.

## I percorsi li genera il framework

- Un risultato finisce in `nodes/{regola}/{provenance_hash}/{file}`. L'hash (fingerprint v4)
  copre la struttura della ricetta, la config, la shell e l'intera linea dei genitori.
- Lavoro identico converge in un'unica directory — anche tra pipeline indipendenti all'interno
  dello stesso DAG. Non progetti nessuna tassonomia di nomi di file né wildcard.
- Accanto al risultato sta `.rip/`: `dependencies.toml` (linea + SHA-256 dei genitori
  consumati), `graph.tgf` (DAG degli antenati), `job.log`, `state`, `run.toml` (tempi,
  dimensioni). La genealogia dei percorsi viaggia con il file invece di stare in un database
  centrale.
- Quello che vuoi guardare lo ottieni come copie in `results/<job>/` con etichette leggibili,
  più `manifest.toml` (percorso visibile, nodo di origine, hash del contenuto). Le copie usano
  reflink/CoW ovunque il filesystem lo consenta.

## Mettiamo in cache per contenuto, non per data

- Ricostruire un genitore con byte identici **non** invalida i consumatori: confrontiamo SHA-256,
  e la mtime serve solo a invalidare il percorso veloce.
- Lo stato vive in file di testo, nessun database. Un `running` rimasto appeso dopo un crash
  forza una riesecuzione.
- `necroflow explain job.toml` dice cosa verrebbe eseguito e perché (per nodo); `doctor` fa
  controlli preliminari con codici `NF_*` stabili; `gc` pulisce il node store;
  `graph --json` / `outputs --json` / `provenance --json` ci sono per strumenti e agenti.

## Risultati tipizzati

- Un `NodeType` è un tipo di file. La gerarchia la decidi tu; sottotipi e unioni (alternative)
  di tipi fanno da contratti di formato, e `filename = None` dà un contratto astratto, di solo
  ingresso.
- Il framework controlla la composizione mentre la pipeline viene costruita — l'errore arriva
  prima che parta qualsiasi cosa costosa.

## L'esecuzione

- Un unico `job.toml` descrive l'esecuzione; `__grid` espande le griglie di parametri (tuning,
  molti dataset) in job separati con etichette deterministiche.
- Un executor locale e parallelo, con tetti di risorse (`threads`, `ram`, e i tuoi), un
  protocollo di scheduler (FIFO di default), `--dry-run`, `--keep-going`, `repeat=N` e
  `autoclean` per i risultati intermedi.
- Un `RuleCall` è atomico: tutti i co-output di una chiamata vengono messi in cache ed eseguiti
  insieme. Un exit 0 con un file dichiarato mancante è un errore, non un successo.

## Come si colloca rispetto a Nextflow (e Snakemake)?

Un'altra categoria di peso e, cosa più importante, un altro confine di astrazione:

- Una macchina sola. Non coordiniamo un cluster — per ora.
- I container sono ortogonali: Necroflow non li usa in alcun modo particolare. Puoi chiudere
  l'intero progetto in un'immagine, oppure basare singole regole su `docker run` e unire la
  comodità di Necroflow alla riproducibilità di un ambiente configurato dall'esterno.
- Python invece di Groovy o di un DSL a wildcard: più semplice per gli sviluppatori, per i data
  scientist e per l'IA. Test e pipeline multi-dataset si scrivono con i mezzi consueti.
- Snakemake è il punto di riferimento più vicino ed è maturo. La differenza non è che lì non si
  possa fare — riguarda chi mantiene la tassonomia dei percorsi e i vincoli sulle wildcard man
  mano che le varianti si accumulano.
- Prefect dimostra che l'orchestrazione dinamica in Python funziona, ma i task generici non danno
  né file di risultato tipizzati né percorsi derivati dalla linea di provenienza.
- Base di codice piccola: ~5,6 mila righe in `src/`.

Contro: è nuovo, quindi diventi automaticamente un early adopter; non c'è HPC né cloud; non c'è
un ecosistema di plugin.

## Piani per il futuro prossimo

- Una pubblicazione — la sto scrivendo adesso.
- Rianimazione del process manager basato su Textual. Ora lo stdout delle regole finisce in
  `.rip/job.log` (perché più regole girano insieme); `tail` funziona, ma un gestore di finestre
  è un'altra cosa.
- Ricognizione su SLURM — ho un supercomputer poco lontano da dove vivo, quindi verificherò come
  si rapporta a ciò che già c'è.
