# Warum Necroflow?

Die Logik schreibst du in Python. Um Cache, DAG und die Genealogie der Pfade kümmert sich das
Framework.

## Eine Pipeline ist ein ganz normales Python-Skript

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

- Ergebnisse sind gewöhnliche Variablen, Regeln gewöhnliche Funktionen. Einen Schritt hinzufügen
  = einen Aufruf hinzufügen plus eine kleine lokale Änderung an der Verwendung der Variablen —
  ohne Dateinamen umzuschreiben.
- Volles Python während des Graphaufbaus: Schleifen, Bedingungen, Funktionen, pytest-Tests.
  Subpipelines (`P.subpipeline`) sind schlichte Fabriken, und gemeinsame Präfixe werden einmal
  berechnet.
- Es liest sich wie normaler prozeduraler Code. Das zählt beim Review: Änderungen bleiben an den
  Aufrufstellen lokalisiert, also lässt sich leicht prüfen, ob ein KI-Agent die Absicht richtig
  verstanden hat — oder man schreibt die Logik selbst und übergibt die Implementierung der Regeln
  an jemand anderen. Eine Pipeline ist so etwas wie ein C++-Header, der die Absicht festhält, wie
  die Dinge funktionieren sollen.

## Die Pfade erzeugt das Framework

- Ein Ergebnis landet in `nodes/{Regel}/{provenance_hash}/{Datei}`. Der Hash (Fingerprint v4)
  umfasst die Rezeptstruktur, die Config, die Shell und die vollständige Elternlinie.
- Identische Arbeit konvergiert in ein einziges Verzeichnis — auch zwischen unabhängigen
  Pipelines innerhalb eines DAG. Du entwirfst weder eine Dateinamen-Taxonomie noch Wildcards.
- Neben dem Ergebnis liegt `.rip/`: `dependencies.toml` (Linie + SHA-256 der konsumierten Eltern),
  `graph.tgf` (Vorfahren-DAG), `job.log`, `state`, `run.toml` (Laufzeiten, Größen). Die Genealogie
  der Pfade reist mit der Datei mit, statt in einer zentralen Datenbank zu sitzen.
- Was du ansehen willst, bekommst du als Kopien unter `results/<job>/` mit lesbaren Labels, dazu
  `manifest.toml` (sichtbarer Pfad, Ursprungsknoten, Inhalts-Hash). Kopien laufen über
  reflink/CoW, wo das Dateisystem es zulässt.

## Wir cachen nach Inhalt, nicht nach Zeit

- Ein Elternteil mit identischen Bytes neu zu bauen invalidiert die Konsumenten **nicht**: wir
  vergleichen SHA-256, mtime invalidiert nur den schnellen Pfad.
- Der Zustand lebt in Textdateien, keine Datenbank. Ein nach einem Absturz hängen gebliebenes
  `running` erzwingt einen Rerun.
- `necroflow explain job.toml` sagt, was laufen würde und warum (pro Knoten); `doctor` macht
  Preflight-Checks mit stabilen `NF_*`-Codes; `gc` räumt den Node Store auf;
  `graph --json` / `outputs --json` / `provenance --json` sind für Werkzeuge und Agenten da.

## Typisierte Ergebnisse

- Ein `NodeType` ist ein Dateityp. Über die Hierarchie entscheidest du; Subtypen und Unions
  (Alternativen) von Typen wirken als Formatverträge, und `filename = None` ergibt einen
  abstrakten, rein eingangsseitigen Vertrag.
- Das Framework prüft die Komposition schon beim Aufbau der Pipeline — der Fehler kommt, bevor
  irgendetwas Teures anläuft.

## Ausführung

- Eine einzige `job.toml` beschreibt den Lauf; `__grid` expandiert Parametergitter (Tuning, viele
  Datensätze) zu getrennten Jobs mit deterministischen Labels.
- Ein lokaler, paralleler Executor mit Ressourcenobergrenzen (`threads`, `ram`, eigene), einem
  Scheduler-Protokoll (standardmäßig FIFO), `--dry-run`, `--keep-going`, `repeat=N` und
  `autoclean` für Zwischenergebnisse.
- Ein `RuleCall` ist atomar: alle Co-Outputs eines Aufrufs werden gemeinsam gecacht und gemeinsam
  ausgeführt. Exit 0 bei fehlender deklarierter Datei ist ein Fehlschlag, kein Erfolg.

## Wie verhält sich das zu Nextflow (und Snakemake)?

Eine andere Gewichtsklasse und, wichtiger noch, eine andere Abstraktionsgrenze:

- Eine Maschine. Wir koordinieren keinen Cluster — vorerst.
- Container sind orthogonal: Necroflow nutzt sie in keiner besonderen Weise. Du kannst das ganze
  Projekt in ein Image einschließen oder einzelne Regeln auf `docker run` stützen und so den
  Komfort von Necroflow mit der Reproduzierbarkeit einer extern eingerichteten Umgebung
  verbinden.
- Python statt Groovy oder einer Wildcard-DSL: einfacher für Entwickler, für Data Scientists und
  für KI. Tests und Pipelines über viele Datensätze schreibt man mit gewöhnlichen Mitteln.
- Snakemake ist der nächstliegende Bezugspunkt und ist ausgereift. Der Unterschied liegt nicht
  darin, dass es dort nicht ginge — sondern darin, wer die Pfad-Taxonomie und die
  Wildcard-Constraints pflegt, wenn sich die Varianten häufen.
- Prefect zeigt, dass dynamische Orchestrierung in Python funktioniert, aber generische Tasks
  liefern weder typisierte Ergebnisdateien noch aus der Abstammung abgeleitete Pfade.
- Kleine Codebasis: ~5,6 Tsd. Zeilen in `src/`.

Nachteile: Das ist neu, also wirst du automatisch zum Early Adopter; es gibt kein HPC und keine
Cloud; es gibt kein Plugin-Ökosystem.

## Pläne für die nähere Zukunft

- Eine Publikation — schreibe ich gerade.
- Wiederbelebung des Textual-basierten Prozessmanagers. Aktuell geht der stdout der Regeln nach
  `.rip/job.log` (weil mehrere Regeln gleichzeitig laufen); `tail` funktioniert, aber ein
  Fenstermanager ist eben doch etwas anderes.
- SLURM sondieren — in meiner Nähe steht ein Supercomputer, also schaue ich mir an, wie sich das
  zu dem verhält, was schon da ist.
