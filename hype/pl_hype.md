# Czemu Necroflow?

Piszesz logikę w Pythonie. Framework bierze na siebie cache, DAG i genealogię ścieżek.

## Pajplajn to zwykły skrypt pythonowy

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

- Wyniki to zwykłe zmienne, reguły to zwykłe funkcje. Dodanie kroku = dodanie wywołania
  i drobna lokalna zmiana w użyciu zmiennych — bez przepisywania nazw plików.
- Pełny Python w fazie budowy grafu: pętle, warunki, funkcje, testy pytestem.
  Podpajplajny (`P.subpipeline`) to zwykłe fabryki, a wspólne prefiksy liczą się raz.
- Czyta się to jak normalny kod proceduralny. To ma znaczenie przy recenzji: zmiany są
  zlokalizowane w miejscach wywołania, więc łatwo sprawdzić, czy agent AI dobrze zrozumiał
  intencję — albo napisać samemu logikę i powierzyć implementację reguł komuś innemu.
  Pajplajn to taki nagłówek C++ z intencją, jak rzeczy mają działać.

## Ścieżki generuje framework

- Wynik ląduje w `nodes/{reguła}/{provenance_hash}/{plik}`. Hash (fingerprint v4) obejmuje
  strukturę przepisu, config, shell i pełną linię rodziców.
- Identyczna praca zbiega się do jednego katalogu — także pomiędzy niezależnymi pajplajnami
  w jednym DAGu. Nie projektujesz taksonomii nazw plików ani wildcardów.
- Obok wyniku leży `.rip/`: `dependencies.toml` (linia + SHA-256 skonsumowanych rodziców),
  `graph.tgf` (DAG przodków), `job.log`, `state`, `run.toml` (czasy, rozmiary).
  Genealogia ścieżek podróżuje z plikiem, nie siedzi w centralnej bazie.
- To, co chcesz oglądać, dostajesz jako kopie w `results/<job>/` pod czytelnymi etykietami,
  plus `manifest.toml` (ścieżka widoczna, węzeł źródłowy, hash treści). Kopie przez
  reflink/CoW tam, gdzie system plików pozwala.

## Cacheujemy po zawartości, nie czasie

- Przebudowa rodzica z identycznymi bajtami **nie** unieważnia konsumentów: porównujemy
  SHA-256, mtime służy tylko do unieważnienia szybkiej ścieżki.
- Stan w plikach tekstowych, żadnej bazy danych. Zwisły `running` po crashu wymusza rerun.
- `necroflow explain job.toml` mówi, co się puści i dlaczego (per węzeł);
  `doctor` robi preflight ze stabilnymi kodami `NF_*`; `gc` sprząta node store;
  `graph --json` / `outputs --json` / `provenance --json` dla narzędzi i agentów.

## Typowane wyniki

- `NodeType` to typ pliku. Ty decydujesz o hierarchii; podtypy i unie (alternatywy) typów działają jako
  kontrakty formatu, a `filename = None` daje kontrakt abstrakcyjny (tylko wejściowy).
- Framework sprawdza kompozycję w trakcie budowy pajplajnu — błąd leci zanim cokolwiek
  kosztownego ruszy.

## Uruchamianie

- Jeden `job.toml` opisuje przebieg; `__grid` rozwija siatki parametrów (strojenie,
  wiele zbiorów danych) na osobne joby z deterministycznymi etykietami.
- Egzekutor lokalny i równoległy, z limitami zasobów (`threads`, `ram`, własne),
  protokołem schedulera (domyślnie FIFO), `--dry-run`, `--keep-going`, `repeat=N`
  i `autoclean` dla wyników pośrednich.
- `RuleCall` jest atomowy: wszystkie współwyjścia jednego wywołania cache'ują się
  i lecą razem. Exit 0 bez zadeklarowanego pliku to błąd, nie sukces.

## Jak to się ma do Nextflow (i Snakemake)?

Inna waga i, co ważniejsze, inna granica abstrakcji:

- Jedna maszyna. Nie koordynujemy klastra — na razie.
- Kontenery są prostopadłe: Necroflow ich nie używa w żaden szczególny sposób. Możesz
  zamknąć cały projekt w obrazie, albo oprzeć pojedyncze reguły o `docker run` i połączyć
  wygodę Necroflow z powtarzalnością środowiska ustawionego zewnętrznie.
- Python zamiast Groovy czy DSL-a z wildcardami: prościej dla developerów, data scientistów
  i dla AI. Testy i pajplajny wielo-zbiorowe pisze się zwykłymi środkami.
- Snakemake to najbliższy punkt odniesienia i jest dojrzały. Różnica nie polega na tym,
  że tam się nie da — tylko na tym, kto utrzymuje taksonomię ścieżek i ograniczenia
  wildcardów, gdy wariantów przybywa.
- Prefect pokazuje, że dynamiczna orkiestracja w Pythonie działa, ale generyczne taski nie
  dają typowanych plików wynikowych ani ścieżek wyprowadzonych z linii pochodzenia.
- Mała baza kodu: ~5.6 tys. linii w `src/`.

Minusy: to jest nowe, więc automatycznie zostajesz early adopterem; nie ma HPC ani chmury;
nie ma ekosystemu wtyczek.

## Plany na najbliższą przyszłość

- Publikacja — właśnie piszę.
- Reanimacja textualowego managera procesów. Teraz stdout reguł idzie do `.rip/job.log`
  (bo kilka reguł działa naraz), `tail` działa, ale menedżer okienek to jednak nie to samo.
- Rozpoznanie SLURMa — mam superkomputer niedaleko, to sprawdzę, jak się to ma do tego,
  co już jest.
