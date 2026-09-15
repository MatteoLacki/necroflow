Czemu zainteresować się Necroflow?

Główny powód to zautomatyzowanie zarządzania zmianami w pajplajnach.
Necroflow to:

- pajplajn = pythonowy skrypt, gdzie ścieżki z wynikami to normalne zmienne, a reguły konsumujące je i produkujące nowe, to zwykłe funkcje. 
    * dodanie nowego kroku to po prostu dodanie kolejnego wywołania funckji tam gdzie trzeba i drobna lokalna zmiana w wykorzystaniu zmiennych.
    * workflow bierze na siebie aktualizację lokalizacji wyniku
        * korzystamy z haszowania proweniencji wyniku, żeby łatwo było śledzić, jak powstał.
        * każdy wynik i tak zawiera pełne ścieżki potrzebnych wyników częściowych
    * koder może korzystać z całego dobrodziejstwa prostego języka programowania, jakim jest python, do kontrolowania jakie wyniki można uzyskać. Obsługujemy wszystkie konstrukty języka, jak pętle czy warunki.
    * sam pajpajn czyta się jak normalny, proceduralny kod.
        * to ma szczególne znaczenie przy recenzowaniu kodu, bo zmiany są zlokalizowane w miejsach wywołania kodu
            * a więc łatwo sprawdzić, czy np. agent AI dobrze zrozumiał nasze intencje
            * albo łatwo napisać logikę pajplajnu samemu i powierzyć innym (także AI) jego implementację. Pipeline to taki C++ header file z intencją, jak rzeczy mają działać.
    
- typowane wyniki:
    * koder decyduje o tym, jakie typy nadać powstającym wynikom
    * framework sprawdza, czy są poprawnie wykorzystane:
        * szybkie raportowanie błędu w fazie kompilacji pajplajna.

Jak to się ma do Nextflow?
Przede wszystkim jesteśmy oczywiście graczem innej wagi: 
- nie zakładamy (na razie) wykorzystania więcej aniżeli jednego VMa/komputera. Nie jesteśmy (na razie) system do koordynowania klastra obliczeń.
- Nextflow zakłada bardzo bogate wykorzystanie konteneryzacji, coś co jest kompletnie prostopadłe do założeń Necroflow. Necroflow nie korzysta z kontenerów w żaden konkretny sposób. W zamierzeniu, kod Necroflow można po prostu zamknąc w kontenerze i tak zarchiwizować. Innym sposobem wykorzystania Necroflow z kontenerami byłoby po prostu zbudowanie reguł korzystających z kontenerów. Wtedy masz połączenie dwóch światów: łączysz wygodę korzystania z necroflow z dobrze (czytaj zewnętrznie) ustawionym kodem o dużej powtarzalności wyników (co do procesora, o ile się w konterach rozeznaję). Tak więc koncept konteryzacji jest jakby zewnętrzny do tego, co robi Necroflow, ale nie wydaje mi się, żeby było to coś kompletnie obcego.
- Necroflow to python a nie Groovy, więc prostsze dla developerów i data-scientistów.
    * jest też prostsze dla AI
        * wygoda definiowania testów
        * wygoda definiowania pipelineów wykorzystujących wiele zbiorów danych lub przeczesywanie wielu zbiorów parametrów (np przy strojenie pipelineów).
- Mała baza kodu (5K linii kodu).

- Minusy w stosunku do Nextflow:
    * No to jest po prostu nowe, więc zostaje się automatycznie early-adapterem.
    * Nie mamy nic na temat HPC ani cloud computing.

Plany na najbliższą przyszłość:
- właśnie piszę publikację
- resuscytacja textual-based managera procesów:
    * jak teraz puścisz Necroflow, to nie zobaczysz stdout z poszczególnych reguł: to nie ma sensu, bo kilka może na raz działać. Zawsze można podglądnąć co tam piszą do plików za pomocą `tail`a, ale to nie to samo, co jednak jakiś manager okienek. Prosta rzecz dla AI, ale muszę jeszcze mieć moment, żeby to skonkretyzować.
- przyglądnięcie się HPC: mam tu lokalnie gdzie mieszkam niedaleko superkomputer ze SLURMem, to sprawdzę jeszcze, jak to się ma do tego ,co już mam.
