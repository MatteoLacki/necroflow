# Pourquoi Necroflow ?

Vous écrivez la logique en Python. Le framework se charge du cache, du DAG et de la généalogie
des chemins.

## Un pipeline est un simple script Python

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

- Les résultats sont des variables ordinaires, les règles des fonctions ordinaires. Ajouter une
  étape = ajouter un appel et faire un petit changement local dans l'usage des variables — sans
  réécrire les noms de fichiers.
- Python au complet pendant la construction du graphe : boucles, conditions, fonctions, tests
  pytest. Les sous-pipelines (`P.subpipeline`) sont de simples fabriques, et les préfixes
  partagés ne sont calculés qu'une fois.
- Cela se lit comme du code procédural normal. C'est important à la relecture : les changements
  restent localisés aux points d'appel, on vérifie donc facilement si un agent IA a bien compris
  l'intention — ou bien on écrit la logique soi-même et on confie l'implémentation des règles à
  quelqu'un d'autre. Un pipeline, c'est une sorte d'en-tête C++ qui déclare comment les choses
  doivent fonctionner.

## C'est le framework qui génère les chemins

- Un résultat atterrit dans `nodes/{règle}/{provenance_hash}/{fichier}`. Le hash (fingerprint v4)
  couvre la structure de la recette, la config, le shell et toute la lignée des parents.
- Un travail identique converge vers un seul répertoire — y compris entre pipelines indépendants
  au sein d'un même DAG. Vous ne concevez ni taxonomie de noms de fichiers, ni wildcards.
- À côté du résultat se trouve `.rip/` : `dependencies.toml` (lignée + SHA-256 des parents
  consommés), `graph.tgf` (DAG des ancêtres), `job.log`, `state`, `run.toml` (durées, tailles).
  La généalogie des chemins voyage avec le fichier au lieu de résider dans une base centrale.
- Ce que vous voulez consulter vous est fourni sous forme de copies dans `results/<job>/` avec
  des étiquettes lisibles, plus `manifest.toml` (chemin visible, nœud d'origine, hash du
  contenu). Les copies utilisent reflink/CoW partout où le système de fichiers le permet.

## On met en cache par contenu, pas par date

- Reconstruire un parent avec des octets identiques n'invalide **pas** les consommateurs : on
  compare des SHA-256, et la mtime ne sert qu'à invalider le chemin rapide.
- L'état vit dans des fichiers texte, sans base de données. Un `running` resté en suspens après
  un crash force une réexécution.
- `necroflow explain job.toml` dit ce qui serait exécuté et pourquoi (par nœud) ; `doctor` fait
  des contrôles préalables avec des codes `NF_*` stables ; `gc` nettoie le node store ;
  `graph --json` / `outputs --json` / `provenance --json` sont là pour les outils et les agents.

## Résultats typés

- Un `NodeType` est un type de fichier. Vous décidez de la hiérarchie ; les sous-types et les
  unions (alternatives) de types font office de contrats de format, et `filename = None` donne
  un contrat abstrait, réservé aux entrées.
- Le framework vérifie la composition pendant la construction du pipeline — l'erreur tombe avant
  que quoi que ce soit de coûteux ne démarre.

## L'exécution

- Un seul `job.toml` décrit l'exécution ; `__grid` développe les grilles de paramètres (réglage,
  plusieurs jeux de données) en jobs distincts avec des étiquettes déterministes.
- Un exécuteur local et parallèle, avec des plafonds de ressources (`threads`, `ram`, et les
  vôtres), un protocole d'ordonnanceur (FIFO par défaut), `--dry-run`, `--keep-going`,
  `repeat=N` et `autoclean` pour les résultats intermédiaires.
- Un `RuleCall` est atomique : toutes les co-sorties d'un appel sont mises en cache et exécutées
  ensemble. Un exit 0 avec un fichier déclaré manquant est un échec, pas un succès.

## Et par rapport à Nextflow (et Snakemake) ?

Une autre catégorie de poids et, surtout, une autre frontière d'abstraction :

- Une seule machine. Nous ne coordonnons pas de cluster — pas encore.
- Les conteneurs sont orthogonaux : Necroflow ne les utilise d'aucune façon particulière. Vous
  pouvez enfermer tout le projet dans une image, ou baser certaines règles sur `docker run` et
  combiner le confort de Necroflow avec la reproductibilité d'un environnement configuré de
  l'extérieur.
- Python plutôt que Groovy ou un DSL à wildcards : plus simple pour les développeurs, pour les
  data scientists et pour l'IA. Les tests et les pipelines multi-jeux-de-données s'écrivent avec
  les moyens habituels.
- Snakemake est le point de comparaison le plus proche, et il est mature. La différence n'est
  pas qu'on n'y arriverait pas — elle porte sur qui maintient la taxonomie des chemins et les
  contraintes de wildcards quand les variantes s'accumulent.
- Prefect montre que l'orchestration dynamique en Python fonctionne, mais des tâches génériques
  ne donnent ni fichiers de résultats typés, ni chemins dérivés de la lignée.
- Une petite base de code : ~5,6 k lignes dans `src/`.

Inconvénients : c'est nouveau, vous devenez donc automatiquement un early adopter ; il n'y a ni
HPC ni cloud ; il n'y a pas d'écosystème de plugins.

## Plans pour le futur proche

- Une publication — en cours de rédaction.
- Réanimation du gestionnaire de processus basé sur Textual. Pour l'instant la sortie standard
  des règles va dans `.rip/job.log` (parce que plusieurs règles tournent en même temps) ; `tail`
  fonctionne, mais un gestionnaire de fenêtres, ce n'est quand même pas pareil.
- Reconnaissance de SLURM — il y a un supercalculateur près de chez moi, je verrai donc comment
  cela se rapporte à l'existant.
