# Automatisation du TP sur Onyxia

`init.sh` est le script d'initialisation du service `vscode-pyspark`. Il telecharge `run.sh` et le lance en arriere-plan.
`run.sh` execute : donnees (lab 03) -> silver Spark (07) -> Polaris (09) -> Trino (10) -> labs Trino 2 et 3,
puis pousse les journaux et les JSON de resultats sur la branche `onyxia-results` (jeton GitHub d'Onyxia requis).
