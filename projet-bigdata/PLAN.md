# Plan du projet Framework Big Data (échéance : vendredi 4 décembre 2026)

Équipe : Ruben COMBE-TAMAIN, Antoine REVERSAT, Edwin DREAN-AVRILLON.

> À vérifier par écrit auprès de l'enseignant avant de s'y fier : le sujet parle de « both team members » et le format de l'objet de l'email ne contient que deux noms. Demandez confirmation que le trinôme est accepté et comment écrire l'objet.

## 1. Choix du projet (décidé pour vous)

**Jeu de données : OpenFlights** (aéroports, compagnies, routes aériennes mondiales).

| Fichier | Lignes | Taille |
| --- | --- | --- |
| `airports.dat` | 7 698 | 1 127 225 o |
| `airlines.dat` | 6 162 | 396 896 o |
| `routes.dat` | 67 663 | 2 377 148 o |
| **Total** | **81 523** | **3 901 269 o (≈ 3,9 Mo)** |

- Source : `https://raw.githubusercontent.com/jpatokal/openflights/master/data/` (page : openflights.org/data.php).
- Licence : Open Database License (ODbL), contenu sous Database Contents License. À citer dans le README.
- Taille : entre 1 et 5 Mo, donc pas de pénalité. Pas utilisé dans les modules du cours.
- À noter dans le README : date de téléchargement (le jour où vous lancez l'ingestion).

**Question (3 phrases max) :** Quels aéroports sont les principaux hubs mondiaux, mesurés par leur nombre de routes sortantes et par le nombre de compagnies qui les desservent ? Quelle part des routes est inexploitable à cause de références invalides, et de combien le nettoyage change-t-il le classement ? Le résultat est la table finale `gold.hubs`.

**Pourquoi ce jeu rapporte des points :**
- Défauts de qualité réels et chiffrés (mesurés avant le plan, voir §3).
- Trois tables à joindre : Spark et Trino ont du sens.
- Un résultat vérifiable de façon indépendante (comptage des routes par aéroport).

## 2. Pipeline (à reproduire dans les 3 namespaces)

```
GitHub (raw .dat)  ->  S3 raw  ->  Spark (nettoyage) -> S3 silver (Iceberg/Parquet)  ->  gold.hubs  ->  vérification indépendante (DuckDB)
```

Chemins S3 (bucket = votre namespace `user-<identifiant>`) :
- brut : `s3://<bucket>/openflights/raw/{airports,airlines,routes}.dat`
- nettoyé : `s3://<bucket>/openflights/silver/`
- final : `s3://<bucket>/openflights/gold/hubs/`

Composants (une trace d'exécution par composant dans le rapport) : S3, Spark (silver), Trino ou Spark SQL (gold), DuckDB (vérification).

## 3. Défauts de qualité réels (à citer dans le rapport, déjà mesurés)

| Constat | Lignes touchées |
| --- | --- |
| Aéroports sans code IATA (`\N`) | 1 626 sur 7 698 |
| Aéroports sans fuseau horaire (`\N`) | 1 021 |
| Routes avec aéroport de départ inconnu (`\N`) | 220 |
| Routes avec aéroport d'arrivée inconnu (`\N`) | 221 |
| Routes dont l'id de départ n'existe pas dans `airports` | 263 |
| Routes dont l'id d'arrivée n'existe pas dans `airports` | 267 |
| Routes sans compagnie (`\N`) | 479 |
| Compagnies marquées inactives | 4 906 sur 6 162 |
| Routes en partage de code (`codeshare = Y`) | 14 597 |
| Doublons exacts de routes | 0 |

Traitement à décrire : remplacer `\N` par `NULL`, écarter ou isoler (table de rejets) les routes à références invalides, compter les lignes avant et après. Ces chiffres sont à **recompter dans votre exécution** et à montrer en capture.

## 4. Répartition (chacun commite ses parties depuis son compte Git)

| Qui | Parties |
| --- | --- |
| Ruben | Dépôt, README, ingestion S3, silver Spark |
| Antoine | Table `gold.hubs`, mesures de comparaison (Spark / Trino / DuckDB) |
| Edwin | Vérification indépendante (DuckDB), qualité des données, schéma du pipeline |

Chacun reproduit le README dans **son propre namespace** à la fin (preuve de reproductibilité, une capture par personne avec son identifiant).

## 5. Calendrier

Aujourd'hui : vendredi 13 novembre. Le cluster a été saturé en CPU (pods en attente) : **lancez les étapes lourdes tôt, évitez les veilles de rendu**.

| Date | Qui | Tâche | Commits |
| --- | --- | --- | --- |
| Ven 13 – Sam 14 nov | Ruben | Créer le dépôt public `bigdata_project`, ajouter Antoine et Edwin en collaborateurs, squelette `README.md`, `evidence/`, `src/`, `k8s/` | 2 |
| Ven 13 – Sam 14 nov | Antoine, Edwin | Onyxia : Compte > Git (nom, email, jeton GitHub), ouvrir l'explorateur de fichiers pour créer le bucket | 0 |
| Lun 16 nov | Ruben | Ingestion vers S3 (§6.1) + captures 01 à 03 | 1 |
| Mer 18 nov | Ruben | Silver Spark (§6.2) + captures 04 à 05 | 1 |
| Ven 20 nov | Antoine | Gold `hubs` (§6.3) + capture 06 | 1 |
| Lun 23 nov | Antoine | Mesures de comparaison (§6.4) + captures 07 à 08 | 1 |
| Mer 25 nov | Edwin | Vérification indépendante et qualité (§6.5) + captures 09 à 10 | 2 |
| Ven 27 nov | Tous | Chacun refait le README de zéro dans son namespace + capture 11 par personne | 3 |
| Sam 28 – Lun 30 nov | Tous | Rapport 4 pages (§8) | 2 |
| Mar 1 déc | Tous | Relecture, vérification de la checklist §9 | 1 |
| Mer 2 déc | Ruben | Envoi de l'email (marge de 2 jours) | 0 |

Total : 14 commits sur 3 semaines, avec les trois auteurs.

## 6. Commandes (les scripts `src/` sont à créer avant : voir §10)

Avant chaque commande à capturer, affichez l'identifiant et l'heure pour qu'ils soient dans la capture :

```bash
stamp() { echo "$KUBERNETES_NAMESPACE | $(date '+%F %T')"; }
stamp; echo "bucket: $KUBERNETES_NAMESPACE"
```

### 6.1 Ingestion (Ruben)
```bash
git clone https://github.com/<compte>/bigdata_project.git && cd bigdata_project
stamp; bash src/01_ingest.sh          # télécharge les 3 fichiers, vérifie la taille, envoie sur S3
stamp; aws s3 --profile default ls s3://$KUBERNETES_NAMESPACE/openflights/raw/ --human-readable
```

### 6.2 Silver (Ruben)
```bash
stamp; bash src/02_silver.sh          # Spark : nettoyage, table de rejets, comptes avant/après
stamp; aws s3 --profile default ls s3://$KUBERNETES_NAMESPACE/openflights/silver/ --recursive | head
```

### 6.3 Gold (Antoine)
```bash
stamp; bash src/03_gold.sh            # construit gold.hubs
stamp; aws s3 --profile default ls s3://$KUBERNETES_NAMESPACE/openflights/gold/hubs/ --recursive
```

### 6.4 Mesures (Antoine)
```bash
stamp; bash src/04_benchmark.sh       # même requête gold, 3 exécutions par moteur, médiane
```
Alternatives à justifier par vos mesures (tableau dans le rapport) : CSV contre Parquet (taille et temps), Spark contre Trino contre DuckDB (temps de la même agrégation).

### 6.5 Vérification indépendante (Edwin)
```bash
stamp; bash src/05_verify.sh          # DuckDB lit les CSV bruts, recalcule le top 10 des hubs, compare à gold.hubs
```
Résultat attendu à confirmer : les premiers hubs sortants sur les données brutes sont ATL (915), ORD (558), PEK (535), LHR (527), CDG (524) ; le script doit montrer l'écart éventuel avec gold après nettoyage.

## 7. Captures à numéroter (dossier `evidence/`)

Chaque capture montre l'identifiant (namespace), la date et l'heure. Sur téléphone, l'heure de la barre d'état suffit pour les pages web d'Onyxia ; dans le terminal, c'est la ligne `stamp`.

| N° | Contenu |
| --- | --- |
| 01 | Onyxia, Stockage de données : bucket avec dossier `openflights/raw/` |
| 02 | Terminal : `stamp` + `aws s3 ls` du brut avec les tailles |
| 03 | Terminal : sortie de l'ingestion (3 fichiers, taille totale) |
| 04 | Terminal : trace Spark du nettoyage (comptes avant/après) |
| 05 | Terminal : `aws s3 ls` du silver |
| 06 | Terminal : trace du gold et listing S3 du gold |
| 07 | Terminal : mesures Spark / Trino / DuckDB |
| 08 | Onyxia ou terminal : `kubectl get pods` (composants utilisés) |
| 09 | Terminal : vérification DuckDB et comparaison avec gold |
| 10 | Terminal : comptes de défauts de qualité |
| 11 | Une par personne : reproduction du README dans son namespace |

## 8. Plan du rapport (4 pages maximum, annexes comprises)

1. Question, jeu de données (source, licence, taille, schéma des 3 tables) : ½ page.
2. Schéma du pipeline et choix techniques, alternatives écartées avec vos mesures : 1 page.
3. Défauts de qualité (tableau §3 avec vos comptes) et traitements : ½ page.
4. Résultats (top hubs) et limites : ½ page.
5. Et avec 100 fois plus de données (≈ 390 Mo) : ½ page. Idées : le jeu tient encore sur un seul nœud, DuckDB suffirait ; avec Spark il faut partitionner (par pays), éviter les petits fichiers ; Trino a besoin de plusieurs workers.
6. Preuves : captures numérotées, référencées dans le texte, lien du dépôt : 1 page.

Format PDF, lien vers le dépôt public dans le rapport. Pour la mise en page, le modèle LaTeX ECE du TP peut servir.

## 9. Checklist avant l'envoi

- [ ] Dépôt **public**, au moins 10 commits sur plusieurs jours, des trois comptes.
- [ ] Arborescence : `README.md`, `evidence/`, `src/`, `k8s/`.
- [ ] README tenant sur une page, dans l'ordre : auteurs, prérequis, jeu de données (nom, URL, licence, date de téléchargement, taille), question en 3 phrases, chemins S3, procédure de reproduction.
- [ ] Données brutes entre 1 et 5 Mo.
- [ ] Chaque capture montre namespace et date/heure.
- [ ] Rapport PDF de 4 pages maximum, annexes comprises.
- [ ] Objet de l'email : `Framework Big Data - <Gr..> - <NOM1 Prénom1> - <NOM2 Prénom2>` (à confirmer pour le trinôme).
- [ ] Jeton GitHub révoqué après usage.

## 10. Prochaine étape

Les scripts `src/01` à `src/05` et le README ne sont pas encore écrits. Une fois ce plan validé, ils seront préparés et testés sur une copie locale (hors Onyxia) avant que vous ne les lanciez : vous n'aurez qu'à exécuter les commandes ci-dessus aux dates prévues.
