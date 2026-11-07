---
title: "SQL pour Data Engineers, Blog 5 : performance I, lire un plan d'exécution"
description: "Lire les plans d'exécution SQL pour comprendre et optimiser les performances des requêtes."
weight: 5
---

# SQL pour Data Engineers, Blog 5 : performance I, lire un plan d'exécution

> **Série** : SQL· **Niveau** : 4/5 · **Temps de lecture** : ~40 min (avec les expériences proposées)
> **Moteurs visés** : Hive (Tez), Spark SQL, Impala (stack Cloudera). Les écarts de dialecte et de version sont signalés.
> **Prérequis** : [Blogs 1 à 4](blog-01-sql-bases.md). On y a semé plusieurs « vérifie avec `EXPLAIN` » : ce blog les règle.

> **À lire avant d'aller plus loin : les extraits de plans de ce blog sont SIMPLIFIÉS.** Ils montrent la **forme** d'un plan et les lignes qui comptent, pas une sortie exacte : le texte réel varie selon le moteur, la version et la configuration. Les ordres de grandeur sont donnés à titre d'exemple. **Tout doit être reproduit et mesuré sur ton cluster** (section 1).

---

## 0. Pourquoi ce blog existe

Jusqu'ici, on a écrit du SQL **correct**. Ici, on s'intéresse au SQL **rapide**. Et la première règle de la performance, c'est :

> **On ne devine pas, on mesure.**

La question d'entretien classique : *« Ta requête met 40 minutes sur une table de 2 milliards de lignes. Que fais-tu ? »* Un mauvais candidat répond « j'ajoute des index » ou « j'augmente la mémoire ». Un bon candidat répond par une **méthode** : où passe le temps, que lit-on, que déplace-t-on, que dit le plan, que disent les données.

Le scénario :

> **Lundi, 8h12.** Le job quotidien `rapport_ventes` tournait en 12 minutes il y a six mois. Il en prend maintenant 40. La table `ventes_hist` a grossi (environ 2 milliards de lignes sur 3 ans), mais pas de façon spectaculaire. On te demande : *« Pourquoi, et comment on corrige ? »*

À la fin du blog, tu auras une **démarche de diagnostic** en cinq étapes, la capacité de **lire un plan**, et quatre leviers concrets : **format de fichier, partitionnement, statistiques, petits fichiers**.

### Plan

1. [Le contexte et les tables de laboratoire](#1-le-contexte-et-les-tables-de-laboratoire)
2. [La méthode : mesurer avant de corriger](#2-la-méthode--mesurer-avant-de-corriger)
3. [`EXPLAIN` selon le moteur](#3-explain-selon-le-moteur)
4. [Lire un plan Spark](#4-lire-un-plan-spark)
5. [Lire un plan Hive (Tez) et Impala](#5-lire-un-plan-hive-tez-et-impala)
6. [Les formats de fichiers](#6-les-formats-de-fichiers)
7. [Le partitionnement](#7-le-partitionnement)
8. [Le bucketing](#8-le-bucketing)
9. [Les statistiques et l'optimiseur](#9-les-statistiques-et-loptimiseur)
10. [Le problème des petits fichiers](#10-le-problème-des-petits-fichiers)
11. [Vérifier les promesses des Blogs 1 à 4](#11-vérifier-les-promesses-des-blogs-1-à-4)
12. [Résolution de l'incident](#12-résolution-de-lincident)
13. [10 questions d'entretien](#13-10-questions-dentretien)
14. [5 exercices](#14-5-exercices)
15. [Checklist de performance](#15-checklist-de-performance)
16. [Prochain blog et blogs restants](#16-prochain-blog-et-blogs-restants)

---

## 1. Le contexte et les tables de laboratoire

Les petites tables des blogs précédents (10 ou 13 lignes) ne permettent pas de **voir** un problème de performance. Voici un générateur pour fabriquer une table de **50 millions de lignes** (ajuste le nombre selon ton cluster). **Je n'ai pas pu l'exécuter ici : lance-le et vérifie les comptages.**

### 1.1 Une table Parquet non partitionnée (Spark SQL)

```sql
USE blog_sql;

CREATE TABLE ventes_hist_pq STORED AS PARQUET AS
SELECT
  id                                                          AS vente_id,
  CAST(pmod(id * 7919, 100000) AS INT)                        AS client_id,
  CASE WHEN pmod(id, 3) = 0 THEN 'Audio' ELSE 'Informatique' END AS categorie,
  CAST(10 + pmod(id * 37, 1990) AS DECIMAL(10,2))             AS montant,
  CASE WHEN pmod(id, 10) < 8  THEN 'VALIDEE'
       WHEN pmod(id, 10) = 8  THEN 'ANNULEE'
       ELSE NULL END                                          AS statut,
  date_add(DATE '2022-01-01', CAST(pmod(id, 1096) AS INT))    AS date_vente
FROM range(0, 50000000);
```

Les données couvrent environ trois ans (2022-01-01 à fin 2024). `range(...)` est une fonction de table de Spark SQL ; sur Hive, il faudra un autre générateur (par exemple `posexplode(split(space(n), ' '))` ou un chargement depuis des fichiers).

### 1.2 La même table, partitionnée par mois

```sql
CREATE TABLE ventes_hist_part (
  vente_id   BIGINT,
  client_id  INT,
  categorie  STRING,
  montant    DECIMAL(10,2),
  statut     STRING,
  date_vente DATE
)
PARTITIONED BY (mois STRING)
STORED AS PARQUET;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ventes_hist_part PARTITION (mois)
SELECT vente_id, client_id, categorie, montant, statut, date_vente,
       date_format(date_vente, 'yyyy-MM') AS mois          -- la colonne de partition en DERNIER
FROM ventes_hist_pq;
```

Ce qui donne environ **36 partitions** (un dossier par mois). La colonne de partition doit être **la dernière** du `SELECT`.

### 1.3 La même table, en texte brut

```sql
CREATE TABLE ventes_hist_txt (
  vente_id BIGINT, client_id INT, categorie STRING,
  montant DECIMAL(10,2), statut STRING, date_vente DATE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ventes_hist_txt SELECT * FROM ventes_hist_pq;
```

### 1.4 Ce que ces trois tables vont permettre

| Expérience | Tables comparées | Ce qu'on observe |
|---|---|---|
| Format | `ventes_hist_txt` vs `ventes_hist_pq` | Volume lu, colonnes lues |
| Partitionnement | `ventes_hist_pq` vs `ventes_hist_part` | Partitions lues |
| Statistiques | Avant / après `ANALYZE` | Choix de jointure, estimations |
| Petits fichiers | Avant / après compactage | Nombre de tâches |

> **Dans tout le blog, les chiffres de volume (« 2 milliards de lignes », « 150 Go ») sont des exemples.** Le vrai ordre de grandeur de ton cluster, tu le mesures.

---

## 2. La méthode : mesurer avant de corriger

### 2.1 Les trois premiers réflexes

Face à « ça met 40 minutes » :

**Réflexe 1 : où passe le temps ?**
Ouvre l'interface du moteur : **Spark UI** (onglets *Stages* et *SQL*), **Tez UI** ou **YARN** pour Hive, **profil de requête** pour Impala. Repère l'étape (stage, vertex) qui domine. Ne touche à rien avant de savoir si c'est la **lecture**, le **shuffle**, le **calcul**, l'**écriture**, ou de l'**attente** de ressources.

**Réflexe 2 : que dit le plan ?**
Lance `EXPLAIN` et cherche les **lignes coûteuses** : combien de données sont lues, combien de `Exchange` (shuffles), quelle stratégie de jointure.

**Réflexe 3 : que disent les données ?**
Nombre et taille des fichiers, format, partitions, statistiques, distribution des clés.

### 2.2 Où regarder dans les interfaces

| Signal | Spark UI | Ce que ça révèle |
|---|---|---|
| Volume lu (*Input Size / Records*) | Stage, onglet détail | La lecture est-elle disproportionnée par rapport au résultat ? |
| *Shuffle Read / Write* | Stage | Le volume de données déplacé entre machines |
| *Spill (memory / disk)* | Stage | Manque de mémoire : les données débordent sur disque |
| Tâches : *Min / 25 % / Median / 75 % / Max* | Stage, « Summary Metrics » | **Skew** : si le Max est très supérieur à la médiane (Blog 6) |
| Nombre de tâches | Stage | 20 000 tâches pour 3 Go = petits fichiers |
| *GC Time* | Stage | Pression mémoire |

### 2.3 Du symptôme à la cause probable

| Symptôme | Cause probable | Section |
|---|---|---|
| Lit beaucoup de données pour un petit résultat | Pas de *pruning* de partitions, format non colonnaire, filtre non « pushable » | 4, 6, 7 |
| Énormément de tâches très courtes | Petits fichiers | 10 |
| Une tâche dure 20 fois plus que les autres | Skew de clé | Blog 6 |
| Jointure en `SortMergeJoin` alors qu'une table est petite | Statistiques absentes ou seuil de *broadcast* | 9 |
| Un seul exécuteur travaille | Window sans `PARTITION BY`, ou partition unique | 11 |
| Beaucoup de *spill* | Partitions trop grosses, mémoire insuffisante | 4 |

---

## 3. `EXPLAIN` selon le moteur

Un **plan** décrit comment le moteur compte exécuter ta requête. Il existe typiquement un plan **logique** (ce qu'il faut calculer) et un plan **physique** (comment, avec quels opérateurs). C'est le plan physique qui intéresse le plus pour la performance.

| Moteur | Commande | Variantes utiles |
|---|---|---|
| **Spark SQL** | `EXPLAIN <requête>` | `EXPLAIN FORMATTED` (lisible), `EXPLAIN EXTENDED` (tous les plans), `EXPLAIN COST` (estimations) |
| **Hive** | `EXPLAIN <requête>` | `EXPLAIN DEPENDENCY` (tables et partitions lues), `EXPLAIN EXTENDED`, `EXPLAIN VECTORIZATION`, `EXPLAIN ANALYZE` (plan avec comptages réels, selon la version) |
| **Impala** | `EXPLAIN <requête>` | `SET EXPLAIN_LEVEL=3` pour plus de détails ; après exécution, `SUMMARY` et `PROFILE` donnent les temps réels |

Deux précisions :

1. **`EXPLAIN` seul n'exécute pas la requête.** Il montre l'intention du moteur avec des **estimations** : utile, mais ces estimations peuvent être fausses (surtout sans statistiques, section 9).
2. Pour comparer **estimation** et **réalité**, il faut mesurer l'exécution réelle (Spark UI, profil Impala, `EXPLAIN ANALYZE` de Hive selon la version).

---

## 4. Lire un plan Spark

### 4.1 Comment lire

Un plan Spark se lit **de bas en haut** : les opérateurs en bas lisent les données, ceux du haut produisent le résultat. L'indentation montre qui alimente qui.

### 4.2 Un plan sain, annoté

La requête : le CA validé de mars 2024 par catégorie, sur la table **partitionnée par mois** (section 1.2).

```sql
SELECT categorie, SUM(montant) AS ca
FROM ventes_hist_part
WHERE mois = '2024-03' AND statut = 'VALIDEE'
GROUP BY categorie;
```

Extrait **simplifié** de `EXPLAIN FORMATTED` (la forme exacte varie) :

```
AdaptiveSparkPlan
+- HashAggregate(keys=[categorie], functions=[sum(montant)])                 ← (5) agrégation finale
   +- Exchange hashpartitioning(categorie, 200)                              ← (4) UN shuffle (inévitable ici)
      +- HashAggregate(keys=[categorie], functions=[partial_sum(montant)])   ← (3) pré-agrégation locale
         +- Project [categorie, montant]
            +- Filter (isnotnull(statut) AND (statut = VALIDEE))             ← (2) filtre
               +- FileScan parquet [categorie, montant, statut, mois]        ← (1) lecture
                    PartitionFilters: [isnotnull(mois), (mois = 2024-03)]
                    PushedFilters:    [IsNotNull(statut), EqualTo(statut,VALIDEE)]
                    ReadSchema:       struct<categorie:string, montant:decimal(10,2), statut:string>
```

Ce qu'un plan sain doit montrer :

1. **`PartitionFilters` non vide** : le moteur ne lit **que** le dossier `mois=2024-03`, pas les 36.
2. **`ReadSchema` étroit** : seules les 3 colonnes utiles sont lues, pas toute la table (*column pruning*, grâce au format colonnaire).
3. **`PushedFilters`** : le filtre est transmis au lecteur Parquet, qui peut sauter des blocs entiers grâce aux statistiques min/max.
4. **Un seul `Exchange`**, nécessaire pour le `GROUP BY`.
5. **Pré-agrégation (`partial_sum`) avant le shuffle** : on déplace des sous-totaux, pas des millions de lignes.

### 4.3 Glossaire des opérateurs

| Opérateur | Signification | Ce qu'il faut en penser |
|---|---|---|
| `FileScan parquet/orc/csv` | Lecture des fichiers | Regarde `PartitionFilters`, `PushedFilters`, `ReadSchema` |
| `Filter` | Filtre sur des lignes | Idéalement poussé vers le scan |
| `Project` | Sélection / calcul de colonnes | Léger |
| **`Exchange`** | **Shuffle** : redistribution des données entre machines par réseau | **Le coût principal.** Compte-les |
| `HashAggregate` (partial / final) | Agrégation en deux temps | Normal : « partial » avant le shuffle, « final » après |
| `Sort` | Tri | Coûteux à grande échelle (mémoire, spill) |
| `BroadcastExchange` | Envoi d'une petite table à tous les exécuteurs | Bon signe, quand la table est vraiment petite |
| `BroadcastHashJoin` | Jointure sans shuffle de la grosse table | Souvent le plus rapide |
| `SortMergeJoin` | Jointure avec shuffle **et tri** des deux côtés | Le choix par défaut pour deux grosses tables |
| `BroadcastNestedLoopJoin`, `CartesianProduct` | Jointure sans clé d'égalité ou produit cartésien | **Alerte rouge** (Blog 1, section 8.7) |
| `Window` | Calcul de fonction de fenêtre | Cherche l'`Exchange` qui le précède |
| `WholeStageCodegen (n)` | Opérateurs fusionnés en un seul code généré | Normal, optimisation |
| `AdaptiveSparkPlan` | Le plan peut être modifié en cours d'exécution (AQE) | Le plan final diffère du plan initial |

### 4.4 Les signaux d'alerte dans un plan

- **`PartitionFilters: []`** alors que la table est partitionnée et que tu filtres : le moteur lit **toutes** les partitions.
- **`PushedFilters: []`** alors que tu as un filtre sur une colonne : le filtre n'est pas utilisé par le lecteur de fichiers.
- **`ReadSchema` qui contient toutes les colonnes** : un `SELECT *` (Blog 1) a défait le *column pruning*.
- **Plusieurs `Exchange` successifs** sur la même clé : redistributions inutiles.
- **`Exchange SinglePartition`** : tout converge vers une seule tâche.
- **`CartesianProduct` / `BroadcastNestedLoopJoin`** : jointure sans condition d'égalité exploitable.
- **`SortMergeJoin` entre une grosse et une petite table** : un `BroadcastHashJoin` aurait été préférable (section 9).

### 4.5 L'AQE et le plan final

Avec l'AQE (*Adaptive Query Execution*, activée par défaut dans les versions récentes de Spark), le moteur **réajuste le plan pendant l'exécution** : fusion de petites partitions après un shuffle, changement de stratégie de jointure selon la taille réelle, traitement du skew. Conséquence : le plan affiché par `EXPLAIN` (initial) peut différer du plan réellement exécuté, visible dans l'onglet **SQL** de la Spark UI. Pour comprendre ce qui s'est passé, regarde le plan **final**.

---

## 5. Lire un plan Hive (Tez) et Impala

### 5.1 Hive sur Tez

Un plan Hive/Tez est un **graphe de sommets** (*vertices*) : des `Map` qui lisent, des `Reducer` qui agrègent, reliés par des arêtes de shuffle. Extrait **simplifié** :

```
Vertex dependency in root stage
Reducer 2 <- Map 1 (SIMPLE_EDGE)

Stage-0
  Fetch Operator
  Stage-1
    Reducer 2
      Group By Operator  (aggregations: sum(VALUE._col0), keys: KEY._col0)
        <-Map 1 [SIMPLE_EDGE]                                   ← shuffle entre Map 1 et Reducer 2
          Group By Operator  (hash, partial aggregation)
            Filter Operator  (predicate: statut = 'VALIDEE')
              TableScan  (ventes_hist_part)
                Statistics: Num rows: ...  Data size: ...  Basic stats: COMPLETE  Column stats: NONE
```

À repérer :

- **Le nombre de vertices et d'arêtes de shuffle** (`SIMPLE_EDGE`, `BROADCAST_EDGE`, `CUSTOM_SIMPLE_EDGE`). Chaque arête de shuffle coûte.
- **`Column stats: NONE`** (ou `Col:NONE`) : **les statistiques de colonnes manquent**, donc les estimations du plan sont peu fiables (section 9).
- **`Map Join Operator`** : jointure par diffusion de la petite table, l'équivalent du *broadcast*.
- **`Merge Join`** / **`Reduce Output Operator`** : jointure avec shuffle.
- `EXPLAIN DEPENDENCY` donne la **liste exacte des partitions lues**, pratique pour vérifier le pruning.

### 5.2 Impala

Le plan Impala se lit lui aussi du bas vers le haut, avec des informations très parlantes sur le scan :

```
SCAN HDFS [blog_sql.ventes_hist_part]
   partitions=1/36 files=12 size=1.2GB
```

- **`partitions=1/36`** : une seule partition lue sur 36. C'est la lecture rapide du pruning.
- **`files=…`** et **`size=…`** : le nombre de fichiers et le volume.
- Un avertissement du type **« missing relevant table and/or column statistics »** indique qu'il faut lancer `COMPUTE STATS` (section 9).
- Les stratégies de jointure apparaissent sous forme de `HASH JOIN [BROADCAST]` ou `[SHUFFLE]`.

---

## 6. Les formats de fichiers

Le format est **le levier le plus rentable** : il détermine combien d'octets lit le moteur avant même de calculer quoi que ce soit.

### 6.1 Ligne contre colonne

- **Formats orientés lignes** (CSV, texte, JSON, Avro) : les données sont stockées ligne par ligne. Pour lire **une** colonne, il faut lire **toute** la ligne.
- **Formats colonnaires** (**Parquet**, **ORC**) : les valeurs d'une même colonne sont stockées ensemble. On ne lit que les colonnes demandées, et elles se compressent très bien (valeurs similaires côte à côte).

### 6.2 Ce qu'apporte un format colonnaire

| Fonction | Principe | Effet |
|---|---|---|
| **Column pruning** | On ne lit que les colonnes du `SELECT` et des filtres | Moins d'octets lus (visible dans `ReadSchema`) |
| **Compression par colonne** | Encodage par dictionnaire, par plages, compression (Snappy, Zstd...) | Fichiers beaucoup plus petits que du texte |
| **Statistiques min/max par bloc** | Chaque groupe de lignes (*row group* en Parquet, *stripe* en ORC) mémorise le min et le max de chaque colonne | Un filtre peut **sauter** des blocs entiers (*predicate pushdown*, `PushedFilters`) |
| **Filtres de Bloom** (option) | Test d'appartenance rapide | Utile pour des recherches par valeur exacte (clé) |
| **Schéma embarqué** | Les types sont dans le fichier | Moins d'erreurs de conversion qu'avec du texte |

### 6.3 Parquet ou ORC ?

| | Parquet | ORC |
|---|---|---|
| Écosystème d'origine | Spark, Impala, large adoption | Hive |
| Tables transactionnelles (ACID) de Hive | Non | **Oui** (les tables ACID managed utilisent ORC) |
| Impala | **Format de référence** | Lecture selon la version, support moins complet |
| Spark | Très bien supporté | Bien supporté |
| Compression courante | Snappy (défaut), Zstd, Gzip | Zlib, Snappy, Zstd |

Règle pratique sur une plateforme Cloudera : **ORC** pour les tables Hive transactionnelles, **Parquet** pour les tables partagées avec Spark et Impala. Vérifie les versions : le support des formats évolue.

### 6.4 Les pièges des formats

1. **Texte compressé en gzip** : un fichier `.gz` n'est **pas découpable** (*non-splittable*). Un énorme fichier gzip est lu par **une seule tâche**, quel que soit le cluster.
2. **`SELECT *`** sur du Parquet défait le *column pruning* (Blog 1).
3. **Un filtre qui applique une fonction sur la colonne** peut empêcher le *predicate pushdown* : `WHERE year(date_vente) = 2024` n'est pas transmis comme borne min/max, alors que `WHERE date_vente >= DATE '2024-01-01' AND date_vente < DATE '2025-01-01'` l'est. Vérifie avec `PushedFilters` dans le plan.
4. **Des données non triées** limitent l'efficacité du min/max : si chaque bloc contient toutes les dates, aucun bloc ne peut être sauté. **Écrire les données triées** par la colonne le plus souvent filtrée (`SORT BY date_vente` à l'écriture) rend les statistiques bien plus discriminantes.
5. **Changer de format ne suffit pas** si les fichiers sont minuscules (section 10).

### 6.5 L'expérience à faire

Lance la même requête sur `ventes_hist_txt` et `ventes_hist_pq` :

```sql
SELECT categorie, SUM(montant) AS ca
FROM ventes_hist_txt             -- puis ventes_hist_pq
WHERE statut = 'VALIDEE'
GROUP BY categorie;
```

Compare **dans l'interface** : le volume lu (*Input Size*), le nombre de colonnes dans `ReadSchema`, le temps de la phase de lecture. Sur du texte, tu lis toutes les colonnes de toutes les lignes ; sur Parquet, trois colonnes seulement.

---

## 7. Le partitionnement

### 7.1 Le principe

Une table **partitionnée** est organisée en **dossiers**, un par valeur de la colonne de partition :

```
/warehouse/ventes_hist_part/
   mois=2022-01/   part-0000.parquet  part-0001.parquet ...
   mois=2022-02/   ...
   ...
   mois=2024-12/   ...
```

Un filtre sur la colonne de partition permet de **ne pas lire** les dossiers inutiles (*partition pruning*). Pour « mars 2024 », on lit 1 dossier sur 36, soit environ 3 % des données, sans même ouvrir les autres fichiers.

La colonne de partition n'est **pas stockée dans les fichiers** : elle est déduite du nom du dossier.

### 7.2 Choisir la colonne de partition

Une bonne colonne de partition est :

1. **Très souvent filtrée** dans les requêtes (typiquement la date) ;
2. **À cardinalité modérée** : des dizaines à quelques milliers de valeurs, pas des millions ;
3. **Équilibrée** : des partitions de tailles comparables ;
4. **Stable** : une ligne ne change pas de partition.

| Candidate | Verdict | Pourquoi |
|---|---|---|
| `mois` ou `jour` (date) | **Oui** | Filtrée presque partout, cardinalité raisonnable, croissante |
| `client_id` | **Non** | Des millions de valeurs : des millions de dossiers minuscules |
| `statut` | **Non** | 3 valeurs dont une à 80 % : peu de pruning, partitions déséquilibrées |
| `categorie` | Rarement | Peu de valeurs, souvent déséquilibrées, rarement filtrée seule |
| `pays` | Selon le cas | À envisager si les requêtes sont systématiquement par pays |

### 7.3 La bonne taille

L'équilibre à trouver : **assez de partitions** pour que le pruning serve, **pas trop** pour ne pas fabriquer des milliers de petits fichiers. Un repère : viser des partitions de l'ordre de **plusieurs centaines de Mo à quelques Go** (à adapter). Au rythme de 2 milliards de lignes sur 3 ans :

| Granularité | Partitions | Lignes par partition (ordre de grandeur) |
|---|---|---|
| Jour | ~1 100 | ~1,8 million |
| **Mois** | **36** | **~55 millions** |
| Année | 3 | ~670 millions |

Pour une table qui grossit de façon régulière, **le mois** est souvent le bon compromis ici ; le **jour** devient pertinent si chaque jour pèse déjà plusieurs centaines de Mo.

### 7.4 Vérifier le pruning

```sql
-- Hive : liste exacte des tables et partitions lues
EXPLAIN DEPENDENCY
SELECT categorie, SUM(montant) FROM ventes_hist_part
WHERE mois = '2024-03' GROUP BY categorie;

-- Spark : cherche PartitionFilters dans le plan
EXPLAIN FORMATTED SELECT ... ;

-- Impala : cherche partitions=1/36 dans le scan
EXPLAIN SELECT ... ;

-- Commandes de gestion
SHOW PARTITIONS ventes_hist_part;
MSCK REPAIR TABLE ventes_hist_part;      -- enregistre des dossiers ajoutés hors Hive (selon version)
```

### 7.5 Les pièges du partitionnement

1. **Sur-partitionner.** Partitionner par jour **et** catégorie **et** pays fabrique des milliers de dossiers contenant quelques Ko. Le métastore (qui liste les partitions) ralentit, le nombre de tâches explose.
2. **Filtre sur une autre colonne.** Si la table est partitionnée par `mois` mais que tu filtres sur `date_vente` seulement, **aucun pruning** : `PartitionFilters` reste vide. Tu dois exprimer le filtre sur la colonne de partition (ou dériver la condition : `mois = '2024-03' AND date_vente = ...`).
3. **Mauvais type de comparaison.** Comparer une partition de type `STRING` à une valeur d'un autre type entraîne des conversions qui peuvent empêcher le pruning. Garde le même type des deux côtés et lis le plan.
4. **Fonction sur la colonne de partition.** Selon le moteur et la version, une expression sur la colonne de partition (`substr(mois, 1, 4) = '2024'`) est parfois évaluée sur la liste des partitions (le pruning fonctionne, mais le métastore est interrogé pour **toutes** les partitions), parfois non. **Le plus sûr est une comparaison directe** (`mois >= '2024-01' AND mois <= '2024-12'`), puis une vérification dans le plan : c'est exactement ce que le Blog 1 recommandait sans l'avoir vérifié.
5. **Partitions déséquilibrées.** Une partition qui contient 60 % des données devient un goulot d'étranglement.
6. **Changer le schéma de partition coûte cher** : il faut réécrire les données. Les formats de table modernes (Iceberg, Blog 8) allègent ce problème.
7. **Le partitionnement dynamique** (`PARTITION (mois)` avec valeurs calculées) peut produire un fichier par tâche et par partition : voir la section 10.

---

## 8. Le bucketing

### 8.1 Le principe

Le **bucketing** (« paniers de hachage ») répartit les lignes d'une table dans un **nombre fixe de fichiers** selon le hachage d'une colonne :

```sql
CREATE TABLE ventes_hist_bkt (
  vente_id BIGINT, client_id INT, categorie STRING,
  montant DECIMAL(10,2), statut STRING, date_vente DATE
)
CLUSTERED BY (client_id) INTO 32 BUCKETS
STORED AS ORC;
```

Toutes les lignes d'un même `client_id` atterrissent dans le **même bucket**. Si deux tables sont bucketées **sur la même clé, avec le même nombre de buckets**, une jointure sur cette clé peut se faire **bucket par bucket, sans shuffle** (*bucket map join* ou *sort-merge-bucket join* dans Hive ; jointure sans `Exchange` dans Spark).

### 8.2 Partitionnement contre bucketing

| | Partitionnement | Bucketing |
|---|---|---|
| Découpage | Par **valeur** de la colonne (un dossier par valeur) | Par **hachage** (nombre fixe de fichiers) |
| Convient à | Colonne filtrée, cardinalité modérée | Colonne de **jointure** ou d'agrégation, forte cardinalité |
| Bénéfice principal | Ne pas lire des données (pruning) | Éviter un shuffle lors de jointures répétées |
| Risque | Trop de partitions, petits fichiers | Nombre de buckets mal choisi, rigidité |

Les deux se combinent : partitionner par `mois` et bucketer par `client_id`.

### 8.3 Pourquoi on s'en sert avec prudence

1. **Hive et Spark ne hachent pas de la même façon.** Une table bucketée par Hive n'est généralement **pas** reconnue comme bucketée par Spark (et inversement) : le gain disparaît quand deux moteurs se partagent la table. Vérifie le comportement de ta version.
2. **Les deux côtés doivent être alignés** : même clé, même nombre de buckets (ou multiples), sinon un `Exchange` réapparaît.
3. **Le nombre de buckets est figé** à la création : trop peu, des fichiers énormes ; trop, des fichiers minuscules.
4. **L'écriture est plus contraignante** (il faut respecter le bucketing à chaque insertion).
5. **Le skew reste possible** : une clé très fréquente remplit un seul bucket.
6. **L'AQE et le *broadcast*** suffisent à résoudre la plupart des cas de jointure sans que l'on ait à bucketer (Blog 6).

> **Recommandation** : n'envisage le bucketing que pour des **jointures répétées entre deux très grosses tables** sur la même clé, dans un moteur unique, après avoir mesuré que le shuffle domine. Sinon, partitionnement + statistiques + format colonnaire suffisent.

---

## 9. Les statistiques et l'optimiseur

### 9.1 À quoi servent les statistiques

L'optimiseur (CBO, *cost-based optimizer*) choisit un plan **en estimant** des tailles : combien de lignes après ce filtre ? combien d'octets pour cette table ? quelle table est la plus petite ? Pour estimer, il lit des **statistiques** :

| Statistique | Niveau | Sert à |
|---|---|---|
| Nombre de lignes, taille en octets | Table / partition | Choisir *broadcast* ou *shuffle*, ordonner les jointures |
| Nombre de valeurs distinctes (NDV) | Colonne | Estimer le nombre de groupes, la sélectivité des jointures |
| Nombre de NULL | Colonne | Estimer l'effet d'un `IS NULL`, d'une jointure |
| Min / Max | Colonne | Estimer la sélectivité d'un filtre par plage |

**Sans statistiques, l'optimiseur devine.** Il peut alors choisir une jointure par shuffle pour une table qui tient en mémoire, ou l'inverse (diffuser une table énorme, ce qui fait échouer le job).

### 9.2 Calculer les statistiques

```sql
-- Hive
ANALYZE TABLE ventes_hist_part PARTITION (mois) COMPUTE STATISTICS;
ANALYZE TABLE ventes_hist_part PARTITION (mois) COMPUTE STATISTICS FOR COLUMNS;

-- Spark SQL
ANALYZE TABLE ventes_hist_pq COMPUTE STATISTICS;
ANALYZE TABLE ventes_hist_pq COMPUTE STATISTICS FOR ALL COLUMNS;

-- Impala
COMPUTE STATS ventes_hist_part;
COMPUTE INCREMENTAL STATS ventes_hist_part;      -- pour les grosses tables partitionnées : recalcule les partitions modifiées
```

### 9.3 Vérifier l'état des statistiques

```sql
DESCRIBE FORMATTED ventes_hist_part;      -- Hive / Impala : numRows, totalSize, numFiles, rawDataSize
DESCRIBE EXTENDED  ventes_hist_pq;        -- Spark : ligne « Statistics »
SHOW TABLE STATS   ventes_hist_part;      -- Impala
SHOW COLUMN STATS  ventes_hist_part;      -- Impala
```

Un `numRows` à **-1** signifie « inconnu » (Hive et Impala). Dans un plan, un **`Column stats: NONE`** (Hive) ou un avertissement **« missing relevant table and/or column statistics »** (Impala) est le signal à traiter.

### 9.4 Les paramètres qui comptent (valeurs par défaut variables selon la version)

| Moteur | Paramètre | Effet |
|---|---|---|
| Hive | `hive.cbo.enable` | Active l'optimisation par coût |
| Hive | `hive.stats.autogather` | Collecte automatique de statistiques à l'écriture (selon le type de table) |
| Hive | `hive.auto.convert.join` + seuil de taille | Conversion automatique en jointure par diffusion (*map join*) |
| Spark | `spark.sql.cbo.enabled` | Active l'optimisation par coût (désactivée par défaut dans de nombreuses versions) |
| Spark | `spark.sql.autoBroadcastJoinThreshold` | Taille max d'une table diffusée automatiquement (10 Mo par défaut) |
| Spark | `spark.sql.adaptive.enabled` | AQE : réajuste le plan à l'exécution avec les tailles réelles |

### 9.5 Exemple : la jointure qui devrait être un *broadcast*

```sql
SELECT c.pays, SUM(v.montant) AS ca
FROM ventes_hist_part v
JOIN clients_dedup c ON v.client_id = c.client_id
WHERE v.mois = '2024-03'
GROUP BY c.pays;
```

`clients_dedup` est petite. **Sans statistiques**, le plan peut afficher :

```
SortMergeJoin [client_id], [client_id], Inner
:- Sort [client_id] ← Exchange hashpartitioning(client_id, 200) ← (grosse table, shuffle)
+- Sort [client_id] ← Exchange hashpartitioning(client_id, 200) ← (petite table, shuffle inutile)
```

**Avec statistiques** (ou grâce à l'AQE qui connaît la taille réelle après filtre), on s'attend à :

```
BroadcastHashJoin [client_id], [client_id], Inner, BuildRight
:- Scan ventes_hist_part (grosse table, pas de shuffle)
+- BroadcastExchange ← Scan clients_dedup (petite table diffusée)
```

Le plan sans shuffle sur la grosse table est typiquement **beaucoup** plus rapide. Un *hint* (`/*+ BROADCAST(c) */` en Spark, `/*+ MAPJOIN(c) */` en Hive, `[BROADCAST]` en Impala) peut forcer la stratégie, mais c'est **un dernier recours** : une statistique juste vaut mieux qu'un hint qui devient faux le jour où la table grossit.

### 9.6 Les pièges des statistiques

1. **Des statistiques périmées sont pires que pas de statistiques** : après un gros chargement, l'optimiseur croit que la table est petite. **Recalcule après les chargements significatifs.**
2. **Les tables externes** ne sont pas toujours mises à jour automatiquement.
3. **Le calcul sur toutes les colonnes** d'une très grosse table est coûteux : limite-toi aux colonnes de jointure et de filtre (`FOR COLUMNS col1, col2`).
4. **Les statistiques par partition** (Hive, Impala) sont à tenir à jour pour les partitions récentes, pas toujours pour tout l'historique.
5. **Un hint masque le problème** au lieu de le résoudre.

---

## 10. Le problème des petits fichiers

### 10.1 Pourquoi c'est un problème

Dans HDFS, **chaque fichier est un objet que le NameNode garde en mémoire** (de l'ordre de la centaine d'octets par objet). Des millions de petits fichiers :

- saturent la mémoire du NameNode ;
- créent **une tâche par fichier** (ou presque) : on passe plus de temps à **démarrer des tâches** qu'à lire des données ;
- ralentissent le listage des dossiers et l'ouverture des fichiers ;
- dégradent la compression et le min/max des formats colonnaires.

> **L'ordre de grandeur à viser** : des fichiers de la taille d'un bloc HDFS ou d'un multiple (typiquement **128 Mo à 512 Mo**). Sous quelques dizaines de Mo en moyenne, c'est un signal d'alerte.

### 10.2 D'où viennent-ils ?

| Cause | Mécanisme |
|---|---|
| **Insertion dynamique de partitions** | Chaque tâche écrit **un fichier par partition rencontrée** : 200 tâches × 36 partitions = jusqu'à 7 200 fichiers par chargement |
| **Valeur par défaut de 200 partitions de shuffle** | 200 fichiers de sortie, même pour 50 Mo de données |
| **Ingestion en continu** (streaming, micro-batchs, `INSERT` fréquents) | Un petit fichier par lot |
| **Sur-partitionnement** | Des milliers de dossiers contenant quelques Ko |
| **Petits fichiers sources** | Dépôts de fichiers quotidiens ou horaires minuscules |

### 10.3 Les détecter

```bash
# Nombre de dossiers, de fichiers et d'octets (taille moyenne = octets / fichiers)
hdfs dfs -count -h /warehouse/blog_sql.db/ventes_hist_part

# Taille de chaque partition
hdfs dfs -du -h /warehouse/blog_sql.db/ventes_hist_part
```

```sql
DESCRIBE FORMATTED ventes_hist_part PARTITION (mois = '2024-03');   -- numFiles, totalSize
```

Dans la **Spark UI** : un stage avec **20 000 tâches** pour **3 Go** lus, une médiane de tâche à quelques centaines de millisecondes.

### 10.4 Les corriger

**À l'écriture : ne pas les créer.**

```sql
-- Hive ou Spark : regrouper les lignes d'un même mois dans une même tâche
INSERT OVERWRITE TABLE ventes_hist_part PARTITION (mois)
SELECT vente_id, client_id, categorie, montant, statut, date_vente, mois
FROM staging
DISTRIBUTE BY mois, pmod(vente_id, 4);      -- 4 fichiers par mois au lieu de 200

-- Spark SQL : par hint
INSERT OVERWRITE TABLE ventes_hist_part PARTITION (mois)
SELECT /*+ REPARTITION(4, mois) */ vente_id, client_id, categorie, montant, statut, date_vente, mois
FROM staging;
```

`DISTRIBUTE BY mois` seul enverrait tout un mois vers **une seule tâche** (un fichier géant et un goulot) ; ajouter `pmod(vente_id, 4)` répartit chaque mois sur 4 tâches. Le nombre à choisir dépend de la taille d'un mois : **taille du mois ÷ taille de fichier visée**. Avec Impala, l'indication `[SHUFFLE]` dans l'`INSERT ... PARTITION (...)` sert le même objectif (selon la version).

**Après coup : compacter.**

| Situation | Commande |
|---|---|
| Table Hive **ORC** non transactionnelle | `ALTER TABLE t PARTITION (mois='2024-03') CONCATENATE;` |
| Table Hive **ACID** | `ALTER TABLE t COMPACT 'major';` (ou compactage automatique selon la configuration) |
| Table **Parquet** | Réécrire la partition : `INSERT OVERWRITE ... SELECT ... DISTRIBUTE BY ...` |
| Spark (DataFrame) | `repartition(n)` ou `coalesce(n)` avant l'écriture |

**Pendant la lecture** : l'AQE de Spark peut **fusionner de petites partitions de shuffle** (`spark.sql.adaptive.coalescePartitions.enabled`). Cela atténue la lenteur mais **ne répare pas** les fichiers eux-mêmes.

### 10.5 Le piège inverse

Un **seul fichier énorme non découpable** (gzip, par exemple) n'a qu'**une** tâche de lecture. Les petits fichiers coûtent en nombre, les gros fichiers non splittables en parallélisme perdu.

---

## 11. Vérifier les promesses des Blogs 1 à 4

Les blogs précédents contenaient plusieurs affirmations du type « vérifie dans le plan ». Voici **quoi chercher** pour chacune. Les extraits sont **simplifiés**.

| Blog | Affirmation | Que chercher dans le plan |
|---|---|---|
| 1 | Un filtre doit être exprimé directement sur la colonne de partition | `PartitionFilters` non vide ; `partitions=1/36` (Impala) ; `EXPLAIN DEPENDENCY` (Hive) |
| 1 | Une fonction sur la colonne de filtre peut empêcher le pushdown | `PushedFilters` : comparer `year(date_vente) = 2024` avec une borne `>= ... AND < ...` |
| 1 | `NOT IN` est dangereux, `NOT EXISTS` plus sûr | La jointure `LeftAnti` produite (ci-dessous) |
| 2 | `EXISTS` / `IN` sont des semi-jointures | `LeftSemi` dans le plan ; pas de multiplication de lignes |
| 2 | Une CTE référencée plusieurs fois peut être recalculée | Deux `FileScan` de la même table (ou un `ReusedExchange` en Spark) |
| 3 | Une window sans `PARTITION BY` envoie tout vers une partition | `Exchange SinglePartition` avant le `Window` |
| 4 | Le *nested column pruning* ne lit que les champs utiles | `ReadSchema` avec `struct<livraison:struct<pays:string>>` |
| 4 | `CUBE` = `2^n` niveaux, `ROLLUP` = `n + 1` | Opérateur `Expand` avec ce nombre de projections |

### 11.1 `NOT IN` contre `NOT EXISTS`

Selon la version de Spark, un `NOT IN` sur une colonne **nullable** peut produire une jointure non équi coûteuse :

```
BroadcastNestedLoopJoin BuildRight, LeftAnti, ((client_id = client_id) OR isnull((client_id = client_id)))
```

alors que `NOT EXISTS` produit en général une jointure sur clé :

```
BroadcastHashJoin [client_id], [client_id], LeftAnti, BuildRight      (ou SortMergeJoin LeftAnti)
```

Les versions récentes de Spark ont ajouté des optimisations pour certains cas de `NOT IN`, donc le comportement dépend de ta version : **le plan fait foi**. Retiens que `NOT EXISTS` est à la fois **plus sûr** (NULL, Blog 1) et **souvent plus rapide**.

### 11.2 Window sans partition

```
Window [sum(ca) windowspecdefinition(jour ASC NULLS FIRST, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)]
+- Sort [jour ASC NULLS FIRST]
   +- Exchange SinglePartition              ← tout sur UNE tâche
```

Avec `PARTITION BY mois`, on obtient `Exchange hashpartitioning(mois, 200)` à la place : le travail est réparti.

### 11.3 `CUBE` et `ROLLUP`

```
Expand [[mois, categorie, montant, 0], [mois, null, montant, 1],
        [null, categorie, montant, 2], [null, null, montant, 3]]     ← CUBE(2 colonnes) : 4 projections
```

Chaque ligne d'entrée est **dupliquée** autant de fois qu'il y a de projections : c'est le coût des `2^n` niveaux. Un `ROLLUP` sur 2 colonnes n'en aurait que 3.

### 11.4 Le *nested column pruning*

```
FileScan parquet [produits, livraison]
   ReadSchema: struct<produits:array<string>, livraison:struct<pays:string>>     ← seul le champ pays est lu
```

Si `ReadSchema` montre `livraison:struct<ville:string,pays:string>`, l'élagage des colonnes imbriquées n'a pas eu lieu (selon la version et la configuration).

---

## 12. Résolution de l'incident

Retour à lundi, 8h12. Un diagnostic **sans suppositions** :

### 12.1 Le constat (à confirmer sur ton cluster)

| Réflexe | Ce que le diagnostic montre (scénario) | Cause |
|---|---|---|
| 1. Où va le temps ? | La phase de **lecture** et le **shuffle** dominent | Trop de données lues et déplacées |
| 2. Que dit le plan ? | Scan texte, `PartitionFilters` absent, `ReadSchema` à toutes les colonnes, `SortMergeJoin` avec une petite table | Format, partitionnement, statistiques |
| 3. Que disent les données ? | Table en `TEXTFILE`, non partitionnée, des dizaines de milliers de petits fichiers, `numRows = -1` | Format, partitionnement, petits fichiers, statistiques |

Le job n'a besoin que du **dernier mois** et de **3 colonnes**, mais il lit **toute l'historique, toutes les colonnes**, dans un format qui ne sait pas sauter de blocs.

### 12.2 Les quatre corrections

| # | Correction | Effet attendu | Section |
|---|---|---|---|
| 1 | Passer en **Parquet** | Colonnes lues en moins, compression, pushdown | 6 |
| 2 | **Partitionner par mois** et filtrer directement sur `mois` | Lecture d'**1 mois sur 36** | 7 |
| 3 | **Calculer les statistiques** après chaque chargement | `BroadcastHashJoin` pour la petite table | 9 |
| 4 | **Compacter** et écrire avec `DISTRIBUTE BY` | Beaucoup moins de tâches, fichiers de taille correcte | 10 |

### 12.3 Estimer le gain *avant* de mesurer

Pas besoin d'inventer un chiffre : le **volume lu** se calcule approximativement.

```
volume lu après  ≈  volume avant  ×  (1 / 36 partitions)  ×  (3 colonnes / 6 colonnes)  ×  (rapport de compression)
```

Avec 36 partitions et 3 colonnes sur 6, on lit déjà environ 1/72 du volume avant compression, avant même de compter le gain du format colonnaire. Le vrai résultat, **tu le mesures** : remplis ce tableau avant/après.

| Mesure | Avant | Après |
|---|---|---|
| Volume lu (*Input Size*) | | |
| Nombre de tâches | | |
| Volume de shuffle | | |
| Stratégie de jointure | | |
| Durée totale | | |

### 12.4 La requête corrigée

```sql
SELECT c.pays, v.categorie, SUM(v.montant) AS ca
FROM ventes_hist_part v                     -- Parquet, partitionnée par mois
JOIN clients_dedup c ON v.client_id = c.client_id
WHERE v.mois = '2024-03'                     -- filtre DIRECT sur la colonne de partition
  AND v.statut = 'VALIDEE'
GROUP BY c.pays, v.categorie;
```

À vérifier dans le plan : `PartitionFilters` non vide, `ReadSchema` à 4 colonnes (`client_id`, `categorie`, `montant`, `statut`), `PushedFilters` sur `statut`, `BroadcastHashJoin` pour `clients_dedup`, un seul `Exchange` pour l'agrégation.

> **Ce qu'on a évité** : « on augmente la mémoire du cluster ». La cause n'était pas un manque de ressources mais un **volume de données lues inutile**. Ajouter des machines aurait masqué le problème pour quelques mois.

---

## 13. 10 questions d'entretien

**1. Une requête met 40 minutes sur 2 milliards de lignes. Que fais-tu ?**
Je mesure d'abord : où passe le temps (Spark UI, Tez UI, profil Impala) ? Je lis le plan : volume lu, pruning, `Exchange`, stratégie de jointure. J'examine les données : format, partitions, fichiers, statistiques, skew. Puis je corrige la cause (format, partitionnement, statistiques, petits fichiers) avant de toucher aux ressources.

**2. Qu'est-ce qu'un `Exchange` dans un plan Spark ?**
Un **shuffle** : une redistribution des données entre machines à travers le réseau (avec écriture sur disque), nécessaire pour un `GROUP BY`, une jointure ou un `ORDER BY`. C'est généralement le coût principal d'une requête : on les compte et on cherche à les réduire.

**3. Différence entre *partition pruning*, *predicate pushdown* et *column pruning* ?**
*Partition pruning* : ne pas lire les dossiers inutiles. *Predicate pushdown* : transmettre le filtre au lecteur de fichiers pour sauter des blocs (statistiques min/max). *Column pruning* : ne lire que les colonnes nécessaires (format colonnaire).

**4. Pourquoi Parquet ou ORC plutôt que CSV ?**
Format **colonnaire** : lecture des colonnes utiles seulement, compression efficace, statistiques min/max par bloc, schéma embarqué. Le CSV oblige à tout lire et à tout reparser.

**5. Comment choisis-tu une colonne de partition ? Pourquoi pas `client_id` ?**
Colonne très filtrée, de cardinalité modérée, équilibrée et stable (typiquement une date). `client_id` a des millions de valeurs : des millions de dossiers et de petits fichiers.

**6. Qu'est-ce que le problème des petits fichiers, et comment le corriger ?**
Des milliers de fichiers très petits : surcharge du NameNode, une tâche par fichier, lectures lentes. Causes : insertion dynamique, 200 partitions de shuffle par défaut, ingestion continue, sur-partitionnement. Corrections : `DISTRIBUTE BY` / `REPARTITION` à l'écriture, compactage (`CONCATENATE`, `COMPACT`, réécriture), tailles de fichiers de 128 à 512 Mo.

**7. Pourquoi les statistiques comptent-elles ?**
Elles alimentent l'optimiseur : choix *broadcast* ou *shuffle*, ordre des jointures, estimation des tailles. Sans elles, il devine. Commandes : `ANALYZE TABLE` (Hive, Spark), `COMPUTE STATS` (Impala). Des statistiques périmées sont pires que pas de statistiques.

**8. `BroadcastHashJoin` ou `SortMergeJoin` ?**
Le premier diffuse la petite table à tous les exécuteurs : **pas de shuffle** de la grosse table. Le second redistribue et trie les deux côtés, pour deux grosses tables. Le choix dépend des tailles estimées (statistiques) et réelles (AQE).

**9. Partitionnement ou bucketing ?**
Le partitionnement découpe par valeur et sert au pruning (colonne filtrée). Le bucketing découpe par hachage en nombre fixe et sert à éviter le shuffle sur des jointures répétées. Le bucketing est rigide et sensible aux différences entre moteurs.

**10. Que change l'AQE ?**
Le plan est **réajusté pendant l'exécution** avec les tailles réelles : fusion de petites partitions, changement de stratégie de jointure, traitement du skew. Le plan d'`EXPLAIN` est donc initial : le plan final se lit dans la Spark UI.

---

## 14. 5 exercices

### Exercice 1 : le pruning qui ne marche pas
La table est partitionnée par `mois`. Le plan de cette requête montre `PartitionFilters: []`. Pourquoi, et comment corriger ?

```sql
SELECT SUM(montant) FROM ventes_hist_part
WHERE date_vente >= DATE '2024-03-01' AND date_vente < DATE '2024-04-01';
```

<details>
<summary>Solution</summary>

Le filtre porte sur `date_vente`, qui **n'est pas la colonne de partition**. Le moteur ne sait pas qu'elle est liée à `mois`. Les `PushedFilters` peuvent servir au min/max des blocs, mais **toutes les partitions sont listées et ouvertes**. Correction : exprimer aussi la condition sur la partition.

```sql
WHERE mois = '2024-03'
  AND date_vente >= DATE '2024-03-01' AND date_vente < DATE '2024-04-01'
```

(On garde le filtre sur `date_vente` si la partition mensuelle couvre plus que ce qu'on veut.) On vérifie ensuite `PartitionFilters: [isnotnull(mois), (mois = 2024-03)]`.
</details>

### Exercice 2 : la jointure par shuffle
Le plan montre un `SortMergeJoin` entre `ventes_hist_part` (énorme) et `clients_dedup` (quelques Mo). Donne trois causes possibles et trois corrections.

<details>
<summary>Solution</summary>

**Causes possibles** : (1) pas de statistiques sur `clients_dedup`, l'optimiseur ne sait pas qu'elle est petite ; (2) la taille estimée dépasse le seuil de diffusion (`spark.sql.autoBroadcastJoinThreshold`, 10 Mo par défaut) ; (3) la table est plus grosse qu'on ne le pense (vérifier la taille réelle).

**Corrections** : (1) `ANALYZE TABLE ... COMPUTE STATISTICS` ; (2) ajuster le seuil ou compter sur l'AQE qui connaît la taille réelle ; (3) un *hint* `/*+ BROADCAST(c) */` en dernier recours, après avoir vérifié que la table tient bien en mémoire.
</details>

### Exercice 3 : le goulot unique
Ce plan est suivi d'une tâche qui dure 30 minutes tandis que le reste du cluster attend. Que lis-tu, et que fais-tu ?

```
Window [sum(montant) ...]
+- Sort [date_vente ASC]
   +- Exchange SinglePartition
```

<details>
<summary>Solution</summary>

`Exchange SinglePartition` : **toutes les lignes convergent vers une seule tâche**. La window function n'a pas de `PARTITION BY`. Corrections : ajouter un `PARTITION BY` pertinent (par exemple par `mois`), ou **agréger avant** (`GROUP BY date_vente`) pour réduire les données à quelques milliers de lignes, puis appliquer la window sur ce petit résultat (comme `ca_jour` au Blog 3).
</details>

### Exercice 4 : 20 000 tâches pour 3 Go
Dans la Spark UI : un stage de lecture a 20 000 tâches pour 3 Go lus, médiane de 0,3 s. Diagnostic et correction ? À quelle taille de fichier faut-il viser ?

<details>
<summary>Solution</summary>

Moyenne : 3 Go ÷ 20 000 ≈ 150 Ko par tâche (donc par fichier) : c'est le problème des **petits fichiers**. Le temps est dominé par le démarrage des tâches. Correction : **compacter** (`CONCATENATE` pour de l'ORC, `COMPACT 'major'` pour de l'ACID, réécriture avec `DISTRIBUTE BY` pour du Parquet) et corriger l'écriture. En visant 128 à 512 Mo par fichier, 3 Go tiendraient en une dizaine à une vingtaine de fichiers, soit un ordre de grandeur de **1 000 fois moins de tâches** (sauf si on veut plus de parallélisme sur les gros volumes).
</details>

### Exercice 5 : choisir le partitionnement
Une table `evenements` reçoit 5 millions de lignes par jour d'environ 500 octets chacune. Les requêtes filtrent presque toujours par jour, parfois par `client_id`. Que choisis-tu ?

<details>
<summary>Solution</summary>

Volume brut : 5 millions × 500 octets = **2,5 Go par jour**. Après compression en format colonnaire (un facteur de l'ordre de 3 à 5), de l'ordre de **0,5 à 0,8 Go par jour** : c'est une bonne taille de partition. On **partitionne par jour**. On ne partitionne **pas** par `client_id` (des millions de valeurs, petits fichiers). Si les recherches par client sont fréquentes, on peut **trier** les données par `client_id` à l'écriture (pour des statistiques min/max efficaces) ou envisager un **bucketing** par `client_id`, après mesure. *(Hypothèses de taille et de compression : à vérifier sur les données réelles.)*
</details>

---

## 15. Checklist de performance

- [ ] J'ai **mesuré** avant de modifier : Spark UI / Tez UI / profil Impala, pas une intuition
- [ ] J'ai lu `EXPLAIN` : `PartitionFilters`, `PushedFilters`, `ReadSchema`, nombre d'`Exchange`, stratégie de jointure
- [ ] Les tables lues par des requêtes analytiques sont en **Parquet ou ORC**, pas en texte
- [ ] Le filtre porte **directement sur la colonne de partition**, avec le bon type
- [ ] La colonne de partition est de cardinalité modérée et les partitions font des centaines de Mo à quelques Go
- [ ] Pas de `SELECT *` : `ReadSchema` ne contient que les colonnes utiles
- [ ] Les **statistiques** sont calculées après les gros chargements (`ANALYZE` / `COMPUTE STATS`)
- [ ] Les jointures avec une petite table sont en **broadcast** (statistiques, pas seulement un hint)
- [ ] Les **fichiers** font en moyenne 128 à 512 Mo ; l'écriture contrôle le nombre de fichiers (`DISTRIBUTE BY`, `REPARTITION`)
- [ ] Aucune window sans `PARTITION BY` sur une grosse table (`Exchange SinglePartition`)
- [ ] `NOT EXISTS` plutôt que `NOT IN` ; aucun `CartesianProduct` involontaire
- [ ] J'ai noté les **mesures avant/après** (volume lu, tâches, shuffle, durée) pour valider chaque correction

---

## 16. Prochain blog et blogs restants

**Blog 6 : performance II, jointures distribuées et skew.** Comment le moteur exécute réellement une jointure (*broadcast*, *sort-merge*, *shuffle hash*), pourquoi une clé très fréquente (le *skew*) ralentit un job entier, et comment la traiter (AQE, *salting*, isolation de la clé chaude, correction à la source).

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions |
| ✅ 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE`, pivot |
| ✅ 5 | Performance I : lire un plan | `EXPLAIN`, formats, partitionnement, bucketing, statistiques, petits fichiers |
| 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| 7 | Modélisation | Star schema, SCD 1 / 2 / 3, calendrier, clés de substitution, Data Vault |
| 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 6

Sans regarder la suite : (1) dans la Spark UI, un stage de jointure a 200 tâches, 199 terminent en 20 secondes et **une seule dure 25 minutes**. Que se passe-t-il et comment le confirmes-tu ? (2) Cite **trois** façons de réduire le problème. Si tu hésites, le Blog 6 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de reproduire chaque expérience sur ton cluster et de noter tes propres mesures.*