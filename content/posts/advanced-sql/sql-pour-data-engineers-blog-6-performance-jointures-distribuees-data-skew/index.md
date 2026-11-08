---
title: "SQL pour Data Engineers, Blog 6 : performance II, jointures distribuées et data skew"
description: "Comprendre les stratégies de jointure distribuée et diagnostiquer puis corriger le data skew."
weight: 6
---

# SQL pour Data Engineers, Blog 6 : performance II, jointures distribuées et data skew

> **Série** : SQL, de basique à tech lead · **Niveau** : 4/5 · **Temps de lecture** : ~40 min (avec les expériences proposées)
> **Moteurs visés** : Hive (Tez), Spark SQL, Impala (stack Cloudera). Les écarts de dialecte et de version sont signalés.
> **Prérequis** : [Blogs 1 à 5]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-5-performance-lire-un-plan" >}}), en particulier le Blog 5 (lire un plan, statistiques, petits fichiers).

> **Ce qui a été vérifié, et ce qui ne l'a pas été.** La **logique** des trois techniques de correction du skew (isolation de la clé chaude, *salting*, agrégation en deux phases) a été **vérifiée sur les petites tables de la série** (`ventes_dedup`, `clients_dedup`) : les résultats sont identiques à la jointure simple. La **syntaxe** Spark / Hive, le **générateur de 50 millions de lignes** et tous les **comportements de performance** n'ont **pas** été exécutés sur un cluster : à reproduire et à mesurer chez toi. Les extraits de plans sont **simplifiés**.

---

## 0. Pourquoi ce blog existe

Le Blog 5 a montré comment **lire** et **réduire** ce qu'une requête lit. Ce blog traite l'autre grand poste de coût d'un job distribué : **les jointures** et le déséquilibre des données (*skew*).

Question d'entretien quasi certaine pour un profil Spark / Hive : *« Dans la Spark UI, 199 tâches se terminent en 20 secondes et la dernière dure 25 minutes. Que se passe-t-il ? Comment tu le règles ? »*

Le scénario :

> **Mardi, 9h07.** Le job `enrichissement_ventes` joint `ventes_skew` (environ 50 millions de lignes) au référentiel `clients_ref` pour ajouter le pays du client. Dans la Spark UI : **200 tâches, 199 terminent en une vingtaine de secondes, une seule dure 25 minutes**. Le cluster est quasi vide pendant ce temps. On te demande : *« Pourquoi une seule tâche est si lente, et comment on la règle ? »*

À la fin du blog, tu sauras **expliquer** le phénomène avec un calcul, le **détecter** avec une requête et la Spark UI, et choisir entre **six remèdes**, avec leurs conditions et leurs coûts.

### Plan

1. [Le laboratoire](#1-le-laboratoire)
2. [Comment une jointure s'exécute en distribué](#2-comment-une-jointure-sexécute-en-distribué)
3. [Choisir et influencer la stratégie](#3-choisir-et-influencer-la-stratégie)
4. [Le data skew : le mécanisme](#4-le-data-skew--le-mécanisme)
5. [Détecter le skew](#5-détecter-le-skew)
6. [Remèdes I : éviter le shuffle ou laisser le moteur corriger](#6-remèdes-i--éviter-le-shuffle-ou-laisser-le-moteur-corriger)
7. [Remèdes II : isolation, salting, correction à la source](#7-remèdes-ii--isolation-salting-correction-à-la-source)
8. [Le skew dans les agrégations et les windows](#8-le-skew-dans-les-agrégations-et-les-windows)
9. [Les autres causes de jointure lente](#9-les-autres-causes-de-jointure-lente)
10. [Résolution de l'incident](#10-résolution-de-lincident)
11. [10 questions d'entretien](#11-10-questions-dentretien)
12. [5 exercices](#12-5-exercices)
13. [Checklist avant de livrer une jointure](#13-checklist-avant-de-livrer-une-jointure)
14. [Prochain blog et blogs restants](#14-prochain-blog-et-blogs-restants)

---

## 1. Le laboratoire

### 1.1 Le laboratoire « grand » : 50 millions de lignes (non exécuté ici)

Un client `0` représente **80 %** des ventes (ventes anonymes, « invités ») : c'est un cas très courant de valeur par défaut qui concentre les lignes.

```sql
USE blog_sql;

CREATE TABLE ventes_skew STORED AS PARQUET AS
SELECT
  id                                                         AS vente_id,
  CASE WHEN pmod(id, 10) < 8 THEN 0                          -- 80 % : client « invité »
       ELSE CAST(pmod(id * 7919, 100000) + 1 AS INT) END     -- 20 % : clients 1 à 100 000
                                                             AS client_id,
  CAST(10 + pmod(id * 37, 1990) AS DECIMAL(10,2))            AS montant,
  date_add(DATE '2022-01-01', CAST(pmod(id, 1096) AS INT))   AS date_vente
FROM range(0, 50000000);

CREATE TABLE clients_ref STORED AS PARQUET AS
SELECT
  CAST(id AS INT)                                                   AS client_id,
  concat('client_', id)                                             AS nom,
  CASE WHEN id = 0 THEN 'INCONNU'                                  -- la ligne 0 = « client invité »
       ELSE element_at(array('FR', 'SN', 'MA', 'IT'), CAST(pmod(id, 4) AS INT) + 1) END AS pays
FROM range(0, 100001);
```

Par construction : **40 000 000 lignes** ont `client_id = 0` (exactement 80 % de 50 millions), et `clients_ref` contient les clients 0 à 100 000. (`range` et `element_at` sont propres à Spark SQL ; adapte pour Hive.)

### 1.2 Le laboratoire « miniature » : les tables de la série (vérifié)

Pour **vérifier la logique** des solutions sans cluster, on utilise `ventes_dedup` (13 lignes) et `clients_dedup` (6 lignes). Distribution des clés dans `ventes_dedup` :

| client_id | Lignes |
|---|---|
| **1** | **4** (31 % : la « clé chaude » du laboratoire) |
| 3 | 3 |
| 2 | 2 |
| NULL | 1 |
| 4, 5, 99 | 1 chacun |

Référence de comparaison, la jointure simple (ventes validées, jointure interne) :

```sql
SELECT c.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
FROM ventes_dedup v
JOIN clients_dedup c ON v.client_id = c.client_id
WHERE v.statut = 'VALIDEE'
GROUP BY c.pays
ORDER BY c.pays;
```

| pays | nb | ca |
|---|---|---|
| FR | 4 | 2405.00 |
| IT | 1 | 25.00 |
| MA | 3 | 300.00 |
| SN | 1 | 300.00 |

**Toutes les solutions du blog devront retrouver exactement ce résultat.** C'est le test de non-régression : une technique d'optimisation ne doit **jamais** changer le résultat.

---

## 2. Comment une jointure s'exécute en distribué

Dans un cluster, les lignes de deux tables sont réparties sur des dizaines de machines. Pour joindre `ventes` et `clients`, **les lignes qui ont la même clé doivent se retrouver au même endroit**. Il n'y a que deux façons d'y arriver :

1. **déplacer les deux tables** pour que les mêmes clés se rejoignent (*shuffle*) ;
2. **copier la petite table partout** pour que chaque machine puisse joindre localement (*broadcast*).

### 2.1 La jointure par shuffle

```
Table ventes (répartie sur N machines)          Table clients (répartie sur M machines)
         │ hash(client_id) % P                          │ hash(client_id) % P
         ▼                                              ▼
   ┌─────────────── réseau (Exchange) ────────────────────────┐
   ▼            ▼            ▼                 ▼
Partition 0  Partition 1  Partition 2  ...  Partition P-1
(toutes les lignes dont hash(client_id) % P = 0, des DEUX tables)
         → chaque partition joint localement ses lignes
```

Les **deux** tables sont repartitionnées par le **hachage de la clé de jointure** vers `P` partitions (200 par défaut pour `spark.sql.shuffle.partitions`). Toutes les lignes d'une même clé atterrissent **dans la même partition**. C'est précisément ce qui crée le *skew* (section 4).

### 2.2 Les quatre stratégies

| Stratégie | Principe | Shuffle de la grosse table ? | Condition |
|---|---|---|---|
| **Broadcast hash join** | La petite table est copiée à tous les exécuteurs ; chaque exécuteur joint localement | **Non** | Une table tient en mémoire (seuil de diffusion) |
| **Sort-merge join** | Shuffle des deux tables, **tri** de chaque partition, fusion | Oui (+ tri) | Jointure d'égalité, deux grosses tables : **défaut** de Spark |
| **Shuffle hash join** | Shuffle des deux tables, table de hachage sur le côté le plus petit de chaque partition | Oui (sans tri) | Un côté nettement plus petit, mais trop gros à diffuser |
| **Nested loop / produit cartésien** | Chaque ligne de gauche est comparée à chaque ligne de droite | Selon le cas | Jointure **sans** condition d'égalité (`<`, `BETWEEN`, ou aucune) : coûteux |

### 2.3 Les équivalents Hive et Impala

| Spark | Hive (Tez) | Impala |
|---|---|---|
| Broadcast hash join | **Map join** (`hive.auto.convert.join`) | `HASH JOIN [BROADCAST]` |
| Sort-merge join | **Merge join** / *common join* (shuffle) | `HASH JOIN [SHUFFLE]` (partitionné) |
| Sort-merge avec tables bucketées | **Bucket map join**, **SMB join** | — |
| Nested loop | Jointure croisée | `NESTED LOOP JOIN` |

### 2.4 Les règles de choix (Spark)

Dans l'ordre de priorité habituel :

1. un **hint** explicite (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`...) ;
2. un **broadcast** automatique si la **taille estimée** d'un côté est sous `spark.sql.autoBroadcastJoinThreshold` (10 Mo par défaut), avec les statistiques du Blog 5 ;
3. sinon, **sort-merge** pour une jointure d'égalité ;
4. sans condition d'égalité exploitable : **nested loop** ou produit cartésien.

Avec l'AQE (Blog 5), le choix peut être **révisé à l'exécution**, quand la taille réelle d'un côté (après filtres) est connue.

### 2.5 Ce qui limite le broadcast

- **La mémoire** : la petite table est chargée sur **chaque** exécuteur (et d'abord rassemblée sur le *driver*). Une table qui grossit peut provoquer une erreur mémoire ou un délai dépassé.
- **Le type de jointure** : on ne peut diffuser que le côté **non conservé**. Pour un `LEFT JOIN`, on diffuse la table de **droite** ; pour un `FULL OUTER JOIN`, on ne peut pas diffuser.
- **La taille réelle** : une table de 5 Mo compressée en Parquet peut peser bien plus en mémoire une fois décompressée.

---

## 3. Choisir et influencer la stratégie

### 3.1 Les hints

| Moteur | Hint | Effet |
|---|---|---|
| Spark | `/*+ BROADCAST(c) */` | Diffuse `c` |
| Spark | `/*+ MERGE(a, b) */`, `/*+ SHUFFLE_HASH(c) */` | Force sort-merge / shuffle hash |
| Hive | `/*+ MAPJOIN(c) */` | Force la jointure par diffusion |
| Impala | `JOIN [BROADCAST]`, `JOIN [SHUFFLE]` | Choisit la distribution |

```sql
SELECT /*+ BROADCAST(c) */ c.pays, SUM(v.montant) AS ca
FROM ventes_skew v
JOIN clients_ref c ON v.client_id = c.client_id
GROUP BY c.pays;
```

> **Un hint est un dernier recours.** Il fige une décision qui était vraie le jour de l'écriture : si `clients_ref` passe de 10 Mo à 5 Go, le *broadcast* forcé fait échouer le job. Corriger les **statistiques** (Blog 5) est plus durable.

### 3.2 Les filtres dynamiques

Certains moteurs savent **réduire la table volumineuse à l'exécution** grâce à un filtre construit sur la petite table : *dynamic partition pruning* (Spark ≥ 3), *dynamic partition pruning* et *semijoin reduction* (Hive), *runtime filters* (Impala). Ils sont décisifs pour une jointure entre une grosse table de faits partitionnée par date et une petite dimension filtrée. Vérifie qu'ils sont actifs (dans le plan, un sous-plan « dynamic pruning » ou des filtres d'exécution) : ils dépendent de la version et de la configuration.

### 3.3 Réduire avant de joindre

Les trois leviers qui précèdent toute jointure :

1. **Filtrer d'abord** : `WHERE` sur la grosse table (partitions, colonnes) avant la jointure (Blog 5).
2. **Ne garder que les colonnes utiles** (pas de `SELECT *`).
3. **Pré-agréger** quand le résultat n'a besoin que d'agrégats : chaque clé n'apparaît alors qu'**une seule fois** dans la grosse table.

```sql
-- Au lieu de joindre 50 millions de lignes puis d'agréger…
SELECT c.pays, SUM(a.ca) AS ca
FROM (
  SELECT client_id, SUM(montant) AS ca
  FROM ventes_skew
  GROUP BY client_id                     -- 50 M de lignes → au plus ~100 001 lignes
) a
JOIN clients_ref c ON a.client_id = c.client_id
GROUP BY c.pays;
```

La pré-agrégation **supprime le skew de la jointure** : la clé `0` n'a plus qu'une seule ligne côté ventes. Le `GROUP BY` reste concerné (section 8), mais il bénéficie de l'agrégation partielle avant le shuffle. **Condition de validité** : on n'a besoin que d'agrégats additifs (`SUM`, `COUNT`, `MIN`, `MAX`) par attribut de la dimension (c'est le principe de la moyenne des moyennes du Blog 1 : on conserve `SUM` et `COUNT`, pas `AVG`).

---

## 4. Le data skew : le mécanisme

### 4.1 Définition

Le **skew** (déséquilibre) est une répartition inégale des données entre partitions : **une petite minorité de partitions concentre la majorité du travail**. Un stage ne se termine que lorsque **sa tâche la plus lente** est finie : **le temps du stage est celui de la partition la plus chargée**, pas la moyenne.

### 4.2 Le calcul sur notre cas

Avec `P = 200` partitions de shuffle et 80 % des lignes sur une seule clé :

```
part de la partition « chaude »   ≈ 0,80 + (0,20 / 200)   ≈ 0,801   (la clé 0 + sa part des autres clés)
part moyenne d'une partition        = 1 / 200              = 0,005
rapport chaude / moyenne            ≈ 0,801 / 0,005        ≈ 160
```

Une partition reçoit **environ 160 fois** plus de données que la moyenne. Les 199 autres terminent vite (une vingtaine de secondes), celle-là reçoit environ 40 millions de lignes et dure 25 minutes. **Ajouter des machines ne change rien** : la clé `0` ne peut pas être répartie, elle va dans une seule partition. Même chose pour augmenter le nombre de partitions de shuffle : la clé chaude reste dans une seule.

### 4.3 D'où vient le skew ?

| Source | Exemple |
|---|---|
| **Valeurs par défaut** | `client_id = 0`, `-1`, `'N/A'`, `'UNKNOWN'`, une date `1970-01-01` |
| **Clés NULL** | Toutes les lignes à clé NULL partagent le même hachage |
| **Entités très populaires** | Un très gros client, un produit vedette, une ville dominante |
| **Robots et trafic anormal** | Un identifiant avec des millions d'événements (Blog 3, sessions) |
| **Pics temporels** | Fin de mois, soldes, jour de lancement |
| **Mauvaise clé** | Joindre sur une colonne à faible cardinalité (`pays`, `statut`) |

### 4.4 Le cas particulier des clés NULL

Dans une jointure **interne**, les lignes à clé NULL ne peuvent jamais correspondre (Blog 1) : elles sont écartées. L'optimiseur ajoute en général un filtre `isnotnull(clé)` avant le shuffle, donc elles ne créent **pas** de skew. Mais pour une jointure **externe** (`LEFT JOIN`), les lignes NULL du côté conservé **doivent rester** dans le résultat : elles sont envoyées dans la même partition et peuvent former un point chaud. La parade est de les **traiter à part** (section 7.2).

### 4.5 Effets observables

- **Une tâche très longue**, les autres très courtes (et un cluster inactif pendant ce temps) ;
- **Du *spill*** (débordement mémoire sur disque) sur la tâche chaude ;
- **Un `OutOfMemoryError`** ou une tâche tuée (*container killed*) : la tâche chaude dépasse la mémoire de l'exécuteur ;
- **Des tentatives répétées** (*retries*) de la même tâche, qui échoue chaque fois au même endroit.

---

## 5. Détecter le skew

### 5.1 Dans la Spark UI

Dans la page d'un stage, le tableau **Summary Metrics** donne des statistiques sur les tâches : *Min, 25th percentile, Median, 75th percentile, Max*. Pour détecter un skew, compare **Median** et **Max** sur :

| Métrique | Signal de skew |
|---|---|
| **Duration** | Max de plusieurs ordres de grandeur au-dessus de la médiane |
| **Shuffle Read Size / Records** | Une tâche lit beaucoup plus de données que les autres |
| **Spill (memory / disk)** | Présent sur une seule tâche ou très peu |
| **GC Time** | Élevé sur la tâche chaude |

Dans notre scénario, un exemple de tableau typique :

| Métrique | Min | Médiane | Max | Ratio Max / Médiane |
|---|---|---|---|---|
| Duration | 12 s | 20 s | 25 min | **75** |
| Shuffle Read Size | 40 Mo | 80 Mo | 18 Go | **225** |

(Valeurs d'illustration. Un ratio Max / Médiane de quelques unités est normal ; **plusieurs dizaines** signalent un skew.)

Sur Hive/Tez, on cherche la même chose dans les compteurs d'un *vertex* : une tâche avec un nombre d'enregistrements traités très supérieur aux autres. Sur Impala, le **profil** de requête montre le déséquilibre entre instances d'un même fragment.

### 5.2 Par une requête : les clés les plus fréquentes

```sql
SELECT
  client_id,
  COUNT(*)                                                  AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)        AS pct
FROM ventes_skew
GROUP BY client_id
ORDER BY n DESC
LIMIT 10;
```

On reconnaît l'**agrégat dans une window** du Blog 2 (section 8.3). Sur `ventes_skew`, attendu (par construction) : `client_id = 0` avec **40 000 000 lignes, soit 80 %**, puis des clés à quelques centaines de lignes. Sur notre miniature :

```sql
SELECT client_id, COUNT(*) AS n,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM ventes_dedup
GROUP BY client_id
ORDER BY n DESC, client_id NULLS LAST;
```

| client_id | n | pct |
|---|---|---|
| 1 | 4 | 30.77 |
| 3 | 3 | 23.08 |
| 2 | 2 | 15.38 |
| 4 | 1 | 7.69 |
| 5 | 1 | 7.69 |
| 99 | 1 | 7.69 |
| NULL | 1 | 7.69 |

(Le `GROUP BY` regroupe bien les NULL en un seul groupe, Blog 1. `NULLS LAST` fixe leur position : elle varie selon le moteur.)

Sur une **très grosse table**, lis un **échantillon** pour ne pas payer un scan complet : `TABLESAMPLE (1 PERCENT)` (Hive, Spark). Le résultat est approximatif, mais un point chaud à 80 % se voit très bien.

### 5.3 La règle de décision

| Part de la clé la plus fréquente | Lecture |
|---|---|
| Autour de `1 / P` (ici 0,5 %) | Pas de skew |
| Quelques pour cent | À surveiller, l'AQE suffit souvent |
| Plusieurs dizaines de pour cent | **Skew sévère** : il faut une solution explicite |

Le seuil qui compte se calcule : **la part de la clé chaude multipliée par le nombre de partitions** donne le rapport à la moyenne. À 0,8 × 200 = **160**, c'est sévère. À 5 % × 200 = **10**, c'est modéré.

---

## 6. Remèdes I : éviter le shuffle ou laisser le moteur corriger

### 6.1 Remède 1 : le broadcast (quand c'est possible)

Si l'un des deux côtés est petit, **diffuse-le**. La grosse table n'est plus repartitionnée par clé : **il n'y a plus de partition chaude**, donc plus de skew sur la jointure.

```sql
SELECT /*+ BROADCAST(c) */ c.pays, SUM(v.montant) AS ca
FROM ventes_skew v
JOIN clients_ref c ON v.client_id = c.client_id
GROUP BY c.pays;
```

Ici `clients_ref` (100 001 lignes, quelques Mo) se diffuse facilement. Dans le plan, on cherche `BroadcastHashJoin` et **l'absence d'`Exchange hashpartitioning(client_id)`** sur `ventes_skew`. C'est la solution **la plus simple et la plus efficace** : **vérifie-la toujours en premier**.

Limites : mémoire (section 2.5), type de jointure, et la table qui grossit un jour.

### 6.2 Remède 2 : l'AQE (*skew join* adaptatif de Spark)

Spark ≥ 3, avec l'AQE activée, peut **détecter et découper automatiquement** les partitions déséquilibrées d'un sort-merge join : la partition chaude du côté volumineux est **scindée en plusieurs sous-partitions**, et la partition correspondante de l'autre côté est répliquée.

| Paramètre | Rôle |
|---|---|
| `spark.sql.adaptive.enabled` | Active l'AQE |
| `spark.sql.adaptive.skewJoin.enabled` | Active la gestion du skew de jointure |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | Une partition est « skewed » si elle dépasse ce multiple de la médiane… |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | …**et** ce seuil de taille en octets |

> **Ne copie pas de valeurs de seuils** trouvées ailleurs : les valeurs par défaut et les noms exacts varient selon la version de Spark. Lis la documentation de **ta** version.

Limites à connaître : elle ne traite que certaines **jointures** (le côté qui peut être scindé dépend du type de jointure : par exemple, elle ne s'applique pas à un `FULL OUTER JOIN`) ; elle ne règle **pas** un skew dans un `GROUP BY` ou une window ; et elle laisse la partition chaude **intacte si sa taille reste sous les seuils**. C'est un excellent **filet de sécurité**, pas une garantie : vérifie dans l'onglet SQL de la Spark UI le plan **final** (section 4.5 du Blog 5).

### 6.3 L'équivalent Hive et Impala

- **Hive** : `hive.optimize.skewjoin=true` (avec un seuil `hive.skewjoin.key` en nombre de lignes par clé) permet une gestion du skew à l'exécution : les clés très fréquentes sont jointes par une *map join* à part. Une variante à la compilation existe pour les tables déclarées `SKEWED BY`. **Disponibilité et comportement dépendent de la version et du moteur d'exécution** (Tez).
- **Impala** : **pas de traitement automatique du skew de jointure** ; on utilise les *hints* `[BROADCAST]` / `[SHUFFLE]` et les solutions manuelles de la section 7.

### 6.4 Remède 3 : la pré-agrégation

Déjà vue en 3.3. Quand le résultat attendu est un agrégat, **joindre après avoir agrégé** réduit les lignes de 50 millions à ~100 000 et fait disparaître le point chaud de la jointure. Si ça convient au besoin, **c'est souvent la correction la plus propre**.

---

## 7. Remèdes II : isolation, salting, correction à la source

### 7.1 Remède 4 : isoler la clé chaude

L'idée : **traiter séparément** les lignes de la clé chaude et les autres, puis recoller avec `UNION ALL`.

- **Branche froide** : la jointure normale, **sans** la clé chaude. Les données sont équilibrées, le shuffle se passe bien.
- **Branche chaude** : les lignes de la clé chaude sont jointes à **une seule ligne** de la dimension. Cette ligne unique est diffusée (*broadcast*) : **aucun shuffle**, donc aucun point chaud.

Sur la miniature (clé chaude : `client_id = 1`, jointure interne, ventes validées) :

```sql
SELECT pays, SUM(nb) AS nb, SUM(ca) AS ca
FROM (
  -- branche froide : tout sauf la clé chaude
  SELECT c.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
  FROM ventes_dedup v
  JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE' AND v.client_id <> 1
  GROUP BY c.pays

  UNION ALL

  -- branche chaude : la clé 1, jointe à UNE ligne de la dimension
  SELECT c.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
  FROM ventes_dedup v
  JOIN (SELECT client_id, pays FROM clients_dedup WHERE client_id = 1) c
    ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE'
  GROUP BY c.pays
) t
GROUP BY pays
ORDER BY pays;
```

Résultat : **identique à la jointure simple** (FR 4 / 2405, IT 1 / 25, MA 3 / 300, SN 1 / 300). 

**Le piège du NULL dans la branche froide.** `v.client_id <> 1` **écarte les lignes à `client_id` NULL** (`NULL <> 1` est inconnu, Blog 1). Pour une jointure **interne**, ça ne change rien (les NULL n'auraient pas joint). Mais pour un **`LEFT JOIN`**, ces lignes doivent rester. Sur la miniature, `client_id <> 1` ne garde que **8 lignes** alors que `client_id IS NULL OR client_id <> 1` en garde **9** : la vente 107 (clé NULL) disparaîtrait.

```sql
-- Version LEFT JOIN, toutes ventes, le NULL est conservé
SELECT pays, SUM(nb) AS nb, SUM(ca) AS ca
FROM (
  SELECT COALESCE(c.pays, 'INCONNU') AS pays, COUNT(*) AS nb, SUM(v.montant) AS ca
  FROM ventes_dedup v
  LEFT JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.client_id IS NULL OR v.client_id <> 1          -- NE PAS écrire seulement « <> 1 »
  GROUP BY COALESCE(c.pays, 'INCONNU')

  UNION ALL

  SELECT c.pays, COUNT(*), SUM(v.montant)
  FROM ventes_dedup v
  JOIN (SELECT client_id, pays FROM clients_dedup WHERE client_id = 1) c
    ON v.client_id = c.client_id
  GROUP BY c.pays
) t
GROUP BY pays
ORDER BY pays;
```

| pays | nb | ca |
|---|---|---|
| FR | 5 | 2495.00 |
| INCONNU | 2 | 175.00 |
| IT | 1 | 25.00 |
| MA | 3 | 300.00 |
| SN | 2 | 380.00 |

(Même résultat qu'un `LEFT JOIN` simple groupé par pays : 13 lignes en tout, dont 2 sans client connu, les ventes 107 et 108.) On peut aussi écrire la condition avec une comparaison null-safe : `NOT (v.client_id <=> 1)`.

**Quand elle est la meilleure.** Quand **une ou deux** clés concentrent le problème et qu'on les connaît (valeurs par défaut, gros client, NULL). **Bonus important** : si la clé chaude **n'a pas besoin de la dimension** (c'est le cas d'un client « invité » sans attributs), la branche chaude **ne fait plus de jointure du tout** : on affecte une constante. C'est ce qu'on fera pour notre incident (section 10).

**Ses limites.** Il faut **connaître** les clés chaudes (une requête de détection, section 5) ; la liste peut évoluer ; la requête est plus longue et à maintenir.

### 7.2 Traiter les NULL à part

Pour un `LEFT JOIN` volumineux où les clés NULL sont nombreuses, on peut même sortir la branche NULL : elles ne joindront jamais, donc on leur colle directement des colonnes vides.

```sql
-- Branche NULL : aucune jointure, colonnes de la dimension à NULL
SELECT v.vente_id, v.montant, CAST(NULL AS STRING) AS pays
FROM ventes_skew v
WHERE v.client_id IS NULL
```

à recoller (`UNION ALL`) avec la jointure des lignes à clé non NULL. Les lignes qui devaient disparaître de la jointure interne sont ainsi **conservées sans coût de shuffle**.

### 7.3 Remède 5 : le salting

Le **salting** (« salage ») **casse artificiellement** la clé chaude en plusieurs sous-clés pour que ses lignes se répartissent sur plusieurs partitions.

1. Côté **volumineux** : on ajoute à chaque ligne un **sel** (un petit entier de 0 à `S - 1`), et on joint sur `(clé, sel)`.
2. Côté **petit** : chaque ligne est **répliquée `S` fois**, une par valeur de sel, pour que toute combinaison trouve son partenaire.

```
Avant :  clé 1 → UNE partition (4 lignes ici, 40 millions dans le vrai cas)
Après :  (1, sel 0) → partition A     (1, sel 1) → partition B     (S sous-clés → S partitions)
```

**Sur la miniature (S = 2, sel = `pmod(vente_id, 2)`).** Les 4 lignes du client 1 se répartissent :

| vente_id | sel |
|---|---|
| 101 | 1 |
| 102 | 0 |
| 110 | 0 |
| 201 | 1 |

**Version générale** (on sale tout le monde, la dimension est répliquée `S` fois : 6 × 2 = **12 lignes**) :

```sql
WITH v AS (
  SELECT vente_id, client_id, montant, statut, pmod(vente_id, 2) AS salt
  FROM ventes_dedup
),
cl AS (
  SELECT c.client_id, c.pays, s.salt
  FROM clients_dedup c
  LATERAL VIEW explode(array(0, 1)) s AS salt          -- chaque client dupliqué pour chaque sel
)
SELECT cl.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
FROM v
JOIN cl ON v.client_id = cl.client_id AND v.salt = cl.salt
WHERE v.statut = 'VALIDEE'
GROUP BY cl.pays
ORDER BY cl.pays;
```

**Version sélective** (on ne sale que la clé chaude, la dimension passe de 6 à **7 lignes** seulement) :

```sql
WITH v AS (
  SELECT vente_id, client_id, montant, statut,
         CASE WHEN client_id = 1 THEN pmod(vente_id, 2) ELSE 0 END AS salt
  FROM ventes_dedup
),
cl AS (
  SELECT client_id, pays, 0 AS salt FROM clients_dedup
  UNION ALL
  SELECT client_id, pays, 1 AS salt FROM clients_dedup WHERE client_id = 1
)
SELECT cl.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
FROM v
JOIN cl ON v.client_id = cl.client_id AND v.salt = cl.salt
WHERE v.statut = 'VALIDEE'
GROUP BY cl.pays
ORDER BY cl.pays;
```

Les deux versions renvoient **exactement** le résultat de la jointure simple (FR 4 / 2405, IT 1 / 25, MA 3 / 300, SN 1 / 300). C'est la preuve que le salting préserve le résultat : chaque ligne de gauche trouve **un et un seul** partenaire à droite.

**Choisir `S`.** Pour que la sous-partition chaude retombe au niveau d'une partition moyenne :

```
S ≈ part de la clé chaude × nombre de partitions
  ≈ 0,80 × 200 = 160   (pour équilibrer exactement)
```

En pratique, on prend un `S` **plus modeste** (quelques dizaines) : au-delà d'un certain point, le coût de la réplication ne vaut plus le gain. Avec `S = 16`, la clé chaude passe de ~160 fois la moyenne à ~10 fois.

**Le sel doit être déterministe.** On voit souvent `floor(rand() * S)` comme sel. **C'est risqué** : si Spark doit **recalculer** une partie du stage (panne d'un exécuteur, relance), `rand()` peut produire **d'autres valeurs** : des lignes peuvent alors se retrouver sans partenaire, ou être comptées deux fois, **sans erreur visible**. Préfère un sel **dérivé d'une colonne stable** de la ligne (comme `pmod(vente_id, S)` ou `pmod(hash(vente_id), S)`), qui donne toujours la même valeur pour la même ligne.

**Ce que ça coûte.**

| Coût | Détail |
|---|---|
| Réplication de la dimension | `×S` (version générale) ou `+ (S-1)` lignes (sélective) |
| Complexité | Clé de jointure composite, requête plus difficile à lire et à maintenir |
| Risque | Résultat faux si le sel n'est pas cohérent des deux côtés |

**Quand l'utiliser.** Quand les deux côtés sont **trop gros pour un broadcast**, que la clé chaude **a besoin de la dimension**, et que l'AQE ne suffit pas.

### 7.4 Remède 6 : corriger à la source

Le skew est souvent un **symptôme de qualité de données**, pas une fatalité technique. Questions à poser :

- **Pourquoi 80 % des ventes ont `client_id = 0` ?** Ventes anonymes ? Erreur d'ingestion ? Une **valeur sentinelle** cache souvent une absence d'information.
- Remplacer `0` par **`NULL`** et ajouter un indicateur (`est_anonyme`). Les NULL se traitent à part (section 7.2) et ne polluent plus les jointures.
- **Séparer** les flux dès l'ingestion (ventes identifiées et ventes anonymes) : deux tables, deux logiques.
- **Documenter** les valeurs sentinelles autorisées (data contract, Blog 9).

> Une bonne correction à la source **supprime le problème pour tous les consommateurs** de la table, au lieu de le contourner requête par requête.

### 7.5 Quel remède choisir ?

| Remède | À privilégier quand | Coût / risque | Complexité |
|---|---|---|---|
| **1. Broadcast** | Un côté tient en mémoire | Mémoire, table qui grossit | Très faible |
| **3. Pré-agrégation** | Le résultat n'a besoin que d'agrégats additifs | Valable seulement si la sémantique le permet | Faible |
| **2. AQE / skew join Hive** | Moteur récent, skew modéré | Partition chaude parfois sous le seuil, pas de garantie | Très faible |
| **4. Isolation de la clé chaude** | Une ou deux clés connues, dont NULL ou valeur par défaut | Requête plus longue, liste à maintenir | Moyenne |
| **5. Salting** | Deux grosses tables, clé chaude qui **a besoin** de la dimension | Réplication, clé composite, sel déterministe | Élevée |
| **6. Source** | Valeur sentinelle ou erreur de collecte | Dépend d'une autre équipe | Variable, mais durable |

**Ordre de bataille recommandé** : *(a)* vérifier si on peut **broadcaster** ; *(b)* regarder si la **pré-agrégation** convient ; *(c)* laisser l'**AQE** travailler et vérifier son effet ; *(d)* sinon **isoler** la clé chaude ; *(e)* le **salting** en dernier ; *(f)* **corriger la source** en parallèle.

---

## 8. Le skew dans les agrégations et les windows

### 8.1 `GROUP BY`

Un `GROUP BY` sur une clé déséquilibrée est **moins touché** qu'une jointure pour les agrégats simples (`SUM`, `COUNT`, `MIN`, `MAX`) : le moteur fait une **agrégation partielle** avant le shuffle (le `partial_sum` du Blog 5), donc la clé chaude envoie **un sous-total par tâche** et non 40 millions de lignes. Le skew se manifeste surtout avec :

- **`COUNT(DISTINCT)`** et les agrégats **non additifs** (médiane, `collect_list`, `collect_set`, Blog 4), qui doivent voir toutes les valeurs d'une clé au même endroit ;
- les **windows** (section 8.3).

### 8.2 L'agrégation en deux phases avec sel

Même idée que le salting : on agrège d'abord **par `(clé, sel)`**, puis on **recombine** par clé. Sur la miniature :

```sql
SELECT client_id, SUM(nb) AS nb, SUM(ca) AS ca
FROM (
  SELECT client_id, pmod(vente_id, 2) AS salt, COUNT(*) AS nb, SUM(montant) AS ca     -- phase 1
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
  GROUP BY client_id, pmod(vente_id, 2)
) t
GROUP BY client_id                                                                     -- phase 2
ORDER BY client_id;
```

Phase 1, pour le client 1 : `sel 0 → (1 ligne, 25.00)`, `sel 1 → (2 lignes, 1280.00)`. Phase 2 : `(3 lignes, 1305.00)`. Le résultat final est **identique à un `GROUP BY client_id` direct** :

| client_id | nb | ca |
|---|---|---|
| NULL | 1 | 25.00 |
| 1 | 3 | 1305.00 |
| 2 | 1 | 300.00 |
| 3 | 3 | 300.00 |
| 4 | 1 | 1100.00 |
| 5 | 1 | 25.00 |
| 99 | 1 | 150.00 |

**Condition** : la fonction doit être **décomposable** : `SUM` de sous-sommes, `COUNT` en `SUM` de sous-comptes, `MIN` / `MAX`. Pas `AVG` directement (on conserve `SUM` et `COUNT`, Blog 1).

**`COUNT(DISTINCT)`** : on déduplique d'abord par `(clé, valeur)`, puis on compte :

```sql
SELECT client_id, COUNT(*) AS nb_produits_distincts
FROM (SELECT client_id, produit FROM ventes_dedup GROUP BY client_id, produit) t
GROUP BY client_id;
```

Hive propose `hive.groupby.skewindata=true`, qui introduit automatiquement une étape intermédiaire de ce type (selon la version et le moteur).

### 8.3 Les windows

`PARTITION BY` sur une clé chaude envoie **toute la partition dans une seule tâche** (Blog 3, section 11), et le sel ne se transpose pas directement (une window a besoin de **toutes** les lignes d'une partition). Pistes : **partitionner plus finement** quand le besoin métier le permet (par exemple par `client_id` **et** `mois`), **filtrer** les clés aberrantes (robots) avant, ou **agréger avant** la window pour réduire les lignes.

---

## 9. Les autres causes de jointure lente

Toute jointure lente n'est pas un skew. Avant de saler quoi que ce soit, **élimine les autres suspects** :

| Suspect | Comment le reconnaître | Remède |
|---|---|---|
| **Clés dupliquées** (jointure plusieurs-à-plusieurs) | Le nombre de lignes **en sortie explose** par rapport aux entrées (Blog 1, section 8.6) | Dédupliquer le côté « un », vérifier l'unicité |
| **Jointure non équi** (`<`, `BETWEEN`, aucune condition) | `BroadcastNestedLoopJoin` ou `CartesianProduct` dans le plan | Ajouter une clé d'égalité (par exemple un intervalle de dates en tranches), ou restreindre |
| **Types différents** | `cast` sur la clé dans le plan | Aligner les types **à l'ingestion** |
| **Jointure sur expression** | `ON UPPER(a.code) = UPPER(b.code)` | Normaliser en amont |
| **Trop peu ou trop de partitions de shuffle** | *Spill* partout (trop peu) ou milliers de mini-tâches (trop) | Ajuster, ou laisser l'AQE fusionner |
| **Table à diffuser trop grosse** | Erreur mémoire ou délai de *broadcast* dépassé | Retirer le hint, passer en shuffle |
| **Statistiques absentes** | `SortMergeJoin` sur une petite table | `ANALYZE` / `COMPUTE STATS` (Blog 5) |
| **Lecture trop volumineuse** | *Input Size* disproportionné | Partitionnement, colonnes, format (Blog 5) |

### L'arbre de décision

```
Une tâche est-elle très supérieure à la médiane (durée ou shuffle read) ?
 ├─ OUI → skew.   Une clé concentre-t-elle les lignes ? (requête de la section 5.2)
 │         ├─ broadcast possible ? → remède 1
 │         ├─ agrégats seulement ? → remède 3
 │         └─ sinon AQE, isolation, salting, source
 └─ NON → toutes les tâches sont lentes
          ├─ sortie qui explose ?          → clés dupliquées
          ├─ spill sur toutes les tâches ? → partitions trop grosses
          ├─ NestedLoop / Cartesian ?      → jointure non équi
          └─ volume lu énorme ?            → pruning, format, colonnes (Blog 5)
```

---

## 10. Résolution de l'incident

### 10.1 Le diagnostic

1. **Constat** : Spark UI, stage de jointure : durée et *Shuffle Read Size* du Max à des dizaines (voire centaines) de fois la médiane.
2. **Hypothèse** : un point chaud. **Confirmation** par la requête de la section 5.2 : `client_id = 0` porte **40 000 000 des 50 000 000 lignes (80 %)**.
3. **Calcul** : 0,80 × 200 ≈ **160 fois** la moyenne : un skew sévère, que l'ajout de machines ne résout pas.
4. **Question de fond** : que représente le client `0` ? **Des ventes anonymes**, qui **n'ont pas d'attributs** à aller chercher dans `clients_ref`.

### 10.2 La correction retenue : isoler la clé chaude, sans jointure

Comme la clé chaude **n'a pas besoin de la dimension**, la branche chaude devient une simple **constante** (aucun shuffle) ; la branche froide se joint normalement et est équilibrée.

```sql
WITH chaud AS (
  SELECT vente_id, montant, CAST('INCONNU' AS STRING) AS pays
  FROM ventes_skew
  WHERE client_id = 0
),
froid AS (
  SELECT v.vente_id, v.montant, c.pays
  FROM ventes_skew v
  JOIN clients_ref c ON v.client_id = c.client_id
  WHERE v.client_id <> 0
)
SELECT pays, COUNT(*) AS nb, SUM(montant) AS ca
FROM (
  SELECT vente_id, montant, pays FROM chaud
  UNION ALL
  SELECT vente_id, montant, pays FROM froid
) t
GROUP BY pays;
```

(Dans le laboratoire, la ligne `0` de `clients_ref` porte le pays `INCONNU` : la jointure simple et cette version donnent donc **le même résultat**. Dans un vrai projet, **vérifie cette équivalence** avant de remplacer une jointure par une constante.)

### 10.3 Les garde-fous

```sql
-- 1) Aucune ligne perdue ni dupliquée : les deux comptes doivent être égaux (50 000 000)
SELECT
  (SELECT COUNT(*) FROM ventes_skew) AS avant,
  (SELECT SUM(nb) FROM ( ...la requête de 10.2... ) r) AS apres;

-- 2) Les 40 000 000 ventes anonymes sont bien étiquetées INCONNU
--    (la ligne « INCONNU » du résultat doit afficher nb = 40 000 000)
```

Même principe qu'aux Blogs 1 et 2 : **on compare les comptes avant et après**, et on **réconcilie** avec la jointure simple sur un échantillon ou sur la miniature.

### 10.4 Filet de sécurité et correction durable

- **Filet** : laisser l'AQE (et son *skew join*) activée pour les clés chaudes futures, et surveiller le ratio Max / Médiane des tâches.
- **Source** : remplacer la valeur sentinelle `0` par `NULL` dans les données et ajouter un indicateur d'anonymat (section 7.4).
- **Broadcast** : comme `clients_ref` est petite, vérifier qu'elle est bien diffusée (`BroadcastHashJoin`) dans la branche froide, par ses statistiques plutôt que par un hint.

### 10.5 Mesures à noter (à remplir sur ton cluster)

| Mesure | Avant | Après |
|---|---|---|
| Durée totale du job | | |
| Durée de la tâche la plus lente | | |
| Ratio Max / Médiane (durée) | | |
| Ratio Max / Médiane (*Shuffle Read*) | | |
| *Spill* | | |
| Stratégie de jointure (branche froide) | | |

> Ce qu'on attend, **structurellement** : un ratio Max / Médiane de quelques unités, plus de tâche isolée, et une durée qui retombe vers celle de la tâche **médiane** plutôt que celle de la tâche chaude. Les chiffres, **tu les mesures**.

### Récapitulatif

| Question | Réponse |
|---|---|
| Pourquoi une seule tâche est lente ? | 80 % des lignes ont la même clé : hachage identique, **une** partition reçoit environ 160 fois la moyenne |
| Pourquoi ajouter des machines n'aide pas ? | La clé chaude ne peut pas être scindée par un shuffle classique |
| Remède retenu | Isoler la clé chaude : branche chaude sans jointure, branche froide équilibrée |
| Filet de sécurité | AQE (*skew join*) |
| Correction durable | Remplacer la valeur sentinelle `0` à la source |

---

## 11. 10 questions d'entretien

**1. Comment le moteur exécute-t-il une jointure en distribué ?**
Soit en **repartitionnant les deux tables par le hachage de la clé** (shuffle : sort-merge ou shuffle hash join), pour que les mêmes clés se retrouvent dans la même partition ; soit en **diffusant la petite table** à tous les exécuteurs (broadcast hash join), ce qui évite de déplacer la grosse.

**2. Broadcast, sort-merge ou shuffle hash : quelle différence ?**
Broadcast : pas de shuffle de la grosse table, exige une petite table. Sort-merge : shuffle + tri des deux côtés, défaut pour deux grosses tables. Shuffle hash : shuffle sans tri, avec une table de hachage sur le plus petit côté de chaque partition.

**3. Qu'est-ce que le data skew et comment le détectes-tu ?**
Une répartition inégale des données entre partitions : quelques-unes concentrent le travail, et la durée du stage est celle de la plus lente. Détection : Spark UI (Max vs Médiane de la durée et du *Shuffle Read*), et requête sur les clés les plus fréquentes (`GROUP BY clé ORDER BY COUNT(*) DESC`).

**4. Pourquoi ajouter des machines ou des partitions de shuffle ne règle pas un skew de clé ?**
Parce que toutes les lignes d'une même clé vont dans **la même partition**, quel que soit leur nombre. Le point chaud reste unique.

**5. Les clés NULL créent-elles du skew ?**
Pas pour une jointure interne (l'optimiseur écarte les lignes à clé NULL avant le shuffle). Oui pour une jointure externe, où elles doivent être conservées et se regroupent. On les traite à part avec une branche dédiée.

**6. Explique le salting. Que coûte-t-il ?**
On ajoute un sel à la clé du côté volumineux pour répartir la clé chaude sur `S` sous-clés, et on réplique `S` fois (ou seulement pour les clés chaudes) l'autre côté. Coût : réplication, clé composite, complexité. Le sel doit être déterministe.

**7. Pourquoi un sel `rand()` est-il risqué ?**
Si une tâche est recalculée, `rand()` peut donner d'autres valeurs : des lignes perdent leur partenaire ou sont comptées deux fois, sans erreur. On dérive le sel d'une colonne stable (`pmod(id, S)`).

**8. Qu'est-ce que le *skew join* de l'AQE et quelles sont ses limites ?**
Spark scinde automatiquement les partitions déséquilibrées du côté volumineux et réplique la partition correspondante de l'autre côté. Limites : seulement certaines jointures, rien pour les `GROUP BY` ou windows, dépend de seuils, comportement variable selon la version.

**9. Pourquoi la pré-agrégation peut-elle supprimer le skew ?**
Après un `GROUP BY` sur la clé, chaque clé n'apparaît qu'une fois : le point chaud disparaît avant la jointure. Valable si l'on n'a besoin que d'agrégats additifs.

**10. Comment traites-tu le skew dans un `GROUP BY` ou une window ?**
`GROUP BY` : l'agrégation partielle atténue le problème pour les agrégats simples ; pour les non-additifs ou `COUNT(DISTINCT)`, agrégation en deux phases avec sel ou déduplication préalable. Window : partitionner plus finement, filtrer les clés aberrantes, ou agréger avant.

---

## 12. 5 exercices

### Exercice 1 : le calcul du skew
Une table de 100 millions de lignes est jointe sur une clé dont **50 %** des lignes ont la même valeur, avec **400** partitions de shuffle. Calcule le rapport partition chaude / moyenne, puis une valeur de `S` pour le salting.

<details>
<summary>Solution</summary>

Part de la partition chaude ≈ 0,50 + 0,50 / 400 ≈ **0,50125**. Part moyenne = 1 / 400 = **0,0025**. Rapport ≈ **200**. Pour ramener la sous-partition chaude au niveau d'une partition moyenne : `S ≈ 0,5 × 400 = 200`. En pratique, un `S` plus modeste (quelques dizaines) réduit déjà le déséquilibre d'un facteur important (avec `S = 20`, le rapport passe à ~10) en limitant la réplication de la dimension. On sale **sélectivement** (clé chaude seulement) pour ne répliquer que `S - 1` lignes supplémentaires par ligne chaude de la dimension.
</details>

### Exercice 2 : l'isolation avec un `LEFT JOIN`
Sur la miniature, écris la jointure `ventes_dedup` → `clients_dedup` (**toutes** les ventes, jointure externe, pays `INCONNU` quand le client est absent), en isolant la clé `1`, sans perdre la vente 107. Pourquoi `client_id <> 1` seul est-il faux ?

<details>
<summary>Solution</summary>

La requête de la section 7.1 (version `LEFT JOIN`), dont le résultat doit être :

| pays | nb | ca |
|---|---|---|
| FR | 5 | 2495.00 |
| INCONNU | 2 | 175.00 |
| IT | 1 | 25.00 |
| MA | 3 | 300.00 |
| SN | 2 | 380.00 |

Avec `WHERE v.client_id <> 1` seul, la vente 107 (`client_id` NULL) est écartée (`NULL <> 1` est inconnu) : **8 lignes** au lieu de 9 dans la branche froide, et `INCONNU` tombe à 1 ligne au lieu de 2 (le total à 12 lignes au lieu de 13). La condition correcte : `v.client_id IS NULL OR v.client_id <> 1`.
</details>

### Exercice 3 : le sel qui fait disparaître des lignes
Un collègue sale avec `floor(rand() * 4)` côté grosse table et réplique la dimension 4 fois. Le job réussit, mais le CA varie légèrement d'une exécution à l'autre après une panne d'exécuteur. Pourquoi, et comment corriger ?

<details>
<summary>Solution</summary>

Après une panne, Spark **recalcule** la partie perdue du stage ; `rand()` produit alors des sels **différents** pour les mêmes lignes. Certaines se retrouvent associées à un sel déjà traité ailleurs (comptées deux fois) ou rejoignent un partenaire qui n'est plus au bon endroit (perdues), sans erreur. Correction : un sel **déterministe**, dérivé d'une colonne stable, par exemple `pmod(vente_id, 4)` ou `pmod(hash(vente_id), 4)`. Ajouter un contrôle : le nombre de lignes en sortie doit être égal au nombre de lignes de la grosse table.
</details>

### Exercice 4 : choisir une stratégie
Jointure entre une table de **2 milliards** de lignes et une dimension de **5 millions** de lignes (≈ 300 Mo). La clé la plus fréquente représente **5 %** des lignes, avec 200 partitions de shuffle. Que fais-tu ?

<details>
<summary>Solution</summary>

Rapport au skew : 0,05 × 200 = **10** : un skew **modéré**, pas sévère. Démarche : (1) la dimension fait ~300 Mo : *broadcast* envisageable **si la mémoire des exécuteurs le permet** (la table est diffusée sur chacun, et rassemblée sur le driver) ; sinon à éviter. (2) Sinon, sort-merge avec **statistiques à jour** et **AQE** activée (skew join) : un rapport de 10 est un cas où l'AQE suffit en général. (3) Si le besoin n'est qu'un agrégat, **pré-agréger** la grosse table. (4) Le salting n'est justifié que si l'AQE ne suffit pas, **après mesure**. On vérifie dans la Spark UI le Max / Médiane du plan final.
</details>

### Exercice 5 : lire la Spark UI
Stage de jointure, tableau *Summary Metrics* : *Duration* min 8 s, médiane 15 s, max 32 min ; *Shuffle Read Size* médiane 64 Mo, max 22 Go ; une seule tâche avec 40 Go de *spill*. Diagnostic et premières actions ?

<details>
<summary>Solution</summary>

Ratios : durée 32 × 60 / 15 = **128**, shuffle read 22 Go / 64 Mo ≈ **350**. Une seule tâche concentre le volume et le *spill* : c'est un **skew de jointure**. Actions dans l'ordre : (1) requête des clés les plus fréquentes pour identifier la clé chaude et sa part ; (2) vérifier si un **broadcast** ou une **pré-agrégation** conviennent ; (3) contrôler que l'**AQE** et son *skew join* sont actifs et lire le plan final ; (4) sinon **isoler** la clé chaude (ou salage), en comparant le résultat à la jointure simple sur un échantillon ; (5) enquêter sur la **source** de la valeur dominante.
</details>

---

## 13. Checklist avant de livrer une jointure

- [ ] J'ai vérifié l'**unicité de la clé** du côté « un » et comparé les comptes avant / après (Blog 1)
- [ ] Je filtre et je projette **avant** de joindre (pruning, colonnes utiles)
- [ ] J'ai regardé si un **broadcast** est possible, par les **statistiques**, pas seulement par un hint
- [ ] Si le résultat n'est qu'un agrégat, j'ai envisagé la **pré-agrégation**
- [ ] J'ai calculé les **clés les plus fréquentes** de la grosse table (requête de la section 5.2)
- [ ] Pour chaque clé dominante, j'ai la **part × nombre de partitions** (rapport au skew)
- [ ] Les **clés NULL** et les **valeurs sentinelles** (`0`, `-1`, `'N/A'`) sont identifiées et traitées à part
- [ ] L'**AQE** et son *skew join* sont actifs, et j'ai lu le **plan final** dans la Spark UI
- [ ] Un éventuel **sel** est **déterministe** (dérivé d'une colonne stable, jamais `rand()`)
- [ ] Toute optimisation retrouve **exactement** le résultat de la jointure simple (test sur échantillon ou miniature)
- [ ] J'ai noté le ratio **Max / Médiane** des tâches avant et après
- [ ] J'ai ouvert un ticket pour **corriger la source** quand le skew vient d'une valeur sentinelle

---

## 14. Prochain blog et blogs restants

**Blog 7 : modélisation.** On quitte l'exécution pour la **structure** des données : tables de faits et de dimensions (*star schema*), clés de substitution, **historisation des changements** (SCD de type 1, 2 et 3), calendrier, et un aperçu du Data Vault. Les tables `ventes_dedup`, `clients_dedup` et le historique de segment du client 1 (Premium puis Gold) y servent de fil rouge.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions |
| ✅ 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE`, pivot |
| ✅ 5 | Performance I : lire un plan | `EXPLAIN`, formats, partitionnement, bucketing, statistiques, petits fichiers |
| ✅ 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| 7 | Modélisation | Star schema, SCD 1 / 2 / 3, calendrier, clés de substitution, Data Vault |
| 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

> Si ta série Spark contient déjà un blog jointures / skew / windows, les Blogs 3 et 6 de cette série SQL peuvent **fusionner ou se compléter** avec lui : ici, l'accent est mis sur le **raisonnement SQL** et les solutions **vérifiables sur la miniature**.

### Pour t'entraîner avant le Blog 7

Sans regarder la suite : (1) Alice (client 1) est passée du segment **Premium** à **Gold** le 15 mars. Quelles colonnes ajoutes-tu à la table clients pour **ne pas perdre** l'ancien segment ? (2) Comment retrouves-tu le segment d'Alice **à la date de chacune de ses ventes** ? (3) Pourquoi une simple mise à jour (`UPDATE`) casserait-elle les rapports passés ? Si tu hésites, le Blog 7 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de reproduire chaque expérience sur ton cluster, de noter tes propres mesures, et de toujours comparer le résultat d'une optimisation à celui de la requête d'origine.*