---
title: "SQL pour Data Engineers, Blog 8 : chargements, idempotence, CDC et MERGE"
description: "Construire des chargements incrémentaux rejouables avec CDC, MERGE et des tables transactionnelles."
weight: 8
---

# SQL pour Data Engineers, Blog 8 : chargements, idempotence, CDC et MERGE

> **Série** : SQL, de basique à tech lead · **Niveau** : 5/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive (tables ACID), Spark SQL, Impala (stack Cloudera). Les écarts de dialecte et de version sont signalés.
> **Prérequis** : [Blogs SQL 1 à 7]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-7-modelisation-star-schema-scd" >}}), en particulier le Blog 2 (déduplication avec `ROW_NUMBER`) et le Blog 7 (SCD2).

> **Ce qui a été vérifié, et ce qui ne l'a pas été.** La **logique** de chargement (rejeu d'un lot, application d'un flux de changements dans le bon ordre, suppressions, chevauchement de fenêtre, réécriture de partition) a été **exécutée sur SQLite** avec les tables de la série : **tous les chiffres du blog en proviennent**. SQLite n'a pas de `MERGE` : le `MERGE` SCD2 a été **émulé** (ligne à ligne, avec les règles `WHEN MATCHED` / `WHEN NOT MATCHED`) et recoupé avec la dimension du Blog 7. La **syntaxe** `MERGE` Hive / Spark / Impala, la prise en charge des tables transactionnelles (ACID, Iceberg, Delta, Kudu) selon ta plateforme et tous les comportements de **performance** n'ont **pas** été testés sur un cluster : à reproduire chez toi.

---

## 0. Pourquoi ce blog existe

Les blogs 1 à 7 ont construit des requêtes **justes** et **rapides** sur des tables **bien modélisées**. Reste la question la plus pratique : **comment alimenter ces tables chaque jour, sans doublons, sans trous, et en pouvant relancer un job qui a planté ?** C'est une question d'entretien « senior » à coup sûr : *« Ton job plante à 80 % et il est relancé. Que se passe-t-il ? »*

Le mot clé est **idempotence** : exécuter une opération **une fois ou dix fois** donne **le même résultat**.

Le scénario, en deux actes :

> **Mercredi, 6h30.** Le job `charger_ventes` plante à 80 % (une machine perd sa mémoire). On le **relance** à 7h. À 9h, la Finance appelle :
> **Acte 1** : *« Le CA validé du lot est de **6 410**. La source dit **3 205**. »*
> L'équipe décide alors de passer d'un chargement par instantanés à la lecture d'un **flux de changements** (insertions, modifications, suppressions). La première version du nouveau job donne, quelques jours plus tard :
> **Acte 2** : *« La vente 108 a été supprimée à la source mais elle est toujours dans l'entrepôt. La vente 110 est annulée à la source mais elle est encore « validée » chez nous. Notre CA validé est **3 475**, il devrait être **3 235**. »*

Quatre bugs, un seul principe : **un chargement doit être rejouable et appliquer les changements dans le bon ordre**.

### Plan

1. [Les données de ce blog](#1-les-données-de-ce-blog)
2. [L'idempotence](#2-lidempotence)
3. [Les stratégies d'écriture](#3-les-stratégies-décriture)
4. [Le chargement incrémental](#4-le-chargement-incrémental)
5. [Le CDC : appliquer un flux de changements](#5-le-cdc--appliquer-un-flux-de-changements)
6. [`MERGE` et les tables transactionnelles](#6-merge-et-les-tables-transactionnelles)
7. [Backfill et retraitement](#7-backfill-et-retraitement)
8. [L'anatomie d'un job idempotent](#8-lanatomie-dun-job-idempotent)
9. [Les pièges des chargements](#9-les-pièges-des-chargements)
10. [Résolution de l'incident](#10-résolution-de-lincident)
11. [10 questions d'entretien](#11-10-questions-dentretien)
12. [5 exercices](#12-5-exercices)
13. [Checklist avant de livrer un job de chargement](#13-checklist-avant-de-livrer-un-job-de-chargement)
14. [Prochain blog et blogs restants](#14-prochain-blog-et-blogs-restants)

---

## 1. Les données de ce blog

### 1.1 Le flux de changements : `ventes_cdc`

Le système source publie un **journal de changements** (CDC, *Change Data Capture*) : chaque ligne est un **événement** (`I` insertion, `U` modification, `D` suppression), avec un **numéro de séquence** (`seq`, l'ordre réel des changements à la source), un **rang d'arrivée** (`arrivee`, l'ordre dans lequel nous avons reçu l'événement) et un horodatage.

```sql
USE blog_sql;

CREATE TABLE ventes_cdc (
  seq        BIGINT,       -- ordre réel des changements (fourni par la source)
  arrivee    BIGINT,       -- ordre d'arrivée chez nous (peut différer de seq)
  op         STRING,       -- 'I', 'U' ou 'D'
  vente_id   BIGINT,
  client_id  INT,
  montant    DECIMAL(10,2),
  statut     STRING,
  date_vente DATE,
  ts         TIMESTAMP
) STORED AS PARQUET;

INSERT INTO ventes_cdc VALUES
  ( 1,  1, 'I', 101, 1,    1200.00, 'VALIDEE', DATE '2024-03-01', TIMESTAMP '2024-03-01 10:00:00'),
  ( 2,  2, 'I', 102, 1,      25.00, 'VALIDEE', DATE '2024-03-01', TIMESTAMP '2024-03-01 10:05:00'),
  ( 3,  3, 'I', 103, 2,      80.00, 'ANNULEE', DATE '2024-03-05', TIMESTAMP '2024-03-05 09:00:00'),
  ( 4,  4, 'I', 104, 3,     150.00, 'VALIDEE', DATE '2024-03-07', TIMESTAMP '2024-03-07 09:00:00'),
  ( 5,  5, 'I', 105, 3,       NULL, 'VALIDEE', DATE '2024-03-09', TIMESTAMP '2024-03-09 09:00:00'),
  ( 6,  6, 'I', 106, 4,    1100.00, 'VALIDEE', DATE '2024-03-10', TIMESTAMP '2024-03-10 09:00:00'),
  ( 7,  7, 'I', 107, NULL,   25.00, 'VALIDEE', DATE '2024-03-11', TIMESTAMP '2024-03-11 09:00:00'),
  ( 8,  8, 'I', 108, 99,    150.00, 'VALIDEE', DATE '2024-03-12', TIMESTAMP '2024-03-12 09:00:00'),
  ( 9,  9, 'I', 109, 2,     300.00, 'VALIDEE', DATE '2024-03-15', TIMESTAMP '2024-03-15 09:00:00'),
  (10, 10, 'I', 110, 1,      90.00, NULL,      DATE '2024-03-20', TIMESTAMP '2024-03-20 09:00:00'),
  -- changements d'avril
  (11, 17, 'U', 110, 1,      90.00, 'VALIDEE', DATE '2024-03-20', TIMESTAMP '2024-04-01 09:00:00'),  -- ARRIVE EN DERNIER (rang 17)
  (12, 12, 'U', 105, 3,     180.00, 'VALIDEE', DATE '2024-03-09', TIMESTAMP '2024-04-01 09:30:00'),  -- correction du montant
  (13, 13, 'D', 108, NULL,    NULL, NULL,      NULL,              TIMESTAMP '2024-04-01 10:00:00'),  -- suppression
  (14, 14, 'I', 201, 1,      80.00, 'VALIDEE', DATE '2024-04-02', TIMESTAMP '2024-04-02 09:00:00'),
  (15, 15, 'I', 202, 3,     150.00, 'VALIDEE', DATE '2024-04-05', TIMESTAMP '2024-04-05 09:00:00'),
  (16, 16, 'I', 203, 5,      25.00, 'VALIDEE', DATE '2024-04-08', TIMESTAMP '2024-04-08 09:00:00'),
  (17, 11, 'U', 110, 1,      90.00, 'ANNULEE', DATE '2024-03-20', TIMESTAMP '2024-04-10 08:00:00');  -- ARRIVE AVANT (rang 11)
```

Les événements **11 et 17** concernent la même vente (110) et sont arrivés **dans le désordre** : le plus récent (annulation du 10 avril, `seq = 17`) est arrivé **avant** le plus ancien (validation du 1er avril, `seq = 11`). Ça arrive souvent dès que les événements transitent par un système distribué.

### 1.2 L'état attendu (la « vérité »)

Si on applique **tous** les événements dans **l'ordre de `seq`** :

| Vente | État final |
|---|---|
| 105 | montant corrigé à **180.00** (au lieu de NULL) |
| 108 | **supprimée** |
| 110 | statut **ANNULEE** (annulation du 10 avril, le dernier changement) |
| 201, 202, 203 | insérées en avril |

Soit **12 lignes** (13 ventes moins la 108) et un **CA validé de 3 235** (10 ventes validées). Par mois : **mars 9 lignes / 2 980**, **avril 3 lignes / 255**.

### 1.3 Les tables cibles

```sql
CREATE TABLE ventes_cible (
  vente_id   BIGINT,
  client_id  INT,
  montant    DECIMAL(10,2),
  statut     STRING,
  date_vente DATE
) PARTITIONED BY (mois STRING)
STORED AS PARQUET;

-- Table de contrôle : jusqu'où a-t-on chargé ?
CREATE TABLE etl_controle (
  job        STRING,
  watermark  TIMESTAMP,
  derniere_execution TIMESTAMP
) STORED AS PARQUET;
```

> **Dialecte** : si les littéraux `DATE '...'` ou `TIMESTAMP '...'` sont refusés, utilise `CAST('...' AS DATE)` / `CAST('...' AS TIMESTAMP)`.

---

## 2. L'idempotence

### 2.1 Définition

Une opération est **idempotente** si l'exécuter **plusieurs fois** produit **le même résultat final** que l'exécuter une seule fois. En chargement de données :

> **Rejouer un job ne doit jamais créer de doublons ni changer des chiffres déjà justes.**

### 2.2 Pourquoi c'est indispensable

Un job est **relancé** bien plus souvent qu'on ne le croit :

| Cause | Exemple |
|---|---|
| **Panne** | Machine perdue, mémoire insuffisante, réseau (notre incident) |
| **Reprise automatique** | L'orchestrateur relance une tâche après un échec (*retry*) |
| **Livraison au moins une fois** | Un système de messages peut livrer **deux fois** le même événement |
| **Correction de données** | On relance parce qu'une source a été corrigée |
| **Backfill** | On recharge une période passée (section 7) |

On ne peut pas empêcher les relances. On peut rendre les jobs **sans conséquence** quand ils sont relancés. Une règle célèbre : **« exactement une fois » en pratique = « au moins une fois » + idempotence.**

### 2.3 Les quatre règles d'un traitement déterministe

Pour qu'un job soit rejouable, son résultat ne doit dépendre **que de ses entrées et de ses paramètres** :

1. **La date de traitement est un paramètre**, pas `current_date()` ni `current_timestamp()` (déjà vu pour les SCD, Blog 7). Rejouer le chargement d'hier doit produire le résultat d'hier.
2. **Rien d'aléatoire** : pas de `rand()`, pas de `uuid()` dans une clé métier (le sel aléatoire du Blog 6 est un exemple de piège).
3. **Un ordre toujours déterministe** : tout `ROW_NUMBER` a un critère de départage unique (Blog 3).
4. **L'écriture ne cumule pas** : elle **remplace** ou **fusionne par clé**, elle n'**ajoute** pas aveuglément.

---

## 3. Les stratégies d'écriture

### 3.1 Le panorama

| Stratégie | Idempotente ? | Gère les mises à jour ? | Gère les suppressions ? | Remarque |
|---|---|---|---|---|
| `INSERT INTO` (ajout) | **Non** | Non | Non | Un rejeu **duplique** |
| `INSERT INTO ... WHERE NOT EXISTS` (ajout des clés absentes) | Oui | **Non** | Non | Ignore les modifications |
| `INSERT OVERWRITE` (table ou partition) | **Oui** | Oui (par réécriture) | Oui (par réécriture) | Il faut recalculer **toute** la partition |
| `MERGE` / `UPSERT` par clé | **Oui** | **Oui** | Oui (avec `DELETE`) | Exige un format transactionnel (section 6) |
| `DELETE` puis `INSERT` dans une même transaction | Oui | Oui | Oui | Dépend du support transactionnel |

### 3.2 Le bug de l'acte 1 : `INSERT INTO` rejoué

Le job fait un `INSERT INTO ventes_cible SELECT ... FROM lot`. Il plante à 80 %, puis il est relancé : le même lot est inséré **une seconde fois**.

Résultat vérifié sur nos 13 ventes (lot de mars-avril) chargées **deux fois** :

| | Lignes | Clés distinctes | CA validé |
|---|---|---|---|
| Après **un** chargement | 13 | 13 | **3 205** |
| Après **deux** `INSERT INTO` | **26** | 13 | **6 410** |

Les **13** clés sont dupliquées. Aucune erreur, aucun avertissement : c'est la mécanique du doublon de clé du Blog 1 (section 8.6), **sans jointure**, pure et simple. Un `SELECT DISTINCT` n'est pas la solution (Blog 1) : il masque le symptôme.

### 3.3 Solution 1 : n'insérer que les clés absentes

```sql
INSERT INTO ventes_cible
SELECT s.*
FROM lot s
LEFT JOIN ventes_cible t ON s.vente_id = t.vente_id
WHERE t.vente_id IS NULL;
```

Rejoué deux fois, ce chargement donne **13 lignes et 3 205** : c'est **idempotent**. Mais attention : **il ignore les modifications**. Si la vente 110 passe de `NULL` à `VALIDEE` à la source, le lot suivant contient la nouvelle version, mais elle est **ignorée** car la clé 110 existe déjà : son statut reste `NULL` dans l'entrepôt. (Vérifié.) Cette stratégie ne convient qu'à des données **jamais modifiées** (événements immuables).

### 3.4 Solution 2 : réécrire la partition

On **remplace** entièrement la partition concernée par le résultat recalculé :

```sql
INSERT OVERWRITE TABLE ventes_cible PARTITION (mois = '2024-04')
SELECT vente_id, client_id, montant, statut, date_vente
FROM etat_courant                       -- le résultat recalculé pour avril
WHERE date_format(date_vente, 'yyyy-MM') = '2024-04';
```

Rejouée deux fois sur notre jeu de données : **la partition d'avril contient 3 lignes (255 de CA)** à chaque fois. **Idempotent**, et **prend en charge** les mises à jour et les suppressions (la partition est recalculée en entier). Son coût : il faut disposer de **toutes** les données de la partition, pas seulement des changements.

**Les pièges de `INSERT OVERWRITE`** :

- **L'étendue de l'écrasement.** Selon le moteur, le type de table et la configuration (par exemple `spark.sql.sources.partitionOverwriteMode`), un `INSERT OVERWRITE` avec partitions dynamiques peut **écraser seulement les partitions présentes** dans le `SELECT`, ou **toute la table**. **Vérifie sur ta plateforme avec une table de test avant la production.**
- **Une partition qui devient vide.** Si le `SELECT` ne renvoie **aucune ligne** pour une partition, certains moteurs **laissent l'ancienne partition intacte** au lieu de la vider. Teste le cas.
- **Pas d'atomicité garantie** sur du stockage de fichiers non transactionnel : un lecteur peut voir une partition à moitié écrite. On écrit dans une zone intermédiaire, on **valide**, puis on **publie** (section 8).

### 3.5 Solution 3 : `MERGE` (section 6)

La mise à jour **par clé** (`MERGE` ou `UPSERT`) est idempotente par construction : appliquer deux fois le même changement à la même clé donne le même état. Vérifié : appliquer deux fois le lot de changements donne **12 lignes et 3 235** à chaque fois.

---

## 4. Le chargement incrémental

### 4.1 Rechargement complet ou incrémental ?

| | **Complet** (*full refresh*) | **Incrémental** |
|---|---|---|
| Principe | On relit **toute** la source et on remplace la cible | On ne lit que ce qui a **changé** depuis le dernier chargement |
| Simplicité | Très simple, naturellement idempotent | Plus complexe (il faut savoir « ce qui a changé ») |
| Coût | Proportionnel à la **taille totale** | Proportionnel au **volume de changements** |
| Suppressions | **Naturellement** gérées | **Invisibles** si on filtre sur une date de modification |
| Quand | Petites tables, référentiels | Gros volumes |

### 4.2 Le filigrane (*watermark*)

Le schéma classique : mémoriser la **dernière date de modification chargée** dans une table de contrôle (`etl_controle`), puis ne lire que les lignes **plus récentes**.

```sql
SELECT *
FROM source
WHERE ts > (SELECT watermark FROM etl_controle WHERE job = 'charger_ventes');
```

### 4.3 La frontière : `>` ou `>=` ?

Avec nos événements, supposons que la dernière exécution se soit arrêtée au filigrane `2024-04-01 09:30:00`. L'événement 12 a **exactement** cet horodatage.

| Condition de lecture | Événements relus (`seq`) | Nombre |
|---|---|---|
| `ts > filigrane` | 13, 14, 15, 16, 17 | **5** |
| `ts >= filigrane` | **12**, 13, 14, 15, 16, 17 | **6** |
| `ts >= filigrane - 1 heure` (chevauchement) | 11, 12, 13, 14, 15, 16, 17 | **7** |

Avec la comparaison **stricte**, l'événement 12 (la correction du montant de la vente 105) est **perdu** s'il a été validé à la source juste après la lecture précédente. C'est une **perte silencieuse** : aucun doublon, aucune erreur, juste un montant resté à `NULL`.

**Les parades :**

1. **Une fenêtre de chevauchement** : on relit volontairement un peu **avant** le filigrane (une heure, un jour, selon les délais de la source). Les lignes déjà chargées sont relues, ce qui **n'a aucune conséquence si l'écriture est idempotente** (merge par clé). Voilà pourquoi **l'idempotence et l'incrémental vont ensemble**.
2. **Mettre à jour le filigrane seulement après un chargement réussi**, avec **le maximum des horodatages réellement lus** (`MAX(ts)` du lot), **pas** l'heure courante : sinon un événement qui arrive en retard est sauté.
3. **Le filigrane est mis à jour au même titre que l'écriture** : s'il avance et que l'écriture échoue, des lignes sont perdues ; s'il n'avance pas et que l'écriture réussit, on relit (inoffensif).

### 4.4 Les limites d'un filigrane par date de modification

| Limite | Conséquence | Remède |
|---|---|---|
| **Suppressions invisibles** | Une ligne supprimée à la source n'a plus de date de modification : on ne la voit pas | Suppression logique (colonne `est_supprime`), ou **CDC** (section 5) |
| **Horloges désynchronisées** | La date de modification d'une transaction n'est pas celle de sa validation | Chevauchement, ou numéro de séquence |
| **Colonne non fiable** | Certaines modifications ne mettent pas à jour la date | Valider avec la source, utiliser un numéro de séquence |
| **Ex æquo** à la frontière | Deux lignes ont la même date | `>=` + idempotence |

---

## 5. Le CDC : appliquer un flux de changements

### 5.1 Pourquoi un journal de changements

Le CDC capture **chaque changement** à la source (y compris les **suppressions**) dans un journal ordonné. On ne rate plus rien, et on peut rejouer l'historique. Il est consommé en **deux phases** : un instantané initial, puis les changements.

### 5.2 L'algorithme d'application

Pour appliquer un lot d'événements, **dans cet ordre** :

1. **Dédupliquer par clé** : garder, pour chaque `vente_id`, **le dernier événement** (par `seq`) ;
2. **Appliquer** : si le dernier événement est `D`, **supprimer** la clé ; sinon, **insérer ou remplacer** la ligne.

Ce que ça donne en SQL (reconstruire l'état à partir de l'ensemble des événements) :

```sql
SELECT vente_id, client_id, montant, statut, date_vente
FROM (
  SELECT
    vente_id, client_id, montant, statut, date_vente, op,
    ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY seq DESC) AS rn     -- seq, JAMAIS arrivee
  FROM ventes_cdc
) t
WHERE rn = 1
  AND op <> 'D';                                  -- les suppressions sont APPLIQUÉES (pas ignorées)
```

Résultat : **12 lignes, CA validé 3 235** (10 ventes validées), avec la vente 105 à 180, la vente 110 en `ANNULEE` et la 108 absente.

### 5.3 Les deux bugs de l'acte 2

Voici ce que donnent les variantes fausses, sur les mêmes événements :

| Variante | Lignes | Ventes validées | CA validé |
|---|---|---|---|
| **Correcte** : ordre `seq`, suppressions appliquées | 12 | 10 | **3 235** |
| Ordre d'**arrivée** (faux), suppressions appliquées | 12 | 11 | 3 325 |
| Ordre `seq`, suppressions **ignorées** | 13 | 11 | 3 385 |
| Ordre d'**arrivée** + suppressions **ignorées** | 13 | 12 | **3 475** |

La dernière ligne est le chiffre de la Finance (3 475). Chaque bug a son propre écart :

**Bug A : l'ordre d'arrivée au lieu de l'ordre de séquence (+ 90).**
La vente 110 a deux événements : `seq 11` (validée, arrivée au rang 17) et `seq 17` (annulée, arrivée au rang 11). En triant par `arrivee`, le dernier événement vu est la **validation**, et la vente 110 reste `VALIDEE` (+ 90 de CA). La vérité est son **dernier changement réel** : `ANNULEE`. **L'ordre d'arrivée n'est pas l'ordre des changements.**

**Bug B : les suppressions ignorées (+ 150).**
Le job filtre les événements `D` **avant** de dédupliquer (`WHERE op <> 'D'`) : la suppression de la vente 108 disparaît, et la dernière version connue de la 108 (insertion, 150 validée) reste dans l'entrepôt. L'ordre correct est l'inverse : **d'abord** déterminer le **dernier événement** de chaque clé, **ensuite** retirer les clés dont le dernier événement est un `D`.

> **Le piège du `D` ignoré par un autre moyen** : si on prend le dernier événement **sans** filtrer `op`, la clé 108 est conservée comme une **ligne fantôme** : un `D` contient des colonnes `NULL`, donc on se retrouve avec une vente sans client, sans montant, sans statut. Le `op <> 'D'` doit être appliqué **après** la sélection du dernier événement, jamais avant.

### 5.4 Les règles d'or du CDC

1. **L'ordre vient de la source** (`seq`, numéro de journal, LSN), **pas** de l'heure d'arrivée ni d'une horloge locale.
2. **Le critère d'ordre doit être unique par clé** : si deux événements d'une même clé ont le même `ts`, le résultat est arbitraire (Blog 3). `seq` règle ce problème.
3. **Un événement livré deux fois** (livraison au moins une fois) est **absorbé** par la déduplication : rejouer l'événement 12 deux fois donne toujours **12 lignes et 3 235** (vérifié).
4. **Les suppressions sont des événements** à appliquer, pas à filtrer.
5. **Garder le journal brut** (couche bronze, Blog 7) : on peut reconstruire l'état à tout moment, ou corriger un bug de logique en **rejouant l'historique**.

### 5.5 Suppression physique ou logique ?

| | Physique (`DELETE`) | Logique (colonne `est_supprime` ou date de suppression) |
|---|---|---|
| La ligne | Disparaît | **Reste**, marquée |
| Historique | Perdu dans la table (mais il reste dans le journal) | Conservé |
| Rapports | Simples | Il faut **filtrer** `est_supprime = false` partout |
| Conformité (suppression de données personnelles) | Directe | À compléter par une purge |

Un bon compromis : la **couche argent** garde une suppression logique, la **couche or** (reporting) n'expose que les lignes actives.

---

## 6. `MERGE` et les tables transactionnelles

### 6.1 Ce que fait `MERGE`

`MERGE` compare une table **source** (le lot de changements) à la table **cible** par une **clé**, et applique des actions différentes selon le résultat : **mise à jour** si la clé existe, **insertion** si elle n'existe pas, **suppression** si l'événement est un `D`. Tout en **une seule instruction**, idempotente par clé.

### 6.2 La syntaxe

**Hive (table transactionnelle ACID, ORC)**

```sql
MERGE INTO ventes_cible AS t
USING lot AS s                                    -- lot : un seul événement par clé (voir 6.3)
ON t.vente_id = s.vente_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET
     client_id  = s.client_id,
     montant    = s.montant,
     statut     = s.statut,
     date_vente = s.date_vente
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT VALUES
     (s.vente_id, s.client_id, s.montant, s.statut, s.date_vente);
```

**Spark SQL** (uniquement sur un format de table qui le supporte : Delta Lake, Apache Iceberg, Apache Hudi ; pas sur du Parquet « nu »)

```sql
MERGE INTO ventes_cible AS t
USING lot AS s
ON t.vente_id = s.vente_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *;
```

**Impala** : selon la version, `UPSERT` pour les tables **Kudu**, et des opérations de mise à jour et de suppression sur des tables **Iceberg** ; vérifie la prise en charge de `MERGE` dans la documentation de ta version.

| Format de table | Mises à jour au niveau ligne ? | Remarque |
|---|---|---|
| Parquet / ORC « classiques » (externes, gérées non transactionnelles) | **Non** | On réécrit des partitions (6.5) |
| Hive **ACID** (ORC transactionnel) | Oui | Fichiers *delta* + compactage |
| **Iceberg**, **Delta**, **Hudi** | Oui | Selon le moteur et la version |
| **Kudu** | Oui (`UPSERT`) | Stockage orienté lignes, pour les données qui changent |

### 6.3 Les règles à respecter

1. **La source doit avoir une seule ligne par clé.** Si deux lignes de la source correspondent à la même ligne de la cible, le résultat est **ambigu** : les moteurs lèvent une **erreur** (Hive : contrôle de cardinalité activé par défaut ; Spark : erreur de correspondances multiples). Dans notre journal, la vente 105 a **plusieurs événements** (insertion, puis modification) : il faut **dédupliquer** avant (section 5.2). Même cause qu'au Blog 1 (clé non unique), ici côté source du `MERGE`.
2. **Ordre des clauses `WHEN`** : la première condition vraie s'applique. Le `DELETE` conditionnel doit précéder le `UPDATE` général.
3. **Limites de Hive** : un nombre limité de clauses (au plus trois en général : mise à jour, suppression, insertion), et on ne peut pas modifier les colonnes de partition ou de bucketing.
4. **Exprime le filtre de partition dans le `ON`** : sans lui, `MERGE` lit **toute la cible** pour trouver les correspondances (Blog 5 : partition pruning). Par exemple `ON t.vente_id = s.vente_id AND t.mois IN (...)`.
5. **Les mises à jour fréquentes créent beaucoup de petits fichiers delta** (Blog 5) : prévoir le **compactage** (`ALTER TABLE ... COMPACT 'major'` sur Hive ACID, ou compactage automatique).
6. **Une clé qui change de partition** (par exemple une vente dont la date change) doit être supprimée de l'**ancienne** partition et insérée dans la **nouvelle** : la clé de partition ne doit pas être modifiable, ou il faut traiter le cas explicitement.

### 6.4 Le SCD2 avec `MERGE`

La dimension du Blog 7 peut se maintenir avec **un seul `MERGE`**, grâce à une astuce : la source contient **deux lignes** par client modifié, l'une avec la **clé de fusion** (pour **fermer** l'ancienne version) et l'autre avec une **clé de fusion `NULL`** (qui ne correspond jamais, donc **insère** la nouvelle version).

```sql
MERGE INTO dim_client_scd2 AS t
USING (
  -- (a) pour chaque client de l'instantané : sert à fermer la version courante si elle a changé,
  --     ou à insérer le client s'il est nouveau
  SELECT s.client_id AS merge_key, s.client_id, s.nom, s.pays, s.segment
  FROM snap s
  UNION ALL
  -- (b) pour chaque client MODIFIÉ : une ligne à clé NULL, jamais « matched » → INSERT de la nouvelle version
  SELECT CAST(NULL AS INT) AS merge_key, s.client_id, s.nom, s.pays, s.segment
  FROM snap s
  JOIN dim_client_scd2 d ON s.client_id = d.client_id AND d.est_courant = 1
  WHERE NOT (s.nom <=> d.nom AND s.pays <=> d.pays AND s.segment <=> d.segment)   -- null-safe (Blog 7)
) u
ON t.client_id = u.merge_key AND t.est_courant = 1
WHEN MATCHED AND NOT (t.nom <=> u.nom AND t.pays <=> u.pays AND t.segment <=> u.segment)
  THEN UPDATE SET date_fin = DATE '2024-03-15', est_courant = 0                   -- fermer l'ancienne version
WHEN NOT MATCHED
  THEN INSERT VALUES (md5(concat_ws('|', CAST(u.client_id AS STRING), '2024-03-15')),
                      u.client_id, u.nom, u.pays, u.segment,
                      DATE '2024-03-15', DATE '9999-12-31', 1);                   -- ouvrir la nouvelle version
```

**Comment ça se lit :**

| Situation du client | Ligne (a) | Ligne (b) | Résultat |
|---|---|---|---|
| **Inchangé** | matched, mais les attributs sont identiques → aucune action | — | Rien ne bouge |
| **Modifié** (Alice, David) | matched et différent → `UPDATE` : **fermeture** | `merge_key` NULL → not matched → `INSERT` | Ancienne version fermée, nouvelle ouverte |
| **Nouveau** (Farid) | not matched → `INSERT` | — | Première version insérée |

**Vérifié** (sur une émulation ligne à ligne des règles du `MERGE`) : appliqué à la dimension d'avant le 15 mars et à l'instantané du 15 mars, il produit **exactement** la dimension du Blog 7 : Alice et David avec deux versions, les autres avec une seule, plus Farid inséré. **Rejoué avec le même instantané, il ne change rien** (8 lignes avant et après) : c'est idempotent. L'ON porte sur `est_courant = 1` : seule la version courante peut être fermée.

> Le même avertissement qu'au Blog 7 s'applique : une date de début **tirée de la source** (date d'effet), pas de la date de chargement, pour les nouveaux clients.

### 6.5 Sans `MERGE` : réécrire les partitions touchées

Sur des tables Parquet ou ORC **non transactionnelles**, il n'y a pas de mise à jour ligne à ligne. L'alternative idempotente : **réécrire les partitions concernées** en combinant **ce qui est conservé** et **ce qui change**.

```sql
-- 1) dernier événement par clé dans le lot (seq, pas arrivee)
WITH lot_dernier AS (
  SELECT vente_id, client_id, montant, statut, date_vente, op
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY seq DESC) AS rn
        FROM lot_evenements) t
  WHERE rn = 1
),
-- 2) partitions touchées : celles des nouvelles lignes ET celles où les clés du lot se trouvent déjà
partitions_touchees AS (
  SELECT date_format(date_vente, 'yyyy-MM') AS mois FROM lot_dernier WHERE op <> 'D'
  UNION
  SELECT c.mois FROM ventes_cible c JOIN lot_dernier l ON c.vente_id = l.vente_id
)
-- 3) on réécrit ces partitions : lignes conservées + nouvelles lignes
INSERT OVERWRITE TABLE ventes_cible PARTITION (mois)
SELECT vente_id, client_id, montant, statut, date_vente, mois
FROM (
  SELECT c.vente_id, c.client_id, c.montant, c.statut, c.date_vente, c.mois      -- conservé
  FROM ventes_cible c
  WHERE c.mois IN (SELECT mois FROM partitions_touchees)
    AND c.vente_id NOT IN (SELECT vente_id FROM lot_dernier)
  UNION ALL
  SELECT l.vente_id, l.client_id, l.montant, l.statut, l.date_vente,             -- nouveau ou modifié
         date_format(l.date_vente, 'yyyy-MM') AS mois
  FROM lot_dernier l
  WHERE l.op <> 'D'                                                              -- les suppressions disparaissent
) x;
```

**Vérifié** : en partant de l'état initial (événements 1 à 10 : **10 lignes, CA validé 2 950**) et du lot des événements 11 à 17, la réécriture des deux partitions touchées (**2024-03** et **2024-04**) donne **12 lignes et 3 235** (mars 9 lignes / 2 980, avril 3 lignes / 255), **identique** à l'état reconstruit depuis tout le journal (aucune ligne en écart dans les deux sens avec `EXCEPT`).

Les points délicats :

- **Les partitions touchées incluent l'ancienne partition** d'une ligne qui change de date, sinon elle serait **dupliquée** (anciennes et nouvelles partitions).
- **`NOT IN` sur `vente_id`** est sûr ici car la clé n'est jamais NULL ; si elle pouvait l'être, préfère `NOT EXISTS` (Blog 1).
- La partition d'une clé **supprimée** doit être réécrite **même si aucune nouvelle ligne n'y arrive** (c'est le rôle de la 2ᵉ branche de `partitions_touchees`).
- Vérifier le comportement d'un `INSERT OVERWRITE` à partitions dynamiques sur ta plateforme (section 3.4).

---

## 7. Backfill et retraitement

Un **backfill** consiste à **recharger une période passée** : une source corrigée, un bug de logique réparé, une nouvelle colonne à calculer.

### 7.1 Les principes

1. **Le job est paramétré par une plage de dates**, jamais « aujourd'hui » codé en dur.
2. **L'écriture est idempotente par partition** (`INSERT OVERWRITE` ou `MERGE`), donc on peut rejouer n'importe quelle plage.
3. **On traite partition par partition**, en lots (un mois, une semaine) pour limiter la charge et pouvoir reprendre après un échec.
4. **On évite les exécutions concurrentes** d'un backfill et du chargement quotidien sur les **mêmes partitions** : verrou ou orchestration qui les sérialise.
5. **On garde la source brute** (journal CDC, fichiers bruts) : sans elle, impossible de rejouer l'histoire.

### 7.2 Le test le plus puissant : le backfill à vide

Avant de lancer un gros retraitement, rejoue une **période stable** (où rien n'a dû changer) et **compare** avec l'existant :

```sql
-- Doit renvoyer 0 ligne dans les deux sens si le job est déterministe et le backfill correct
SELECT * FROM cible_existante  WHERE mois = '2024-03'
EXCEPT
SELECT * FROM cible_recalculee WHERE mois = '2024-03';

SELECT * FROM cible_recalculee WHERE mois = '2024-03'
EXCEPT
SELECT * FROM cible_existante  WHERE mois = '2024-03';
```

Si des lignes diffèrent alors que rien n'a changé à la source, le job n'est **pas déterministe** (`current_timestamp`, `rand`, ordre non unique) : un défaut qu'on veut découvrir **avant** de s'en servir pour réparer des données.

### 7.3 Le temps d'événement et le temps de traitement

| | Temps d'**événement** | Temps de **traitement** |
|---|---|---|
| Question | **Quand** la vente a-t-elle eu lieu ? | **Quand** l'a-t-on chargée ? |
| Colonne | `date_vente` | `date_chargement`, `arrivee` |
| Sert à | Partitionner, rapporter | Suivre l'avancement du chargement, filtrer les nouveautés |

Un backfill est un **retraitement** : on rejoue des événements anciens **maintenant**. La logique métier doit toujours utiliser le temps d'**événement** (résolution d'une version SCD2 à la date de la vente, Blog 7), jamais l'heure à laquelle le job s'exécute.

### 7.4 Backfill et SCD2

Reconstruire une dimension historisée suppose de disposer de **l'historique des versions** (la table `clients_historique` du Blog 7) ou de **rejouer les instantanés successifs** dans l'ordre, chacun avec **sa propre date en paramètre**. On ne peut pas reconstituer le passé avec un seul instantané : ce qui a changé entre-temps est perdu.

---

## 8. L'anatomie d'un job idempotent

### 8.1 Le squelette

```
Paramètres : job, date_traitement (ou plage), run_id

1. LIRE le filigrane          (table de contrôle)
2. EXTRAIRE                   ce qui est > filigrane - chevauchement   → zone intermédiaire (staging) ÉCRASÉE à chaque exécution
3. DÉDUPLIQUER                dernier événement par clé, ordre = seq   (ROW_NUMBER avec départage unique)
4. VALIDER                    unicité de la clé, comptes, valeurs obligatoires, réconciliation (Blog 9)
5. APPLIQUER                  MERGE par clé, ou INSERT OVERWRITE des partitions touchées
6. PUBLIER                    rendre le résultat visible (sans état intermédiaire exposé)
7. AVANCER le filigrane       MAX(ts) du lot, SEULEMENT si tout est réussi
8. JOURNALISER                run_id, volumes, durée, filigrane avant / après
```

### 8.2 Ce qui doit être un paramètre

| Doit être un **paramètre** | Ne doit **jamais** apparaître dans la logique |
|---|---|
| Date ou plage de traitement | `current_date()`, `current_timestamp()` |
| Seuil de chevauchement | `rand()`, `uuid()` dans une clé |
| Identifiant d'exécution (pour les logs) | `LIMIT` sans `ORDER BY` |
| Nom de la cible | Nom de table avec date codée en dur |

### 8.3 Publier sans exposer d'état intermédiaire

Sur du stockage non transactionnel, un lecteur qui interroge la table **pendant** l'écriture peut voir des données à moitié chargées. Les stratégies courantes :

- **Écrire dans une zone intermédiaire**, valider, puis **remplacer** la partition cible en une opération courte (selon la plateforme : `INSERT OVERWRITE`, changement d'emplacement de partition, instantané Iceberg) ;
- Utiliser un **format transactionnel** (ACID, Iceberg, Delta) qui rend l'écriture **atomique** : on voit l'ancienne ou la nouvelle version, jamais un mélange ;
- Écrire un **marqueur de fin** (`_SUCCESS`) que les consommateurs attendent.

Ce que la plateforme garantit **réellement** dépend de ta version : teste le comportement d'un lecteur pendant une écriture.

---

## 9. Les pièges des chargements

| # | Piège | Conséquence | Remède |
|---|---|---|---|
| 1 | **`INSERT INTO` rejoué** | Doublons (acte 1 : 26 lignes, CA 6 410 au lieu de 3 205) | `INSERT OVERWRITE` de la partition, ou `MERGE` par clé |
| 2 | **`INSERT INTO ... NOT EXISTS`** pour des données qui changent | Les modifications sont ignorées (110 reste à `NULL`) | `MERGE`, ou réécriture de partition |
| 3 | **Trier par ordre d'arrivée** | Un événement ancien arrivé tard écrase un événement récent (acte 2, bug A) | Ordre par `seq` (numéro de la source) |
| 4 | **Filtrer les suppressions avant la déduplication** | Les clés supprimées restent (acte 2, bug B) | Dernier événement **d'abord**, retirer les `D` **ensuite** |
| 5 | **Filigrane avec `>`, sans chevauchement** | Pertes silencieuses à la frontière | `>=` ou fenêtre de chevauchement + écriture idempotente |
| 6 | **Filigrane = heure courante** | Événements tardifs sautés | `MAX(ts)` des lignes réellement lues, mis à jour après succès |
| 7 | **Source du `MERGE` avec plusieurs lignes par clé** | Erreur de cardinalité (ou résultat ambigu) | Dédupliquer avant, une ligne par clé |
| 8 | **`INSERT OVERWRITE` dynamique mal compris** | Écrasement de **toute** la table, ou partition qui n'est pas vidée | Tester le comportement, configurer explicitement le mode |
| 9 | **Logique dépendant de `current_timestamp`** | Rejeu différent, backfill impossible | Date de traitement en **paramètre** |
| 10 | **Clé qui change de partition** | Doublon dans l'ancienne et la nouvelle partition | Inclure l'ancienne partition dans la réécriture |
| 11 | **Mises à jour fréquentes sur ACID sans compactage** | Explosion de petits fichiers, requêtes lentes (Blog 5) | Compactage planifié |
| 12 | **Aucun contrôle après chargement** | Un doublon n'est vu que par la Finance | Contrôles d'unicité et de réconciliation dans le job (Blog 9) |

---

## 10. Résolution de l'incident

### 10.1 Acte 1 : le lot rejoué

**Cause** : `INSERT INTO` non idempotent. Après la relance de 7h, les **13** clés sont **dupliquées** (26 lignes, CA validé **6 410**, au lieu de 13 lignes et **3 205**).

**Correction** : remplacer l'ajout par une écriture **idempotente** : `INSERT OVERWRITE` de la partition (sur du Parquet non transactionnel) ou `MERGE` par clé (sur du transactionnel). Après la correction, **rejouer le même lot** doit toujours donner 13 lignes et 3 205. Pour réparer les données déjà doublées, **recalculer les partitions concernées** (c'est possible parce qu'on a gardé le journal brut).

### 10.2 Acte 2 : le flux de changements

**Causes** : *(A)* tri par ordre d'arrivée, *(B)* suppressions filtrées avant la déduplication. Résultat : **3 475** au lieu de **3 235**, soit **+ 90** (110 reste validée) **+ 150** (108 n'est jamais supprimée).

**Correction** : la requête de la section 5.2 (dernier événement par clé, **ordre `seq`**, puis retrait des `D`), appliquée par `MERGE` (6.2) ou par réécriture de partitions (6.5).

### 10.3 Le job corrigé, en une page

```sql
-- 0) paramètres : filigrane lu dans etl_controle, chevauchement de 1 heure
--    (la syntaxe d'intervalle varie : INTERVAL 1 HOUR en Spark, INTERVAL '1' HOUR en Hive, selon la version)
-- 1) lot = événements récents (avec chevauchement), idempotent grâce à l'étape 2
WITH lot AS (
  SELECT *
  FROM ventes_cdc
  WHERE ts >= (SELECT watermark - INTERVAL 1 HOUR FROM etl_controle WHERE job = 'charger_ventes')
),
-- 2) un seul événement par clé : le plus récent selon seq
dernier AS (
  SELECT vente_id, client_id, montant, statut, date_vente, op, ts
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY seq DESC) AS rn
    FROM lot
  ) t
  WHERE rn = 1
)
-- 3) application : MERGE (section 6.2) ou réécriture des partitions touchées (section 6.5)
SELECT * FROM dernier;

-- 4) contrôles (avant de publier)
-- 5) UPDATE etl_controle : watermark = (SELECT MAX(ts) FROM lot), seulement si tout est réussi
```

### 10.4 Les contrôles qui auraient détecté les deux actes

```sql
-- C1. Unicité de la clé dans la cible : doit renvoyer 0 ligne  (détecte l'acte 1)
SELECT vente_id, COUNT(*) AS n
FROM ventes_cible
GROUP BY vente_id
HAVING COUNT(*) > 1;

-- C2. Réconciliation avec l'état reconstruit depuis le journal : doit renvoyer 0 ligne dans les deux sens
--     (détecte l'acte 2 : 108 en trop, 110 au mauvais statut)
SELECT vente_id, client_id, montant, statut, date_vente FROM ventes_cible
EXCEPT
SELECT vente_id, client_id, montant, statut, date_vente FROM etat_reconstruit;
-- puis l'inverse

-- C3. Totaux par mois : mars = 9 lignes / 2 980, avril = 3 lignes / 255 ; total = 12 lignes / 3 235
SELECT mois, COUNT(*) AS nb, SUM(CASE WHEN statut = 'VALIDEE' THEN montant END) AS ca_valide
FROM ventes_cible
GROUP BY mois;
```

| Contrôle | Chargement correct | Acte 1 (rejeu) | Acte 2 (3 475) |
|---|---|---|---|
| C1 : clés dupliquées | 0 | **13** | 0 |
| C2 : écart avec l'état reconstruit | 0 | (doublons visibles ailleurs) | **108 en trop, 110 au mauvais statut** |
| C3 : CA validé | **3 235** | 6 410 | 3 475 |

### 10.5 Récapitulatif

| Question | Réponse |
|---|---|
| Pourquoi le CA a-t-il doublé ? | Le lot a été inséré deux fois : `INSERT INTO` n'est pas idempotent |
| Pourquoi 110 est-elle encore validée ? | L'ordre d'arrivée a primé sur l'ordre de séquence de la source |
| Pourquoi 108 existe-t-elle encore ? | Les événements `D` ont été filtrés avant la déduplication |
| Quelle règle de base ? | Appliquer le **dernier événement par clé, dans l'ordre de la source**, de façon **idempotente** |
| Comment s'en prémunir ? | Contrôles d'unicité, de réconciliation et de totaux à chaque exécution |

---

## 11. 10 questions d'entretien

**1. Qu'est-ce que l'idempotence, et pourquoi la veut-on dans un chargement ?**
Exécuter un traitement plusieurs fois donne le même résultat qu'une seule exécution. On la veut parce qu'un job est relancé (pannes, retries, backfills, livraison au moins une fois) : sans elle, chaque relance risque des doublons.

**2. Ton job plante à 80 % et il est relancé. Que se passe-t-il ?**
Si l'écriture est un `INSERT INTO`, les données déjà écrites sont dupliquées. Avec un `INSERT OVERWRITE` de partition ou un `MERGE` par clé, la relance remplace ou fusionne : le résultat final est le même. On veille aussi à ce que le filigrane n'avance qu'après un chargement réussi.

**3. Comment fonctionne un chargement incrémental par filigrane, et quels sont ses pièges ?**
On mémorise la dernière date de modification chargée et on ne lit que ce qui est plus récent. Pièges : la frontière (`>` perd les ex æquo), les événements tardifs, les suppressions invisibles. Parades : fenêtre de chevauchement, `>=`, écriture idempotente, `MAX(ts)` du lot, CDC pour les suppressions.

**4. Comment appliquer un flux CDC correctement ?**
Dédupliquer par clé en gardant le dernier événement selon la séquence de la source (pas l'ordre d'arrivée), puis retirer les clés dont le dernier événement est un `D`, puis insérer ou remplacer le reste. Les suppressions s'appliquent après la sélection du dernier événement, jamais avant.

**5. Pourquoi ne pas trier par ordre d'arrivée ?**
Parce que les événements peuvent arriver dans le désordre : un ancien événement arrivé tard écraserait un événement récent. L'ordre qui fait foi est celui de la source (numéro de séquence).

**6. Que se passe-t-il si la source d'un `MERGE` contient deux lignes pour la même clé ?**
Le résultat est ambigu et le moteur lève une erreur de cardinalité (sur Hive et Spark). On déduplique la source avant, avec `ROW_NUMBER`, pour avoir une ligne par clé.

**7. Comment maintenir une dimension SCD2 avec un `MERGE` ?**
En alimentant le `MERGE` avec deux lignes par client modifié : une avec la clé de fusion (qui ferme la version courante) et une avec une clé `NULL` (jamais « matched », donc insérée comme nouvelle version). La détection de changement est null-safe.

**8. `INSERT OVERWRITE` ou `MERGE` ?**
`INSERT OVERWRITE` remplace une partition entière : simple et idempotent, mais il faut tout le contenu de la partition ; il convient aux tables non transactionnelles. `MERGE` applique des changements par clé : adapté à de gros volumes avec peu de changements, mais exige un format transactionnel.

**9. Qu'est-ce qu'un backfill, et comment le tester ?**
Un rechargement d'une période passée. On le teste en rejouant une période **stable** et en comparant avec l'existant (`EXCEPT` dans les deux sens) : toute différence révèle un job non déterministe.

**10. Pourquoi garder la source brute (journal CDC) ?**
Pour pouvoir **rejouer** : corriger un bug de logique, reconstruire l'état, faire un backfill, auditer. Sans le brut, un chargement erroné est irréparable.

---

## 12. 5 exercices

### Exercice 1 : idempotent ou non ?
Pour chaque instruction, dis si un rejeu est sans conséquence : (a) `INSERT INTO cible SELECT * FROM staging` ; (b) `INSERT OVERWRITE TABLE cible PARTITION (mois='2024-04') SELECT ... FROM source WHERE mois = '2024-04'` ; (c) `INSERT INTO cible SELECT s.* FROM staging s WHERE NOT EXISTS (SELECT 1 FROM cible t WHERE t.vente_id = s.vente_id)` ; (d) un `MERGE` par clé avec une source dédupliquée ; (e) un `INSERT OVERWRITE` qui ajoute `current_timestamp() AS charge_le`.

<details>
<summary>Solution</summary>

(a) **Non** : doublons. (b) **Oui** : la partition est remplacée. (c) **Oui pour les insertions, mais ignore les modifications** (idempotent seulement pour des données immuables). (d) **Oui**. (e) Les **données métier** sont identiques à chaque rejeu, mais la colonne technique `charge_le` change : acceptable si elle n'entre pas dans une clé ni dans une comparaison, mais elle **rend impossible** le test « backfill à vide » (section 7.2) tant qu'on ne l'exclut pas de la comparaison. Mieux vaut un horodatage **passé en paramètre**.
</details>

### Exercice 2 : la frontière du filigrane
Le filigrane est `2024-04-01 09:30:00`. Combien d'événements du journal sont lus avec `ts > filigrane`, avec `ts >= filigrane`, et avec un chevauchement d'une heure ? Lequel recommandes-tu ?

<details>
<summary>Solution</summary>

**5**, **6** et **7** événements (voir la section 4.3). L'événement 12 (09:30:00 pile) n'est lu qu'avec `>=` ou le chevauchement. On recommande le **chevauchement** (par exemple 1 heure, à ajuster aux délais de la source) **avec une écriture idempotente** : les événements déjà chargés (ici le 11) sont relus sans conséquence.
</details>

### Exercice 3 : l'état de la vente 110
Que vaut le statut de la vente 110 si on trie par `seq` ? Et par `arrivee` ? Quelle est la bonne réponse ?

<details>
<summary>Solution</summary>

Par `seq` : le dernier événement est le 17 (annulation du 10 avril) → **`ANNULEE`**. Par `arrivee` : le dernier événement arrivé est le 11 (validation du 1er avril, rang 17) → `VALIDEE`. La bonne réponse est **`ANNULEE`** : l'ordre qui compte est celui des changements à la source.
</details>

### Exercice 4 : l'erreur de cardinalité
Le journal contient 17 événements pour 13 clés. Quel est le problème si on l'utilise tel quel comme source d'un `MERGE` ? Que contient la source corrigée ?

<details>
<summary>Solution</summary>

Plusieurs clés ont **plusieurs événements** (105, 110...) : une ligne de la cible correspondrait à plusieurs lignes de la source, d'où une **erreur de cardinalité**. La source corrigée (dernier événement par clé, par `seq`) contient **13 lignes**, une par clé, dont **une suppression** (108, `op = 'D'`) qui déclenche le `DELETE`. Après le `MERGE`, la cible a **12 lignes**.
</details>

### Exercice 5 : réécrire sans `MERGE`
Tu pars de l'état initial (événements 1 à 10 : 10 lignes, CA validé 2 950) et du lot des événements 11 à 17. Quelles partitions faut-il réécrire, et quels résultats attends-tu ?

<details>
<summary>Solution</summary>

Partitions **2024-03** (la 105 et la 110 changent, la 108 est supprimée) et **2024-04** (les ventes 201, 202, 203 arrivent). Après réécriture : **12 lignes, 3 235 de CA validé** ; **mars : 9 lignes / 2 980**, **avril : 3 lignes / 255**. Attention à la partition de la vente 108 : elle doit être réécrite **même si aucune nouvelle ligne n'y arrive** (suppression pure), ce qui est le rôle de la seconde branche de `partitions_touchees`.
</details>

---

## 13. Checklist avant de livrer un job de chargement

- [ ] **Rejouer le job** (même entrée, même date) donne **exactement le même résultat** : testé, pas supposé
- [ ] Aucune écriture par `INSERT INTO` simple sur une table où un rejeu est possible
- [ ] La **date de traitement** est un paramètre : pas de `current_date()` / `current_timestamp()` dans la logique
- [ ] Pas de `rand()` / `uuid()` dans une clé ; tout `ROW_NUMBER` a un critère de départage unique
- [ ] Le **filigrane** n'avance qu'après un chargement réussi, avec `MAX(ts)` du lot ; une **fenêtre de chevauchement** protège la frontière
- [ ] L'ordre des événements est celui de la **source** (`seq`), pas celui d'**arrivée**
- [ ] Les **suppressions** sont appliquées **après** la sélection du dernier événement par clé
- [ ] La source d'un `MERGE` a **une seule ligne par clé** ; le filtre de partition figure dans le `ON`
- [ ] Le comportement de `INSERT OVERWRITE` (étendue, partition vide) a été **testé** sur la plateforme cible
- [ ] Les **partitions touchées** incluent l'ancienne partition d'une clé qui change de date
- [ ] Les **contrôles** (unicité de clé, réconciliation, totaux par mois) tournent **à chaque exécution** et font échouer le job
- [ ] Le **journal brut** est conservé, et un **backfill à vide** a été testé sur une période stable
- [ ] Le compactage est planifié si les mises à jour créent de petits fichiers

---

## 14. Prochain blog et blogs restants

**Blog 9 : data quality et tests.** Comment **savoir qu'un chiffre est faux avant la Finance** : contraintes et règles de qualité, tests de réconciliation, **quarantaine** des lignes invalides, détection d'anomalies de volume et de fraîcheur, observabilité, et **contrats de données**. Les contrôles C1 à C3 de ce blog, ceux du Blog 7 (intervalles SCD2) et la part de « membre inconnu » en sont les premières briques.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions |
| ✅ 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE`, pivot |
| ✅ 5 | Performance I : lire un plan | `EXPLAIN`, formats, partitionnement, bucketing, statistiques, petits fichiers |
| ✅ 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| ✅ 7 | Modélisation | Star schema, SCD 1 / 2 / 3, clés de substitution, calendrier, Data Vault |
| ✅ 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 9

Sans regarder la suite : (1) comment sais-tu qu'un chiffre est faux **avant** que la Finance ne t'appelle ? Quels **cinq tests automatisés** mettrais-tu sur `ventes_cible` ? (2) Face à une ligne invalide (montant négatif, client inexistant), faut-il la **rejeter**, la **corriger** ou la mettre en **quarantaine** ? (3) Que mesure-tu pour détecter qu'un chargement « réussi » est en réalité **vide ou incomplet** ? Si tu hésites, le Blog 9 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de rejouer toi-même un chargement deux fois, de provoquer chaque bug (ordre d'arrivée, suppressions ignorées, filigrane strict) et de vérifier que tes contrôles les détectent.*