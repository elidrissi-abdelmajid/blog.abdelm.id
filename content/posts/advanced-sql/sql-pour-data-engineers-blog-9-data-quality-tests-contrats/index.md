---
title: "SQL pour Data Engineers, Blog 9 : data quality, tests et contrats de données"
description: "Automatiser les tests de qualité SQL, mettre les données en quarantaine et établir des contrats de données fiables."
weight: 9
---

# SQL pour Data Engineers, Blog 9 : data quality, tests et contrats de données

> **Série** : SQL, de basique à tech lead · **Niveau** : 5/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Les écarts de dialecte sont signalés.
> **Prérequis** : [Blogs SQL 1 à 8]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-8-chargements-idempotence-cdc-merge" >}}), en particulier les blogs 3 (déduplication), 7 (modélisation) et 8 (chargements idempotents). On réutilise `clients_dedup` et les contrôles C1 à C3 du Blog 8.

> **Ce qui a été vérifié, et ce qui ne l'a pas été.** Toute la **logique de contrôle** (règles de validation, classification des lignes, quarantaine, comptes de réconciliation, moyennes et écarts-types de volumes) a été **exécutée sur SQLite et recoupée en Python** avec les petites tables de ce blog : **tous les chiffres en proviennent**. La **syntaxe** propre à Hive / Spark / Impala (`STDDEV_SAMP` en window, `INSERT ... SELECT` multi-tables) n'a **pas** été testée sur un cluster, ni les outils de qualité cités en fin de blog : à reproduire chez toi.

---

## 0. Pourquoi ce blog existe

Au Blog 8, on a rendu les chargements **rejouables**. Mais un chargement parfaitement idempotent peut charger **des données fausses** de façon parfaitement idempotente. Ce blog traite la question la plus importante en production :

> **Comment savoir qu'un chiffre est faux avant que la Finance ne t'appelle ?**

En entretien : *« Comment tu t'assures de la qualité des données dans ton pipeline ? »* Une mauvaise réponse : « je vérifie à la main ». Une bonne réponse : des **tests automatisés** à chaque exécution, une **politique** claire pour les lignes invalides (rejeter, corriger, mettre en quarantaine), des **seuils** qui arrêtent le pipeline, et des **contrats** qui disent ce que chaque équipe garantit.

Le scénario :

> **Vendredi, 9h10.** Le dashboard affiche, pour le 9 avril, un CA validé de **120 273**. Les jours précédents, on tournait autour de quelques centaines. La direction se félicite… puis la Finance appelle : *« C'est faux. »*
> Le chargement est **réussi** (aucune erreur, aucun plantage), il est **idempotent**, il se **rejoue** sans doublon. Et pourtant **le chiffre est faux**. Qu'a-t-on manqué, et comment l'empêcher la prochaine fois ?

Réponse en avance : le lot du jour contenait **des montants négatifs, un montant 100 fois trop grand, une date en 2025, un doublon, un statut en minuscules**… et personne ne l'a vérifié.

### Plan

1. [Les données de ce blog](#1-les-données-de-ce-blog)
2. [Les dimensions de la qualité](#2-les-dimensions-de-la-qualité)
3. [Un test = une requête qui renvoie les lignes en échec](#3-un-test--une-requête-qui-renvoie-les-lignes-en-échec)
4. [Rejeter, corriger, mettre en quarantaine ou avertir](#4-rejeter-corriger-mettre-en-quarantaine-ou-avertir)
5. [La réconciliation](#5-la-réconciliation)
6. [Fraîcheur, volume, complétude, dérive](#6-fraîcheur-volume-complétude-dérive)
7. [Enregistrer les résultats des tests](#7-enregistrer-les-résultats-des-tests)
8. [Tester le SQL lui-même](#8-tester-le-sql-lui-même)
9. [Les contrats de données](#9-les-contrats-de-données)
10. [Observabilité, outils et gestion des alertes](#10-observabilité-outils-et-gestion-des-alertes)
11. [Les pièges de la qualité de données](#11-les-pièges-de-la-qualité-de-données)
12. [Résolution de l'incident](#12-résolution-de-lincident)
13. [10 questions d'entretien](#13-10-questions-dentretien)
14. [5 exercices](#14-5-exercices)
15. [Checklist avant de livrer un pipeline](#15-checklist-avant-de-livrer-un-pipeline)
16. [Prochain blog et blogs restants](#16-prochain-blog-et-blogs-restants)

---

## 1. Les données de ce blog

### 1.1 Le lot entrant du 10 avril (`lot_entrant`)

Un nouveau lot arrive de la source. Il contient **11 lignes** avec **des défauts plantés volontairement**, chacun représentatif d'un type de problème réel :

```sql
USE blog_sql;

CREATE TABLE lot_entrant (
  arrivee    INT,            -- ordre d'arrivée (utile pour départager les doublons)
  vente_id   BIGINT,
  client_id  INT,
  montant    DECIMAL(12,2),
  statut     STRING,
  date_vente DATE
) STORED AS PARQUET;

INSERT INTO lot_entrant VALUES
  ( 1, 301, 1,    60.00,     'VALIDEE',  DATE '2024-04-09'),   -- valide
  ( 2, 302, 2,    45.00,     'validee',  DATE '2024-04-09'),   -- statut en MINUSCULES
  ( 3, 303, 3,   -50.00,     'VALIDEE',  DATE '2024-04-09'),   -- montant NÉGATIF
  ( 4, 304, 77,  120.00,     'VALIDEE',  DATE '2024-04-09'),   -- client INEXISTANT (77)
  ( 5, 305, 4,   120000.00,  'VALIDEE',  DATE '2024-04-09'),   -- montant 100 FOIS TROP GRAND (centimes ?)
  ( 6, 306, 5,   30.00,      'VALIDEE',  DATE '2025-01-15'),   -- date dans le FUTUR
  ( 7, 307, NULL, 20.00,     'VALIDEE',  DATE '2024-04-09'),   -- client NULL
  ( 8, 308, 1,   75.00,      'EXPEDIEE', DATE '2024-04-09'),   -- statut HORS DOMAINE
  ( 9, 301, 1,   60.00,      'VALIDEE',  DATE '2024-04-09'),   -- DOUBLON de 301
  (10, 310, 2,   NULL,       'VALIDEE',  DATE '2024-04-09'),   -- montant NULL
  (11, 311, 3,   33.00,      'VALIDEE',  DATE '2024-04-10');   -- valide
```

La date de traitement est le **2024-04-10** (un **paramètre**, jamais `current_date()`, Blog 8).

### 1.2 Pourquoi le dashboard affiche 120 273

Sans aucun contrôle, le job charge toutes les lignes dont le statut vaut exactement `'VALIDEE'`. Le CA du lot est alors :

```
  60 (301)  - 50 (303)  + 120 (304)  + 120 000 (305)  + 30 (306)  + 20 (307)  + 60 (301 en double)  + 33 (311)
= 120 273
```

Le montant **100 fois trop grand** (305) fait tout basculer, mais **chaque défaut contribue** : un négatif, un orphelin, un doublon (+ 60), une vente de 2025. Les ventes 302 (statut en minuscules : 45), 308 (statut hors domaine) et 310 (montant `NULL`) sont silencieusement **absentes** : un chiffre peut être faux **dans les deux sens**.

### 1.3 Les volumes quotidiens (`volumes_quotidiens`)

Pour la détection d'anomalies de volume, on garde le nombre de lignes chargées chaque jour :

```sql
CREATE TABLE volumes_quotidiens (jour DATE, nb INT) STORED AS PARQUET;

INSERT INTO volumes_quotidiens VALUES
  (DATE '2024-03-28', 100), (DATE '2024-03-29',  98), (DATE '2024-03-30', 103),
  (DATE '2024-03-31',  97), (DATE '2024-04-01', 101), (DATE '2024-04-02',  99),
  (DATE '2024-04-03', 102), (DATE '2024-04-04', 100), (DATE '2024-04-05',  96),
  (DATE '2024-04-06', 104), (DATE '2024-04-07', 101), (DATE '2024-04-08',  99),
  (DATE '2024-04-09', 100),
  (DATE '2024-04-10',  11);                                         -- chargement « réussi » mais presque vide
```

> **Dialecte** : si le littéral `DATE '...'` est refusé, utilise `CAST('...' AS DATE)`.

---

## 2. Les dimensions de la qualité

« Qualité » est un mot vague. On la décompose en **dimensions**, chacune avec ses tests :

| Dimension | Question | Exemple dans notre lot |
|---|---|---|
| **Unicité** | Une entité apparaît-elle une seule fois ? | La vente 301 en double |
| **Complétude** | Les champs obligatoires sont-ils renseignés ? | Montant `NULL` (310) |
| **Validité** | Les valeurs respectent-elles le format, le domaine, la plage ? | Montant négatif (303), statut `EXPEDIEE` (308), montant 120 000 (305) |
| **Cohérence** | Les valeurs sont-elles cohérentes entre colonnes, entre tables ? | Statut `validee` ≠ `VALIDEE` (302) |
| **Intégrité référentielle** | Les clés étrangères pointent-elles vers quelque chose ? | Client 77 inexistant (304), client `NULL` (307) |
| **Exactitude** | La valeur correspond-elle à la réalité ? | 120 000 au lieu de 1 200 |
| **Fraîcheur** | Les données sont-elles à jour ? | Dernier chargement trop ancien |
| **Volume** | Le nombre de lignes est-il plausible ? | 11 lignes au lieu de ~100 |

### Où tester ?

| Couche (Blog 7) | Quoi tester | Pourquoi là |
|---|---|---|
| **Bronze** (brut) | Fraîcheur, volume, schéma, lisibilité (JSON invalide, Blog 4) | Détecter tôt que la **source** a changé |
| **Silver** (nettoyé) | Unicité, complétude, validité, intégrité, quarantaine | C'est là qu'on **corrige** et qu'on rejette |
| **Gold** (reporting) | Réconciliation, totaux, cohérence entre rapports | Garantir que **le chiffre publié** est juste |

Et **quand** : **avant** l'écriture finale (pour bloquer) et **après** (pour réconcilier). Un test qui tourne seulement quand quelqu'un y pense ne protège de rien.

---

## 3. Un test = une requête qui renvoie les lignes en échec

### 3.1 Le principe

Un test de qualité en SQL est une **requête qui renvoie les lignes qui violent la règle**. **Zéro ligne = le test passe.** C'est le même principe que les contrôles C1 à C3 du Blog 8 et les contrôles d'intervalles du Blog 7.

Avantages : tout le monde sait l'écrire (c'est du SQL), il est lisible, on voit **quelles** lignes échouent (pas seulement combien), et il tourne dans n'importe quel moteur.

### 3.2 Le catalogue des tests

On travaille sur le lot **dédupliqué** (le doublon de la 301 est traité à part, sous-section 3.3). Chaque test a un identifiant, une dimension et une requête :

**Unicité** (sur le lot **brut**, avant déduplication)

```sql
-- U1 : une vente_id ne doit apparaître qu'une fois
SELECT vente_id, COUNT(*) AS n
FROM lot_entrant
GROUP BY vente_id
HAVING COUNT(*) > 1;
-- → la vente 301 (2 lignes)
```

**Complétude**

```sql
-- N1 : le montant est obligatoire
SELECT * FROM lot_dedup WHERE montant IS NULL;                 -- → la vente 310
```

**Validité : valeur, plage, domaine, date**

```sql
-- V1 : un montant de vente est strictement positif
SELECT * FROM lot_dedup WHERE montant IS NOT NULL AND montant <= 0;      -- → 303 (-50)

-- V2 : un montant de vente ne dépasse pas la borne métier (ici 10 000)
SELECT * FROM lot_dedup WHERE montant > 10000;                            -- → 305 (120 000)

-- D1 : le statut appartient au domaine, APRÈS normalisation de la casse et des espaces
SELECT * FROM lot_dedup
WHERE statut IS NULL OR UPPER(TRIM(statut)) NOT IN ('VALIDEE', 'ANNULEE');  -- → 308 (EXPEDIEE)

-- T1 : la date de vente n'est pas dans le futur (par rapport à la DATE DE TRAITEMENT, un paramètre)
SELECT * FROM lot_dedup WHERE date_vente > DATE '2024-04-10';             -- → 306 (2025-01-15)
```

**Cohérence : une valeur qui n'est pas dans sa forme normale**

```sql
-- D2 : le statut est déjà normalisé (majuscules, sans espaces)
SELECT * FROM lot_dedup WHERE statut <> UPPER(TRIM(statut));              -- → 302 ('validee')
```

**Intégrité référentielle**

```sql
-- R1 : la clé client est renseignée
SELECT * FROM lot_dedup WHERE client_id IS NULL;                          -- → 307

-- R2 : si la clé client est renseignée, elle existe dans le référentiel
SELECT l.*
FROM lot_dedup l
LEFT JOIN clients_dedup c ON l.client_id = c.client_id
WHERE l.client_id IS NOT NULL AND c.client_id IS NULL;                    -- → 304 (client 77)
```

(Le test R2 s'écrit avec un `LEFT JOIN ... IS NULL` plutôt qu'un `NOT IN`, pour éviter le piège du NULL, Blog 1.)

### 3.3 La déduplication du lot

Avant de valider, on retire les **doublons exacts** de clé, en gardant la première occurrence reçue :

```sql
CREATE TABLE lot_dedup STORED AS PARQUET AS
SELECT arrivee, vente_id, client_id, montant, statut, date_vente
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY arrivee) AS rn      -- départage déterministe
  FROM lot_entrant
) t
WHERE rn = 1;
```

Le lot passe de **11 à 10 lignes** : la deuxième copie de la 301 est écartée. (Si les deux copies **différaient**, il faudrait une règle métier pour choisir : la plus récente, la plus complète, un rejet des deux. Une déduplication silencieuse n'est correcte que pour de vrais doublons.)

### 3.4 Le résultat des tests

| Test | Dimension | Lignes en échec | Ligne(s) concernée(s) |
|---|---|---|---|
| U1 | Unicité (lot brut) | 1 clé dupliquée | 301 |
| N1 | Complétude | 1 sur 10 | 310 |
| V1 | Validité | 1 sur 10 | 303 |
| V2 | Validité | 1 sur 10 | 305 |
| D1 | Validité | 1 sur 10 | 308 |
| T1 | Validité | 1 sur 10 | 306 |
| D2 | Cohérence | 1 sur 10 | 302 |
| R1 | Intégrité | 1 sur 10 | 307 |
| R2 | Intégrité | 1 sur 10 | 304 |

Neuf tests, **un défaut chacun** : le lot est donc truffé de problèmes que **neuf requêtes simples** détectent toutes.

---

## 4. Rejeter, corriger, mettre en quarantaine ou avertir

Un test qui échoue pose une question : **que fait-on de la ligne ?** Il y a quatre réponses, et la bonne dépend du défaut.

### 4.1 La matrice de décision

| Action | Quand | Exemple dans le lot | Pourquoi |
|---|---|---|---|
| **Corriger** automatiquement | La correction est **sûre** et **sans ambiguïté** | `validee` → `VALIDEE` (302) ; doublon exact retiré (301) | Aucune information perdue |
| **Accepter avec avertissement** | La ligne est **utilisable**, mais le défaut doit être suivi | Client `NULL` (307) ; client 77 inconnu (304) → rattachés au membre « inconnu » (Blog 7) | Perdre la vente coûte plus cher que de l'étiqueter |
| **Rejeter vers la quarantaine** | La valeur est **invalide**, et on ne peut pas la deviner | Montant négatif (303), montant NULL (310), statut `EXPEDIEE` (308), date en 2025 (306), montant 120 000 (305) | Charger fausserait les chiffres ; on garde la ligne pour analyse |
| **Bloquer tout le lot** | Le taux de défauts révèle un **problème systémique** | Plus de 20 % de rejets | Charger « le propre » cacherait un bug de la source |

**Règle** : ne **jamais** supprimer une ligne silencieusement. Chaque ligne est **soit chargée, soit en quarantaine, soit comptée comme doublon** : rien ne disparaît.

### 4.2 La classification en SQL

Chaque ligne reçoit une **décision** (la première règle qui échoue gagne : c'est une décision de **priorité**) et une **note** :

```sql
SELECT
  l.vente_id, l.client_id, l.montant,
  UPPER(TRIM(l.statut)) AS statut_normalise,
  l.date_vente,
  CASE
    WHEN l.montant IS NULL                                                  THEN 'REJET:montant_null'
    WHEN l.montant <= 0                                                     THEN 'REJET:montant_non_positif'
    WHEN l.montant > 10000                                                  THEN 'REJET:montant_hors_plage'
    WHEN l.statut IS NULL OR UPPER(TRIM(l.statut)) NOT IN ('VALIDEE', 'ANNULEE')
                                                                            THEN 'REJET:statut_hors_domaine'
    WHEN l.date_vente > DATE '2024-04-10'                                   THEN 'REJET:date_future'
    ELSE 'OK'
  END AS decision,
  CASE
    WHEN l.client_id IS NULL                                  THEN 'AVERT:client_null'
    WHEN c.client_id IS NULL                                  THEN 'AVERT:client_inconnu'
    WHEN l.statut <> UPPER(TRIM(l.statut))                    THEN 'CORRIGE:statut_normalise'
    ELSE ''
  END AS note
FROM lot_dedup l
LEFT JOIN clients_dedup c ON l.client_id = c.client_id
ORDER BY l.vente_id;
```

| vente_id | client_id | montant | statut_normalise | decision | note |
|---|---|---|---|---|---|
| 301 | 1 | 60.00 | VALIDEE | OK | |
| 302 | 2 | 45.00 | VALIDEE | OK | CORRIGE:statut_normalise |
| 303 | 3 | -50.00 | VALIDEE | **REJET:montant_non_positif** | |
| 304 | 77 | 120.00 | VALIDEE | OK | AVERT:client_inconnu |
| 305 | 4 | 120000.00 | VALIDEE | **REJET:montant_hors_plage** | |
| 306 | 5 | 30.00 | VALIDEE | **REJET:date_future** | |
| 307 | NULL | 20.00 | VALIDEE | OK | AVERT:client_null |
| 308 | 1 | 75.00 | EXPEDIEE | **REJET:statut_hors_domaine** | |
| 310 | 2 | NULL | VALIDEE | **REJET:montant_null** | |
| 311 | 3 | 33.00 | VALIDEE | OK | |

Bilan : **5 lignes chargées** (301, 302, 304, 307, 311) pour un **CA de 278** (`60 + 45 + 120 + 20 + 33`), et **5 rejets** vers la quarantaine. Comparé aux **120 273** du dashboard : l'écart est entièrement expliqué, ligne par ligne.

> **Une ligne peut cumuler plusieurs défauts.** Ici, pour la lisibilité, une seule raison prime (l'ordre des `WHEN` est une décision de priorité). En production, on enregistre **toutes** les raisons (une liste ou une ligne par raison), pour qu'une correction à la source ne laisse pas de défaut caché.

### 4.3 La table de quarantaine

On conserve les lignes rejetées avec **tout ce qu'il faut pour les analyser et les rejouer** :

```sql
CREATE TABLE quarantaine_ventes (
  run_id           STRING,
  date_traitement  DATE,
  vente_id         BIGINT,
  client_id        INT,
  montant          DECIMAL(12,2),
  statut           STRING,
  date_vente       DATE,
  raison           STRING,          -- ex. 'montant_non_positif'
  ligne_brute      STRING           -- la ligne d'origine, sans transformation
) STORED AS PARQUET;
```

Trois règles pour qu'elle serve :

1. **Elle a un propriétaire** : quelqu'un la consulte et agit (sinon c'est une poubelle).
2. **Elle se rejoue** : une fois la source corrigée, les lignes repassent par **le même pipeline**, qui est idempotent (Blog 8) : pas de doublon, pas de procédure spéciale.
3. **Elle est mesurée** : son volume dans le temps est un indicateur de santé de la source.

### 4.4 Le disjoncteur : bloquer tout le lot

Au-delà des lignes, on teste le **lot entier**. Taux de rejet de notre lot :

```sql
SELECT
  SUM(CASE WHEN decision <> 'OK' THEN 1 ELSE 0 END)                        AS nb_rejets,
  COUNT(*)                                                                  AS nb_lignes,
  ROUND(100.0 * SUM(CASE WHEN decision <> 'OK' THEN 1 ELSE 0 END) / COUNT(*), 1)  AS taux_rejet_pct
FROM classification;          -- le résultat de la requête 4.2
```

| nb_rejets | nb_lignes | taux_rejet_pct |
|---|---|---|
| 5 | 10 | **50.0** |

Avec un seuil à **20 %**, ce lot déclenche le **disjoncteur** : un lot dont **la moitié** est invalide ne contient pas « quelques lignes sales », il révèle un **problème de source** (changement d'unité, bug d'export, mauvaise version). Deux politiques possibles :

| Politique | Effet sur ce lot | À préférer quand |
|---|---|---|
| **A : charger le propre, mettre le reste en quarantaine** | 5 lignes chargées, CA 278, 5 en quarantaine | Les défauts sont **indépendants** et rares (taux de rejet bas) |
| **B : bloquer tout le lot et alerter** | **Rien** n'est chargé ; les rapports restent sur les données de la veille | Le taux dépasse le seuil : une cause **commune** est probable |

Ici, la politique **B** est la plus sûre : un montant 100 fois trop grand **et** des dates en 2025 **et** des statuts inconnus, dans le même lot, suggèrent un problème d'export. **Mieux vaut un dashboard en retard qu'un dashboard faux.** Le seuil (20 %) est un **choix métier** à documenter et à ajuster avec le temps.

---

## 5. La réconciliation

La réconciliation répond à : **« a-t-on gardé tout ce qu'on devait garder, sans rien inventer ? »** C'est le test le plus puissant, car il ne suppose pas qu'on sait quelles erreurs chercher.

### 5.1 L'invariant de comptage

Chaque ligne du lot est **soit chargée, soit rejetée, soit un doublon retiré** :

```
lignes lues  =  lignes chargées  +  lignes rejetées  +  doublons retirés
     11      =        5          +        5          +        1
```

Si l'égalité est fausse, **des lignes ont disparu** ou **apparu** : un `INNER JOIN` qui a perdu des faits (Blog 1), une déduplication trop agressive, un filtre oublié. C'est le « comptage avant / après » des blogs précédents, érigé en test.

```sql
SELECT
  (SELECT COUNT(*) FROM lot_entrant)                                                AS lues,
  (SELECT COUNT(*) FROM classification WHERE decision = 'OK')                       AS chargees,
  (SELECT COUNT(*) FROM classification WHERE decision <> 'OK')                      AS rejetees,
  (SELECT COUNT(*) FROM lot_entrant) - (SELECT COUNT(*) FROM lot_dedup)             AS doublons;
```

### 5.2 La réconciliation des totaux

Les **sommes** doivent aussi se retrouver. Pour une chaîne de transformations, on compare un total **avant** et **après** (et on explique les écarts connus) :

| Niveau | Contrôle |
|---|---|
| Source → bronze | Comptes et totaux identiques |
| Bronze → silver | `lues = chargées + rejetées + doublons` ; montants : somme des chargées = somme du lot moins celle des rejetées |
| Silver → gold | Le CA du fait = le CA de la source, par mois (Blog 7, section 7.6) |

Sur notre état final du Blog 8 : **12 lignes et 3 235 de CA validé** (mars 9 lignes / 2 980, avril 3 lignes / 255) doivent se retrouver **à l'identique** dans la couche de reporting. Un écart, même de 1 centime, signale un problème (arrondi, type `DOUBLE` à la place de `DECIMAL`, Blog 1).

### 5.3 Réconcilier ligne à ligne : `EXCEPT` dans les deux sens

Pour comparer deux versions d'une table (la cible et l'état recalculé depuis la source, Blog 8) :

```sql
SELECT vente_id, client_id, montant, statut, date_vente FROM ventes_cible
EXCEPT
SELECT vente_id, client_id, montant, statut, date_vente FROM etat_recalcule;      -- doit renvoyer 0 ligne

SELECT vente_id, client_id, montant, statut, date_vente FROM etat_recalcule
EXCEPT
SELECT vente_id, client_id, montant, statut, date_vente FROM ventes_cible;        -- doit renvoyer 0 ligne
```

Les deux sens sont nécessaires : le premier trouve les lignes **en trop**, le second les lignes **manquantes**. (`EXCEPT` compare les NULL comme égaux, Blog 2 : adapté ici.)

### 5.4 Ce que la réconciliation ne détecte pas

Elle prouve que **deux versions concordent**, pas qu'elles sont **justes**. Si la source envoie un montant 100 fois trop grand et que le pipeline le copie fidèlement, la réconciliation passe. D'où la nécessité des tests de **validité** (V2) **en plus**.

---

## 6. Fraîcheur, volume, complétude, dérive

Les tests de la section 3 jugent **le contenu** d'un lot. Ceux-ci jugent **le comportement** du pipeline dans le temps : ce sont eux qui détectent un chargement « réussi » mais **vide, en retard ou anormal**.

### 6.1 La fraîcheur

```sql
SELECT
  MAX(date_vente)                                                  AS derniere_date,
  datediff(DATE '2024-04-10', MAX(date_vente))                     AS retard_jours      -- date de traitement en paramètre
FROM ventes_cible;
```

Sur l'état du Blog 8 : dernière date **2024-04-08**, retard de **2 jours** à la date de traitement du 10. Le seuil dépend de la source (un chargement quotidien tolère 1 jour ; on tient compte des week-ends et des jours fériés). Ce test détecte un job **qui ne tourne plus** sans erreur visible.

### 6.2 Le volume : « réussi mais presque vide »

Le 10 avril, on charge **11 lignes** alors qu'on en reçoit environ 100 par jour. Aucune erreur, aucune règle violée : **le job a réussi**. Seule une comparaison avec l'historique le voit.

On compare le volume du jour à la **moyenne et à l'écart-type des 13 jours précédents** (le jour courant est **exclu** de la référence, sinon l'anomalie contaminerait sa propre base, Blog 3) :

```sql
SELECT
  jour, nb,
  AVG(nb)         OVER (ORDER BY jour ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)   AS moyenne_prec,
  STDDEV_SAMP(nb) OVER (ORDER BY jour ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)   AS ecart_type_prec,
  (nb - AVG(nb) OVER (ORDER BY jour ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING))
    / NULLIF(STDDEV_SAMP(nb) OVER (ORDER BY jour ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 0) AS z
FROM volumes_quotidiens
ORDER BY jour DESC
LIMIT 1;
```

Pour le 10 avril : moyenne des 13 jours précédents **100,0**, écart-type **2,27**, donc un **z-score de -39,2** (le volume est à 39 écarts-types sous la normale, soit **11 % du volume habituel**).

| Volume du jour | Rapport à la moyenne | z-score | Lecture |
|---|---|---|---|
| **11** | 0,11 | **-39,2** | **Anomalie** : chargement presque vide |
| 95 | 0,95 | -2,2 | Variation normale |
| 100 | 1,00 | 0,0 | Normal |
| 200 | 2,00 | **+44,0** | **Anomalie** : volume doublé (doublons ?) |

Les limites à connaître :

1. **Un écart-type minuscule** (2,27 sur une base de 100) rend le z-score **ultra sensible** : une variation de 8 % donnerait déjà |z| > 3. On combine souvent **deux règles** : un seuil sur le **z-score** *et* un seuil sur le **rapport** (par exemple alerte si le volume sort de ±30 %).
2. **La saisonnalité** : un lundi n'est pas un dimanche. Compare plutôt au **même jour de la semaine** des semaines précédentes, ou ajuste les seuils.
3. **Pas assez d'historique** : avec moins de 7 jours de référence, le test n'a pas de sens : exige un minimum.
4. **Un volume anormalement élevé** n'est pas forcément un bon signe (doublons, rechargement non idempotent).

### 6.3 La complétude : le taux de NULL par colonne

```sql
SELECT
  COUNT(*)                                       AS total,
  COUNT(*) - COUNT(client_id)                    AS client_id_null,
  COUNT(*) - COUNT(montant)                      AS montant_null,
  COUNT(*) - COUNT(statut)                       AS statut_null
FROM ventes_dedup;
```

Sur les 10 ventes du Blog 1 : **1 / 1 / 1** NULL (clé client, montant, statut). Un **taux** vaut mieux qu'un compte : on suit son **évolution**. Un taux de NULL qui passe de 1 % à 15 % du jour au lendemain indique presque toujours un **changement à la source** (colonne renommée, filtre modifié, jointure cassée en amont).

### 6.4 L'intégrité référentielle, suivie dans le temps

Sur les 13 ventes de `ventes_dedup`, **2** n'ont pas de client connu (107 : clé `NULL` ; 108 : client 99), soit **15,4 %** (Blog 7, section 7.5). On suit ce taux avec un **seuil d'alerte**. À grande échelle, une clé « inconnu » qui grossit peut aussi devenir une **clé chaude** (Blog 6).

### 6.5 La dérive de schéma et de distribution

| Dérive | Exemple | Détection |
|---|---|---|
| **Schéma** | Une colonne apparaît, disparaît, change de type | Comparer la structure de la table (via le catalogue : `DESCRIBE` ou l'API du métastore) au **contrat** (section 9) |
| **Distribution** | La part de `ANNULEE` passe de 5 % à 40 % | Suivre la répartition de quelques colonnes clés dans le temps |
| **Unités** | Montants soudainement 100 fois plus grands (centimes au lieu d'euros) | Test de plage (V2) + moyenne et maximum suivis dans le temps |

Un changement de **type** ou d'**unité** est la panne la plus coûteuse : le pipeline continue de tourner, les chiffres sont simplement **faux**.

---

## 7. Enregistrer les résultats des tests

Un test dont le résultat n'est pas **conservé** ne permet ni de suivre une tendance, ni d'alerter, ni d'auditer. On enregistre chaque exécution dans une table.

### 7.1 La table des résultats

```sql
CREATE TABLE dq_resultats (
  run_id       STRING,
  test_id      STRING,
  dimension    STRING,
  severite     STRING,        -- BLOQUANT, ERREUR, AVERTISSEMENT, CORRECTION
  nb_echecs    BIGINT,
  nb_total     BIGINT,
  taux_echec   DECIMAL(6,4),
  seuil        DECIMAL(6,4),  -- NULL si « aucun échec toléré »
  statut       STRING         -- OK, ECHEC, BLOQUE
) STORED AS PARQUET;
```

### 7.2 Les quatre niveaux de sévérité

| Sévérité | Conséquence d'un échec | Exemples dans notre lot |
|---|---|---|
| **BLOQUANT** | Le pipeline **s'arrête** ; le filigrane n'avance pas (Blog 8) | Taux de rejet > 20 %, volume anormal, schéma modifié |
| **ERREUR** | La ligne part en **quarantaine** | Montant négatif, statut hors domaine, date future |
| **AVERTISSEMENT** | La ligne est **chargée**, l'échec est **compté et suivi** | Client `NULL`, client inconnu |
| **CORRECTION** | La ligne est **corrigée** automatiquement, l'action est **tracée** | Statut en minuscules, doublon exact |

### 7.3 L'alimentation en SQL

Chaque test produit une ligne de résultat (la structure est la même, on empile avec `UNION ALL`) :

```sql
INSERT INTO dq_resultats
SELECT 'run_2024-04-10', 'N1', 'completude', 'ERREUR',
       SUM(CASE WHEN montant IS NULL THEN 1 ELSE 0 END),
       COUNT(*),
       CAST(SUM(CASE WHEN montant IS NULL THEN 1 ELSE 0 END) AS DECIMAL(10,4)) / COUNT(*),
       NULL,
       CASE WHEN SUM(CASE WHEN montant IS NULL THEN 1 ELSE 0 END) = 0 THEN 'OK' ELSE 'ECHEC' END
FROM lot_dedup

UNION ALL
SELECT 'run_2024-04-10', 'V1', 'validite', 'ERREUR',
       SUM(CASE WHEN montant <= 0 THEN 1 ELSE 0 END),
       COUNT(*),
       CAST(SUM(CASE WHEN montant <= 0 THEN 1 ELSE 0 END) AS DECIMAL(10,4)) / COUNT(*),
       NULL,
       CASE WHEN SUM(CASE WHEN montant <= 0 THEN 1 ELSE 0 END) = 0 THEN 'OK' ELSE 'ECHEC' END
FROM lot_dedup
-- ... un bloc par test (V2, D1, D2, T1, R1, R2), puis le test de lot B1
;
```

### 7.4 Ce que le tableau de bord de qualité affiche pour notre lot

| test_id | dimension | sévérité | nb_echecs | nb_total | taux | statut |
|---|---|---|---|---|---|---|
| U1 | unicité | CORRECTION | 1 | 10 (clés) | 10 % | ECHEC (corrigé) |
| N1 | complétude | ERREUR | 1 | 10 | 10 % | ECHEC |
| V1 | validité | ERREUR | 1 | 10 | 10 % | ECHEC |
| V2 | validité | ERREUR | 1 | 10 | 10 % | ECHEC |
| D1 | validité | ERREUR | 1 | 10 | 10 % | ECHEC |
| T1 | validité | ERREUR | 1 | 10 | 10 % | ECHEC |
| D2 | cohérence | CORRECTION | 1 | 10 | 10 % | ECHEC (corrigé) |
| R1 | intégrité | AVERTISSEMENT | 1 | 10 | 10 % | ECHEC (suivi) |
| R2 | intégrité | AVERTISSEMENT | 1 | 10 | 10 % | ECHEC (suivi) |
| **B1** | **lot : taux de rejet** | **BLOQUANT** | **5** | **10** | **50 %** (seuil 20 %) | **BLOQUÉ** |

Les neuf tests de ligne montrent chacun **10 %** d'échec (une ligne sur dix) : individuellement, rien d'alarmant. C'est le **test de lot B1**, qui **agrège** (5 lignes rejetées sur 10), qui fait ressortir qu'on a un problème **systémique**.

### 7.5 La tendance importe plus que l'instantané

Un taux de rejet de 2 % tous les jours est une **base normale** ; un jour à 2 % puis un jour à 9 % est un **signal**, même si les deux restent sous le seuil. Garder l'historique de `dq_resultats` permet de comparer au passé (même technique que le volume, section 6.2).

---

## 8. Tester le SQL lui-même

On teste les **données** (section 3), mais aussi **la logique de transformation** : la requête de classification de la section 4.2 est du code, et le code a des bugs.

### 8.1 Le test avec fixtures

Une **fixture** est un petit jeu d'entrée dont on connaît **la sortie attendue**. C'est exactement ce qu'on a fait dans toute la série (des tables de 10 à 13 lignes dont on recalcule les résultats à la main).

```sql
-- Entrée : le lot dédupliqué (10 lignes, section 1)
-- Sortie attendue : les décisions de la section 4.2
CREATE TABLE attendu_classification (vente_id BIGINT, decision STRING, note STRING);
INSERT INTO attendu_classification VALUES
  (301, 'OK',                          ''),
  (302, 'OK',                          'CORRIGE:statut_normalise'),
  (303, 'REJET:montant_non_positif',   ''),
  (304, 'OK',                          'AVERT:client_inconnu'),
  (305, 'REJET:montant_hors_plage',    ''),
  (306, 'REJET:date_future',           ''),
  (307, 'OK',                          'AVERT:client_null'),
  (308, 'REJET:statut_hors_domaine',   ''),
  (310, 'REJET:montant_null',          ''),
  (311, 'OK',                          '');

-- Le test : EXCEPT dans les deux sens, doit renvoyer 0 ligne chacun (Blog 8, section 7.2)
SELECT vente_id, decision, note FROM classification
EXCEPT
SELECT vente_id, decision, note FROM attendu_classification;

SELECT vente_id, decision, note FROM attendu_classification
EXCEPT
SELECT vente_id, decision, note FROM classification;
```

Si quelqu'un modifie la requête (réordonne les `WHEN`, change un seuil), le test échoue **avant** la mise en production.

### 8.2 Les cas limites à tester

Les bugs vivent aux **frontières**. Ajoute des lignes à la fixture pour :

| Cas limite | Comportement attendu (d'après les règles) |
|---|---|
| `montant = 0.00` | `REJET:montant_non_positif` (la règle est « strictement positif ») |
| `montant = 10000.00` | `OK` (la règle rejette strictement au-delà de 10 000) |
| `montant = 10000.01` | `REJET:montant_hors_plage` |
| `date_vente` = date de traitement | `OK` (la règle rejette strictement après) |
| `statut = ' VALIDEE '` (avec espaces) | `OK` + correction de normalisation |
| `statut = NULL` | `REJET:statut_hors_domaine` |
| Ligne cumulant **deux** défauts | La règle prioritaire (premier `WHEN`) l'emporte : la priorité est testée et documentée |

### 8.3 Les tests d'invariants

Au lieu de comparer à une sortie fixe, on vérifie une **propriété** qui doit toujours être vraie, sur n'importe quelles données :

| Invariant | Test |
|---|---|
| **Conservation des lignes** | `lues = chargées + rejetées + doublons` (section 5.1) |
| **Unicité après chargement** | Aucune clé dupliquée dans la cible (C1 du Blog 8) |
| **Idempotence** | Exécuter deux fois le même lot donne la même cible (Blog 8) |
| **Pas de ligne inventée** | Toute ligne de la cible existe dans le lot ou dans la cible d'avant |
| **Additivité** | Somme des chargées + somme des rejetées + somme des doublons = somme du lot brut |

### 8.4 Le test de non-régression

Quand un incident survient, **la ligne qui l'a provoqué devient une fixture**. Le montant 120 000 (305), la date de 2025 (306) et le statut en minuscules (302) de ce blog sont des cas qui, une fois ajoutés au jeu de tests, garantissent que **la même panne ne se reproduira pas sans que quelqu'un le voie**.

---

## 9. Les contrats de données

### 9.1 Pourquoi un contrat

Dans notre incident, la source a envoyé un montant 100 fois trop grand, une date de 2025 et un statut inconnu. **Personne n'avait écrit ce qui était « normal ».** Un **contrat de données** est un accord **écrit et vérifiable** entre le **producteur** d'un jeu de données et ses **consommateurs** : ce que les données contiennent, ce qui est garanti, et ce qui se passe quand ça change.

### 9.2 Ce qu'il contient

| Section | Contenu | Exemple |
|---|---|---|
| **Schéma** | Colonnes, types, nullabilité, clé | `montant DECIMAL(12,2) NOT NULL` |
| **Sémantique** | Définitions métier, unités | « `montant` en euros, TTC » ; « CA validé = ventes de statut `VALIDEE` » |
| **Règles de qualité** | Domaines, plages, unicité | `statut ∈ {VALIDEE, ANNULEE}` ; `0 < montant ≤ 10 000` |
| **Valeurs spéciales** | Sentinelles autorisées ou interdites (Blog 6) | « Pas de `client_id = 0` : utiliser `NULL` » |
| **Fraîcheur** | Quand les données sont disponibles | « Avant 07h00, chaque jour » |
| **Volume** | Plage attendue | « 80 à 120 lignes par jour » |
| **Propriétaire** | Qui répond, comment le joindre | Équipe, canal d'astreinte |
| **Évolution** | Politique de changement | « Préavis de 30 jours pour un changement non rétrocompatible » |

### 9.3 Un exemple

```yaml
dataset: ventes_quotidiennes
version: 1.2.0
proprietaire: equipe-ventes@exemple.com
fraicheur: { disponible_avant: "07:00", tolerance_retard: "2h" }
volume_quotidien: { min: 80, max: 120 }
cle: [vente_id]
colonnes:
  - { nom: vente_id,   type: bigint,        nullable: false, unique: true }
  - { nom: client_id,  type: int,           nullable: true,  reference: clients.client_id }
  - { nom: montant,    type: "decimal(12,2)", nullable: false, regles: ["> 0", "<= 10000"], unite: EUR }
  - { nom: statut,     type: string,        nullable: false, domaine: [VALIDEE, ANNULEE] }
  - { nom: date_vente, type: date,          nullable: false, regles: ["<= date_traitement"] }
valeurs_interdites: ["client_id = 0 (sentinelle)"]
politique_changement: "préavis 30 jours pour tout changement cassant"
```

On voit que **les tests des sections 3 et 4 sont l'exécution de ce contrat** : chaque règle devient une requête qui renvoie les lignes en échec.

### 9.4 Changement compatible ou cassant ?

| Changement | Type | Pourquoi |
|---|---|---|
| Ajouter une colonne **nullable** | Compatible | Les consommateurs existants l'ignorent |
| **Supprimer** ou **renommer** une colonne | **Cassant** | Les requêtes qui l'utilisent échouent |
| **Changer le type** (par exemple `DECIMAL` en `STRING`) | **Cassant** | Les calculs cassent ou deviennent faux sans erreur |
| **Changer l'unité** (euros en centimes) | **Cassant, et sournois** | Aucune erreur technique, les chiffres sont faux (notre montant 120 000) |
| Élargir un domaine (nouveau statut) | À négocier | Les consommateurs qui filtrent sur la liste peuvent perdre des lignes |
| Resserrer une règle (plage plus étroite) | Généralement compatible | À annoncer quand même |

### 9.5 Les responsabilités

- **Le producteur** garantit le contrat (il teste **avant** de publier) et **prévient** avant un changement cassant.
- **Le consommateur** vérifie **à l'entrée** que ce qu'il reçoit respecte le contrat (ce blog) et ne suppose rien d'autre.
- **Le contrat se versionne** (version sémantique) et **se vérifie automatiquement**, par exemple dans l'intégration continue (schéma) et à chaque exécution (données).

Un contrat que personne ne vérifie est de la documentation ; un contrat vérifié à chaque exécution **est** un test.

---

## 10. Observabilité, outils et gestion des alertes

### 10.1 Les cinq piliers de l'observabilité des données

| Pilier | Question | Exemple |
|---|---|---|
| **Fraîcheur** | Les données sont-elles à jour ? | Retard de 2 jours (6.1) |
| **Volume** | Le nombre de lignes est-il plausible ? | 11 lignes au lieu de ~100 (6.2) |
| **Schéma** | La structure a-t-elle changé ? | Colonne ajoutée ou type modifié (6.5) |
| **Distribution** | Les valeurs ont-elles changé de profil ? | Taux de NULL, maximum, répartition (6.3, 6.5) |
| **Lignage** | D'où vient cette donnée, qui l'utilise ? | Quelle table alimente quel rapport |

Le **lignage** permet de répondre à « si cette source est fausse, quels rapports sont touchés ? » : c'est l'objet de la gouvernance (Blog 10).

### 10.2 Les outils (non testés ici)

Les tests de cette série sont du **SQL pur**, qui fonctionne partout. Des outils existent pour industrialiser la démarche : par exemple les **tests de dbt** (`unique`, `not_null`, `accepted_values`, `relationships`), **Great Expectations**, **Soda**, **Deequ** (pour Spark). Leur disponibilité et leur intégration dépendent de ta plateforme et de ta version : **évalue-les sur ton cluster avant de t'engager**. Le principe est toujours le même : une règle, un seuil, une action, un historique des résultats.

### 10.3 Où brancher les tests dans le pipeline

```
Extraction → [tests bronze : fraîcheur, volume, schéma]  → bloquent si échec
          → Nettoyage → [tests silver : unicité, validité, intégrité] → quarantaine / avertissement
          → [disjoncteur : taux de rejet, volume]                       → bloque le lot
          → Écriture (MERGE / OVERWRITE, Blog 8)
          → [réconciliation + totaux]                                   → bloque la publication
          → Publication + avancement du filigrane                       → seulement si tout est vert
```

Le point clé : **l'avancement du filigrane** (Blog 8) est la **dernière** étape. Si un test bloquant échoue, le filigrane n'avance pas, et la prochaine exécution reprend au même endroit.

### 10.4 Les alertes : peu, claires, actionnables

| Sévérité | Qui est prévenu, comment | Délai attendu |
|---|---|---|
| **BLOQUANT** | Astreinte (message immédiat) | Quelques heures |
| **ERREUR** (quarantaine qui grossit) | Propriétaire de la source (ticket) | Un jour ouvré |
| **AVERTISSEMENT** | Tableau de bord, revue hebdomadaire | Suivi de tendance |

**La fatigue d'alerte est le plus grand risque.** Des alertes qui se déclenchent tous les jours sans action finissent **ignorées**, y compris la vraie. Pour chaque alerte, définis **qui fait quoi** (un *runbook* : vérifier telle requête, contacter tel propriétaire, rejouer telle étape), et supprime ou ajuste toute alerte qui n'appelle pas d'action.

---

## 11. Les pièges de la qualité de données

| # | Piège | Conséquence | Remède |
|---|---|---|---|
| 1 | **Ne tester que le schéma** (types, colonnes) | Le montant 120 000 est un `DECIMAL` valide | Tests de **contenu** : plages, domaines, unicité |
| 2 | **Supprimer silencieusement** les lignes invalides | `lues ≠ chargées + rejetées + doublons`, impossible à expliquer | Quarantaine + invariant de comptage |
| 3 | **Corriger à l'aveugle** (remplacer un montant NULL par 0) | Les sommes sont fausses, sans trace | Corriger seulement ce qui est **sûr** ; sinon quarantaine |
| 4 | **Un test « zéro ligne » qui passe sur une table vide** | Un chargement vide passe tous les tests de contenu | **Toujours** un test de **volume minimal** en plus |
| 5 | **`NOT IN` dans un test** | Un NULL dans la liste vide le résultat : le test ne trouve plus rien (Blog 1) | `LEFT JOIN ... IS NULL` ou `NOT EXISTS` |
| 6 | **Comparaison avec NULL** (`montant <= 0` ne capture pas les NULL) | Les valeurs manquantes échappent au test de plage | Un test de complétude **séparé** (N1) |
| 7 | **Référence de volume contaminée** (jour courant inclus) | L'anomalie fait monter sa propre moyenne | Exclure le jour courant de la fenêtre |
| 8 | **Seuils fixes ignorant la saisonnalité** | Fausses alertes le lundi, ou anomalies manquées | Comparer au même jour de la semaine |
| 9 | **Réconciliation circulaire** (les deux totaux utilisent la même logique fautive) | Les deux concordent, et sont faux | Réconcilier avec une source **indépendante** |
| 10 | **Tests qui échouent en permanence** | Ignorés (fatigue d'alerte) | Corriger la cause ou supprimer le test |
| 11 | **`current_date()` dans un test** | Non rejouable, backfill impossible (Blog 8) | Date de traitement en **paramètre** |
| 12 | **Quarantaine sans propriétaire** | Les lignes s'accumulent, personne n'agit | Propriétaire, délai de traitement, rejeu automatisé |
| 13 | **Pas de fixtures** pour les transformations | Un changement de règle casse un cas sans que personne ne le voie | Fixtures, cas limites, tests de non-régression |

---

## 12. Résolution de l'incident

### 12.1 Ce qui s'est passé

Le lot du 10 avril contenait neuf défauts : un doublon, un statut en minuscules, cinq lignes à rejeter et deux lignes utilisables mais à surveiller. Sans contrôle, le pipeline a chargé toutes les lignes dont le statut vaut **exactement** `'VALIDEE'`, doublon compris : d'où un CA de **120 273** composé de :

| Ligne | Contribution | Défaut |
|---|---|---|
| 305 | + 120 000 | Montant 100 fois trop grand |
| 304 | + 120 | Client inexistant (utilisable, à suivre) |
| 301 ×2 | + 60 + 60 | Doublon : la même vente comptée deux fois |
| 311 | + 33 | Valide |
| 306 | + 30 | Date en 2025 |
| 307 | + 20 | Client NULL (utilisable, à suivre) |
| 303 | **- 50** | Montant négatif |

Et l'erreur va dans les **deux sens** : la vente 302 (statut en minuscules, 45) est **absente** du total, alors qu'elle est valide.

### 12.2 Ce que fait le pipeline corrigé

| Étape | Résultat sur ce lot |
|---|---|
| Déduplication | 11 → 10 lignes (la 301 en double est retirée) |
| Tests de ligne (section 3) | 9 tests, 1 échec chacun |
| Classification (section 4.2) | 5 lignes OK (dont 302 corrigée, 304 et 307 en avertissement), **5 rejets** |
| Réconciliation (section 5.1) | `11 = 5 + 5 + 1` ✓ |
| **Disjoncteur (section 4.4)** | **50 % de rejets > seuil de 20 % → lot BLOQUÉ** |
| Tests de volume (section 6.2) | Lot de 11 lignes au lieu de ~100 → z-score d'environ **-39** → alerte |
| Filigrane | **N'avance pas** |

Conséquences :

- **Le dashboard n'affiche pas 120 273.** Il reste sur les chiffres de la veille (en retard, mais **juste**) et une **alerte** part vers l'astreinte, avec la liste des raisons de rejet.
- Si la politique avait été **A** (charger le propre) : **5 lignes, CA de 278**, et 5 lignes en quarantaine avec leur raison.
- Dans les deux cas : **aucune ligne n'est perdue silencieusement** (invariant de comptage), et la source reçoit un ticket précis.

### 12.3 Ce qu'on met en place durablement

1. **Les neuf tests** (section 3) et le **test de lot** (section 4.4) à chaque exécution, avec leurs résultats dans `dq_resultats`.
2. **Un contrat de données** avec le producteur (section 9), notamment sur l'**unité des montants**, la **casse des statuts** et la **date de traitement**.
3. **Un test de volume** (section 6.2), pour détecter un chargement presque vide ou doublé.
4. **Les fixtures** de ce blog (302, 305, 306...) dans la suite de tests de la requête de classification (section 8).
5. **Un propriétaire** et un délai pour la quarantaine.

### 12.4 Récapitulatif

| Question | Réponse |
|---|---|
| Pourquoi 120 273 ? | Aucun contrôle : montants aberrants, doublon, ligne de 2025, négatif… chargés tels quels |
| Pourquoi le chargement a-t-il « réussi » ? | Réussir techniquement ≠ être juste : aucune erreur, aucun plantage |
| Comment l'éviter ? | Tests de ligne, quarantaine, disjoncteur de lot, réconciliation, volume |
| Que fait-on d'un lot à 50 % de rejets ? | On le **bloque** et on alerte : mieux vaut un dashboard en retard qu'un dashboard faux |
| Comment s'assurer que ça ne revienne pas ? | Contrat de données + fixtures de non-régression |

---

## 13. 10 questions d'entretien

**1. Comment t'assures-tu de la qualité des données dans un pipeline ?**
Des tests automatisés à chaque exécution, à plusieurs niveaux : contenu des lignes (unicité, complétude, validité, intégrité), lot (taux de rejet, volume, fraîcheur), réconciliation entre couches. Une politique claire pour les lignes invalides, des seuils qui arrêtent le pipeline, et des résultats conservés pour suivre les tendances.

**2. Quelles sont les dimensions de la qualité de données ?**
Unicité, complétude, validité, cohérence, intégrité référentielle, exactitude, fraîcheur, volume. Chacune se traduit par des tests précis.

**3. Que fais-tu d'une ligne invalide ?**
Selon le cas : la **corriger** si la correction est sûre, l'**accepter avec avertissement** si elle reste utilisable, la **mettre en quarantaine** si elle est invalide et non devinable, ou **bloquer le lot** si le taux de défauts révèle un problème systémique. Jamais la supprimer silencieusement.

**4. Pourquoi un test de qualité peut-il passer alors que les données sont fausses ?**
Parce qu'il ne teste pas la bonne chose (schéma seul), parce qu'un test « zéro ligne en échec » passe sur une table vide, ou parce que la réconciliation compare deux calculs qui partagent la même erreur. D'où la combinaison de tests de contenu, de volume et de réconciliation indépendante.

**5. Comment détectes-tu un chargement « réussi » mais vide ou incomplet ?**
Par un test de volume : comparer le nombre de lignes du jour à la moyenne et à l'écart-type des jours précédents (en excluant le jour courant), ou à un rapport (±30 %), en tenant compte de la saisonnalité.

**6. Comment réconcilies-tu deux tables ?**
Comptage avant / après avec l'invariant `lues = chargées + rejetées + doublons`, comparaison des totaux, et `EXCEPT` dans les deux sens pour trouver les lignes en trop et manquantes.

**7. Qu'est-ce qu'un disjoncteur (*circuit breaker*) dans un pipeline ?**
Un test de lot qui **arrête** le chargement quand les défauts dépassent un seuil (par exemple 20 % de rejets) : la cause est probablement systémique (source), et charger « la partie propre » masquerait le problème.

**8. Comment testes-tu une transformation SQL ?**
Avec des fixtures (entrée connue, sortie attendue, comparaison par `EXCEPT` dans les deux sens), des cas limites aux frontières, des tests d'invariants (conservation des lignes, idempotence), et en transformant chaque incident en test de non-régression.

**9. Qu'est-ce qu'un contrat de données, et que contient-il ?**
Un accord écrit et vérifiable entre producteur et consommateur : schéma, sémantique (unités, définitions), règles de qualité, fraîcheur, volume, propriétaire, politique de changement. Il se versionne et se vérifie automatiquement.

**10. Un changement de source est-il compatible ou cassant ?**
Ajouter une colonne nullable est compatible ; supprimer ou renommer une colonne, changer un type ou une unité est cassant (l'unité est le pire cas : aucune erreur technique, des chiffres faux). Tout changement cassant exige un préavis et une version du contrat.

---

## 14. 5 exercices

### Exercice 1 : écrire un test
Écris le test D1 (statut dans le domaine) sur `lot_dedup` et donne le résultat attendu. Pourquoi normalise-t-on avant de tester ?

<details>
<summary>Solution</summary>

```sql
SELECT * FROM lot_dedup
WHERE statut IS NULL OR UPPER(TRIM(statut)) NOT IN ('VALIDEE', 'ANNULEE');
```

Résultat : **une seule ligne, la 308** (`EXPEDIEE`). La 302 (`validee`) **n'est pas** en échec car, après normalisation, elle appartient au domaine : c'est le test D2 qui la signale (à corriger). Sans normalisation, la 302 serait rejetée à tort. Le test teste le **sens** du statut, pas sa **casse**.
</details>

### Exercice 2 : rejeter, corriger, quarantaine, avertir ?
Pour chaque cas, choisis l'action : (a) montant négatif ; (b) statut en minuscules ; (c) client inconnu ; (d) date dans le futur ; (e) doublon exact ; (f) 45 % des lignes d'un lot sont invalides.

<details>
<summary>Solution</summary>

(a) **Quarantaine** (valeur invalide, impossible à deviner : remboursement ? erreur de signe ?). (b) **Corriger** (normalisation sûre, tracée). (c) **Accepter avec avertissement** (rattacher au membre « inconnu », suivre le taux). (d) **Quarantaine** (date impossible). (e) **Corriger** (déduplication, comptée dans l'invariant). (f) **Bloquer le lot** (disjoncteur : problème systémique probable).
</details>

### Exercice 3 : l'invariant de comptage
Un lot de 11 lignes brutes contient un doublon. Après le pipeline, 5 lignes sont chargées et 5 rejetées. Vérifie l'invariant. Que signifierait un résultat `chargées + rejetées = 11` ?

<details>
<summary>Solution</summary>

`lues = chargées + rejetées + doublons` donne `11 = 5 + 5 + 1` : **l'invariant tient**. Si `chargées + rejetées = 11`, alors **le doublon n'a pas été retiré** (il a été chargé ou rejeté comme une ligne à part entière) : par exemple 6 chargées + 5 rejetées, avec la clé 301 présente **deux fois** dans la cible. Le test d'unicité de clé (C1, Blog 8) le détecterait aussi.
</details>

### Exercice 4 : le volume
Le volume du jour est de 95, puis de 11, puis de 200, avec une moyenne de 100 et un écart-type de 2,27 sur les 13 jours précédents. Calcule le z-score et le rapport de chacun, et dis lesquels alertent. Pourquoi combiner z-score et rapport ?

<details>
<summary>Solution</summary>

| Volume | z-score | Rapport | Alerte ? |
|---|---|---|---|
| 95 | -2,2 | 0,95 | Non |
| 11 | -39,2 | 0,11 | **Oui** |
| 200 | +44,0 | 2,00 | **Oui** |

L'écart-type est très faible (2,27 pour une base de 100) : le z-score devient hypersensible (une variation de 8 % donnerait déjà |z| > 3). Le **rapport** (±30 %, par exemple) borne l'alerte en termes métier ; le z-score se charge des changements subtils sur une série stable. Les deux ensemble limitent les fausses alertes sans rater les vraies.
</details>

### Exercice 5 : refais sans regarder
Le lot suivant (9 lignes brutes, date de traitement 2024-04-10, mêmes règles qu'en section 4.2) :

| arrivee | vente_id | client_id | montant | statut | date_vente |
|---|---|---|---|---|---|
| 1 | 401 | 1 | 25.00 | VALIDEE | 2024-04-10 |
| 2 | 402 | 2 | 0.00 | VALIDEE | 2024-04-10 |
| 3 | 403 | 3 | 18.00 | annulee | 2024-04-10 |
| 4 | 404 | 4 | 40.00 | VALIDEE | 2024-04-11 |
| 5 | 405 | 99 | 22.50 | VALIDEE | 2024-04-10 |
| 6 | 406 | 5 | NULL | VALIDEE | 2024-04-10 |
| 7 | 407 | 1 | 130.00 | VALIDEE | 2024-04-10 |
| 8 | 408 | 2 | 12.00 | VALIDEE | 2024-04-10 |
| 9 | 401 | 1 | 25.00 | VALIDEE | 2024-04-10 |

Classe chaque ligne, donne les nombres chargées / rejetées / doublons, le CA chargé, le taux de rejet, et dis ce que fait le disjoncteur à 20 %.

<details>
<summary>Solution</summary>

- **Doublon** : la deuxième ligne de la 401 (9 → 8 lignes).
- 401 : **OK** ; 402 : **REJET** (montant non positif) ; 403 : **OK + correction** (`annulee` → `ANNULEE`) ; 404 : **REJET** (date future) ; 405 : **OK + avertissement** (client 99 inconnu) ; 406 : **REJET** (montant NULL) ; 407 : **OK** ; 408 : **OK**.
- **Chargées : 5** (401, 403, 405, 407, 408) ; **rejetées : 3** (402, 404, 406) ; **doublons : 1**. Invariant : `9 = 5 + 3 + 1` ✓.
- **CA chargé : 207,50** (`25 + 18 + 22,5 + 130 + 12`).
- **Taux de rejet : 3 / 8 = 37,5 %**, soit **au-dessus de 20 %** : le **disjoncteur bloque** le lot.
</details>

---

## 15. Checklist avant de livrer un pipeline

- [ ] Chaque dimension de qualité pertinente a **au moins un test** (unicité, complétude, validité, intégrité, fraîcheur, volume)
- [ ] Un test = une requête qui renvoie les **lignes en échec** ; **zéro ligne = succès**
- [ ] Un test de **volume minimal** protège contre le « zéro ligne en échec » sur une table vide
- [ ] Les tests évitent les pièges du NULL (`NOT IN`, comparaisons avec NULL)
- [ ] Une **politique** par type de défaut : corriger, avertir, quarantaine, bloquer
- [ ] **Aucune ligne n'est supprimée silencieusement** : l'invariant `lues = chargées + rejetées + doublons` est testé
- [ ] Un **disjoncteur** sur le taux de rejet du lot, avec un seuil documenté
- [ ] La **réconciliation** avec une source indépendante, en comptes et en totaux
- [ ] Les tests de volume **excluent le jour courant** de leur référence et tiennent compte de la saisonnalité
- [ ] Les **résultats des tests** sont enregistrés (`dq_resultats`) et leur tendance est suivie
- [ ] Les transformations ont des **fixtures**, des **cas limites** et des **tests de non-régression**
- [ ] Un **contrat de données** écrit couvre schéma, unités, domaines, fraîcheur, volume et politique de changement
- [ ] Le **filigrane n'avance qu'après** la réussite de tous les tests bloquants
- [ ] La **quarantaine** a un propriétaire et un rejeu ; les **alertes** sont peu nombreuses, avec un runbook

---

## 16. Prochain blog et blogs restants

**[Blog 10 : tech lead]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-10-tech-lead" >}}).** Le dernier blog de la série change de perspective : il ne s'agit plus d'écrire une requête juste, mais de **faire écrire du SQL juste à une équipe**. Standards de code SQL, **revue de code**, organisation des couches et des responsabilités, **gouvernance** (catalogue, lignage, droits d'accès, données personnelles), **coûts**, choix d'architecture et **compromis**, et comment **répondre à une question d'architecture** en entretien. Il s'appuie sur les neuf blogs précédents comme un fil rouge.

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
| ✅ 9 | Data quality et tests | Tests, quarantaine, disjoncteur, réconciliation, volume, contrats de données |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 10

Sans regarder la suite : (1) tu reprends une équipe de cinq personnes qui écrit du SQL de façon hétérogène. Quels **cinq standards** imposes-tu en premier, et comment les **fais-tu respecter** sans freiner l'équipe ? (2) Lors d'une **revue de code**, quelles sont les **cinq questions** que tu poses systématiquement à une requête ? (3) Comment **justifies-tu** un choix d'architecture (par exemple Hive contre Spark contre Impala pour un usage donné) à quelqu'un qui n'est pas technique ? Si tu hésites, le Blog 10 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de fabriquer ton propre lot « sale », d'écrire les tests avant de regarder les solutions, et de vérifier que ton pipeline bloque ce qu'il doit bloquer.*