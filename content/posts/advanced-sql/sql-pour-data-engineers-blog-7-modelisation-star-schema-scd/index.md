---
title: "SQL pour Data Engineers, Blog 7 : modélisation, star schema et historisation (SCD)"
description: "Modéliser les données analytiques avec un star schema, des clés de substitution et l'historisation SCD."
weight: 7
---

# SQL pour Data Engineers, Blog 7 : modélisation, star schema et historisation (SCD)

> **Série** : SQL, de basique à tech lead · **Niveau** : 4-5/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Les écarts de dialecte sont signalés.
> **Prérequis** : [Blogs SQL 1 à 6]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-6-performance-jointures-distribuees-data-skew" >}}). On réutilise `ventes_dedup`, `clients_dedup`, `ca_jour` et le cas d'Alice (Premium puis Gold).

> **Ce qui a été vérifié, et ce qui ne l'a pas été.** La **logique SQL** de ce blog (construction d'une dimension SCD2, rapports « segment actuel » vs « segment à la date de la vente », mise à jour ensembliste rejouable, contrôles d'intervalles, calendrier) a été **exécutée et recoupée sur SQLite** avec les petites tables de la série : **tous les chiffres du blog en proviennent**. La **syntaxe** propre à Hive, Spark et Impala (`md5`, `sequence`, `explode`...) et les aspects de **performance** n'ont **pas** été testés sur un cluster : à reproduire chez toi.

---

## 0. Pourquoi ce blog existe

Les six premiers blogs portaient sur le **langage** et son **exécution**. Celui-ci porte sur la **forme des données** : savoir quelles tables construire pour que les requêtes soient **justes, simples et rapides**. En entretien « tech lead », c'est là que ça se joue : *« Comment tu modéliserais ce domaine ? »*, *« Comment tu gardes l'historique d'un changement ? »*, *« Pourquoi une clé de substitution ? »*.

Le scénario :

> **Jeudi, 9h40.** La Finance te montre deux versions du rapport « CA validé par segment de client » pour mars et avril :
> - le dashboard affiche **Gold 1 305**, **Premium 300**, **Standard 1 425** ;
> - un analyste dit : *« Alice n'est Gold que depuis le 15 mars. Ses 1 225 de ventes du 1er mars ont été réalisées quand elle était **Premium**. Et David : en mars, son segment n'était même pas renseigné. »*
>
> *« Les deux chiffres sont-ils justes ? Et pourquoi, à chaque changement de segment d'un client, le CA **passé** d'un segment bouge-t-il ? »*

Les deux chiffres sont justes : ils répondent à **deux questions différentes** (le segment **actuel** ou le segment **à la date de la vente**). Le rôle du data engineer est de **modéliser** les données pour pouvoir répondre aux deux, sans se tromper et sans réécrire l'histoire.

### Plan

1. [Le point de départ](#1-le-point-de-départ)
2. [Les concepts de base : grain, faits, dimensions](#2-les-concepts-de-base--grain-faits-dimensions)
3. [Le schéma en étoile de notre domaine](#3-le-schéma-en-étoile-de-notre-domaine)
4. [Les clés de substitution](#4-les-clés-de-substitution)
5. [Historiser les changements : les SCD](#5-historiser-les-changements--les-scd)
6. [Maintenir une dimension SCD2 en SQL](#6-maintenir-une-dimension-scd2-en-sql)
7. [Les contrôles d'intégrité d'une dimension historisée](#7-les-contrôles-dintégrité-dune-dimension-historisée)
8. [La dimension calendrier](#8-la-dimension-calendrier)
9. [Les autres approches : table large, Data Vault, médaillon](#9-les-autres-approches--table-large-data-vault-médaillon)
10. [Les pièges de modélisation](#10-les-pièges-de-modélisation)
11. [Résolution de l'incident](#11-résolution-de-lincident)
12. [10 questions d'entretien](#12-10-questions-dentretien)
13. [5 exercices](#13-5-exercices)
14. [Checklist avant de livrer un modèle](#14-checklist-avant-de-livrer-un-modèle)
15. [Prochain blog et blogs restants](#15-prochain-blog-et-blogs-restants)

---

## 1. Le point de départ

On utilise les tables du Blog 3 (`ventes_dedup`, `clients_dedup`) et on ajoute **l'historique des changements** tel qu'il arrive de la source : une ligne par version d'un client, avec sa **date d'effet**.

```sql
USE blog_sql;

CREATE TABLE clients_historique (
  client_id  INT,
  nom        STRING,
  pays       STRING,
  segment    STRING,
  date_effet DATE        -- date à laquelle cette version devient valable
) STORED AS PARQUET;

INSERT INTO clients_historique VALUES
  (1, 'Alice Martin', 'FR', 'Premium',  DATE '2023-01-15'),
  (1, 'Alice Martin', 'FR', 'Gold',     DATE '2024-03-15'),   -- changement de segment
  (2, 'Bruno Diallo', 'SN', 'Standard', DATE '2023-03-02'),
  (3, 'Chloé Benali', 'MA', 'Premium',  DATE '2023-05-20'),
  (4, 'David Chen',   'FR', NULL,       DATE '2024-01-10'),   -- segment inconnu...
  (4, 'David Chen',   'FR', 'Standard', DATE '2024-03-15'),   -- ...puis renseigné
  (5, 'Emma Rossi',   'IT', 'Standard', DATE '2024-02-14'),
  (6, 'Farid Haddad', 'MA', 'Standard', DATE '2024-06-01');
```

Rappel des ventes **validées** qui nous intéressent, avec leur date (table `ventes_dedup`) :

| vente_id | client_id | date_vente | montant |
|---|---|---|---|
| 101 | 1 | 2024-03-01 | 1200.00 |
| 102 | 1 | 2024-03-01 | 25.00 |
| 104 | 3 | 2024-03-07 | 150.00 |
| 105 | 3 | 2024-03-09 | NULL |
| 106 | 4 | 2024-03-10 | 1100.00 |
| 107 | NULL | 2024-03-11 | 25.00 |
| 108 | 99 | 2024-03-12 | 150.00 |
| 109 | 2 | 2024-03-15 | 300.00 |
| 201 | 1 | 2024-04-02 | 80.00 |
| 202 | 3 | 2024-04-05 | 150.00 |
| 203 | 5 | 2024-04-08 | 25.00 |

(Plus la vente 103, annulée, et la vente 110, de statut NULL, qui ne sont pas « validées ».)

> **Dialecte** : si le littéral `DATE '...'` est refusé sur ta version, utilise `CAST('...' AS DATE)`.

---

## 2. Les concepts de base : grain, faits, dimensions

### 2.1 Deux mondes : transactionnel et analytique

| | Transactionnel (OLTP) | Analytique (OLAP, entrepôt, lac) |
|---|---|---|
| Objectif | Enregistrer des opérations | Analyser, agréger, comparer |
| Accès | Quelques lignes, lectures et écritures fréquentes | Beaucoup de lignes, lecture d'agrégats |
| Modèle | **Normalisé** (éviter la redondance) | **Dénormalisé** (faciliter la lecture) |
| Exemple | Table `commandes`, `clients`, `produits` reliées par des clés | Schéma **en étoile** : un fait entouré de dimensions |

Dans un entrepôt colonnaire (Blog 5), répéter une valeur (un nom de pays) coûte peu grâce à la compression, alors que **faire une jointure de plus** coûte (shuffle, Blog 6). C'est pourquoi on **dénormalise** côté analytique.

### 2.2 Le grain : la décision la plus importante

Le **grain** d'une table de faits dit **ce que représente une ligne**. C'est la première chose à écrire, avant toute colonne :

> *« Une ligne de `fait_ventes` = **une ligne de vente** (un produit vendu à un client, un jour donné). »*

Le grain détermine ce qu'on peut calculer, et ce qui produit du **double comptage**. Mélanger des grains dans une même table (par exemple, des lignes de vente **et** des totaux de commande) est l'erreur la plus coûteuse (section 10).

### 2.3 Faits et dimensions

| | Table de **faits** | Table de **dimension** |
|---|---|---|
| Contient | Les **événements mesurables** : montant, quantité | Le **contexte** : qui, quoi, où, quand |
| Taille | Très grande, grandit en continu | Plutôt petite, change lentement |
| Colonnes | **Mesures** (numériques) + **clés** vers les dimensions | **Attributs descriptifs** (nom, pays, segment, catégorie) |
| Exemple | `fait_ventes` | `dim_client`, `dim_produit`, `dim_date` |

Les **mesures** se classent par **additivité** :

| Type | Peut-on les sommer ? | Exemple |
|---|---|---|
| **Additive** | Sur toutes les dimensions | `montant`, `quantite` |
| **Semi-additive** | Sur certaines dimensions, pas sur le temps | Solde de stock (on ne somme pas deux jours) |
| **Non additive** | Jamais directement | Prix unitaire, ratio, pourcentage |

**Règle pratique** : une mesure non additive se stocke par ses **composants** (le numérateur et le dénominateur), et le ratio se calcule **à la lecture**. C'est la leçon de la moyenne des moyennes du Blog 1.

### 2.4 Les trois grands types de tables de faits

| Type | Une ligne = | Exemple |
|---|---|---|
| **Transactionnel** | Un événement | Une ligne de vente |
| **Snapshot périodique** | L'état à intervalle régulier | Stock en fin de journée |
| **Snapshot cumulatif** | Le cycle de vie d'un processus, mis à jour | Commande : dates de création, paiement, expédition, livraison |

Notre cas est un fait **transactionnel**.

---

## 3. Le schéma en étoile de notre domaine

### 3.1 Les tables

```
                       dim_date
                          │
 dim_client ──────── fait_ventes ──────── dim_produit
```

Une table de faits au centre, des dimensions autour : c'est une **étoile**. Une requête typique joint le fait à une ou deux dimensions et agrège.

### 3.2 Les dimensions

```sql
-- dim_produit : un produit = une ligne
CREATE TABLE dim_produit STORED AS PARQUET AS
SELECT
  md5(produit)  AS produit_sk,
  produit,
  categorie
FROM (SELECT DISTINCT produit, categorie FROM ventes_dedup) p;

-- dim_client : on la construit en section 5 (avec l'historique)
-- dim_date   : on la construit en section 8
```

Les dimensions sont **larges et descriptives** : on y met tous les attributs utiles pour filtrer et regrouper (pays, segment, catégorie...), même s'ils se répètent.

### 3.3 La table de faits

Le grain est écrit en haut : **une ligne par ligne de vente**.

```sql
CREATE TABLE fait_ventes (
  vente_id    BIGINT,         -- dimension « dégénérée » : identifiant d'origine, sans dimension associée
  date_vente  DATE,           -- clé vers dim_date
  client_sk   STRING,         -- clé de substitution vers dim_client (voir section 5)
  produit_sk  STRING,         -- clé de substitution vers dim_produit
  statut      STRING,         -- attribut à faible cardinalité
  quantite    INT,            -- mesure additive
  montant     DECIMAL(10,2)   -- mesure additive
) STORED AS PARQUET;
```

Trois idées à retenir :

- **`vente_id` est une dimension dégénérée** : un identifiant utile (pour retrouver la ligne, dédupliquer) mais qui n'a pas de table de dimension.
- **Les clés vers les dimensions sont des clés de substitution** (section 4), pas les identifiants métier.
- **Les mesures sont additives** (`montant`, `quantite`), ce qui permet toutes les agrégations.

### 3.4 Le membre « inconnu »

Dans notre jeu de données, la vente 107 a un `client_id` NULL et la vente 108 un client 99 qui n'existe pas. Une jointure interne les **perdrait** (Blog 1). La pratique standard : une ligne **« inconnu »** dans chaque dimension, vers laquelle pointent les faits sans correspondance :

```sql
-- Une ligne spéciale dans la dimension client
--   client_sk = '-1', client_id = NULL, nom = 'Inconnu', pays = 'INCONNU', segment = 'INCONNU'
-- Dans le fait : COALESCE(clé trouvée, '-1')
```

Ainsi **aucune ligne de fait n'est perdue**, et les ventes sans client apparaissent explicitement sous `INCONNU` dans les rapports (comme au Blog 1). **Attention au Blog 6** : à grande échelle, une clé « inconnu » qui concentre beaucoup de lignes est une **clé chaude** : surveille sa part.

### 3.5 Étoile, flocon ou table large ?

| Modèle | Principe | Avantage | Inconvénient |
|---|---|---|---|
| **Étoile** | Dimensions **dénormalisées** | Requêtes simples, peu de jointures | Redondance dans les dimensions |
| **Flocon** (*snowflake*) | Dimensions **normalisées** en sous-tables | Moins de redondance | Plus de jointures, plus de shuffles |
| **Table large** (*one big table*) | Tout dans une seule table dénormalisée | Aucune jointure à la lecture | Taille, historique difficile, modification des attributs coûteuse |

Dans un lac de données colonnaire, l'**étoile** reste le compromis le plus courant pour la couche de reporting. On choisit la **table large** pour des usages très précis (tableaux de bord figés, apprentissage automatique).

---

## 4. Les clés de substitution

### 4.1 Pourquoi ne pas utiliser `client_id` directement ?

Un `client_id` est une **clé naturelle** (métier). Il pose trois problèmes :

1. **Il n'est pas unique dans le temps** dès qu'on historise : Alice a **deux versions**, donc deux lignes pour `client_id = 1`. Quelle ligne choisir dans une jointure ?
2. **Il appartient à la source** : il peut changer (fusion de systèmes), être réutilisé, ou différer d'un système à l'autre.
3. **Il n'existe pas pour les cas spéciaux** (le membre « inconnu »).

Une **clé de substitution** (*surrogate key*) est un identifiant **propre à l'entrepôt**, qui identifie **une ligne de dimension** (donc une **version** d'un client). Le fait pointe vers cette version précise.

### 4.2 Comment la générer en distribué

| Méthode | Principe | Avantage | Inconvénient |
|---|---|---|---|
| **Séquence** (`ROW_NUMBER() OVER (ORDER BY ...)` + décalage) | Entier croissant | Compact, lisible | Une window globale (tout vers **une** tâche, Blog 3, section 11), **non déterministe** si l'ordre n'est pas unique, difficile à rejouer |
| **Identifiant unique généré** (`uuid()`, `monotonically_increasing_id()`) | Valeur unique par ligne | Pas de goulot | **Différent à chaque exécution** : casse la rejouabilité |
| **Hachage déterministe** de la clé naturelle + date de début | `md5(concat_ws('\|', client_id, date_debut))` | **Même clé à chaque exécution**, parallélisable, rejouable | Chaîne de 32 caractères, collisions théoriquement possibles (négligeables avec un bon hachage) |

On privilégie souvent le **hachage déterministe** : une exécution **rejouée** (Blog 8) produit **exactement les mêmes clés**, et aucune jointure globale n'est nécessaire. Le choix dépend de la taille et des exigences d'audit.

```sql
md5(concat_ws('|', CAST(client_id AS STRING), CAST(date_debut AS STRING)))  AS client_sk
```

> Sépare toujours les composants avec un **délimiteur** (`'|'`) : sans lui, `('1', '23')` et `('12', '3')` donnent la même concaténation, donc le même hachage.

---

## 5. Historiser les changements : les SCD

Une **dimension à variation lente** (*Slowly Changing Dimension*, SCD) est une dimension dont les attributs **changent** de temps en temps. La question : que fait-on de l'ancienne valeur ?

### 5.1 Les types

| Type | Principe | Historique ? | Quand |
|---|---|---|---|
| **0** | On ne modifie jamais | Valeur d'origine seulement | Attribut figé (date de naissance) |
| **1** | **Écraser** l'ancienne valeur | **Non** | Correction d'une erreur, historique sans intérêt |
| **2** | **Nouvelle ligne** pour chaque version | **Complet** | Besoin d'analyser « tel que c'était à l'époque » |
| **3** | **Colonne** « valeur précédente » | **Une seule** version | Un seul changement à conserver |

Les types 4 et 6 (table d'historique séparée, combinaison 1+2+3) sont des variantes avancées.

### 5.2 Type 1 : le rapport « segment actuel »

Avec une dimension de type 1, chaque client n'a **qu'une ligne**, avec son segment **actuel** (Gold pour Alice, Standard pour David). C'est ce qui produit le chiffre du dashboard :

| segment | nb | ca |
|---|---|---|
| Gold | 3 | 1305.00 |
| Premium | 3 | 300.00 |
| Standard | 3 | 1425.00 |

Toutes les ventes d'Alice (101, 102, 201) sont attribuées à **Gold**, y compris celles de mars, réalisées quand elle était Premium. Et **à chaque changement de segment, le passé change**. Ce n'est pas un bug : c'est la **définition** du type 1. Elle répond à : « *quel est le CA des clients qui sont aujourd'hui Gold ?* ».

### 5.3 Type 3 : une colonne « précédent »

```sql
-- client_id | segment | segment_precedent | date_changement
--         1 | Gold    | Premium           | 2024-03-15
```

Simple, mais on ne garde **qu'un seul** changement. Utile rarement.

### 5.4 Type 2 : une ligne par version

On ajoute à la dimension : une **date de début**, une **date de fin** et un indicateur de **version courante**. Chaque changement **ferme** la ligne précédente et **ouvre** une nouvelle ligne.

**Construction à partir de l'historique** : la date de fin d'une version est la date d'effet de la **suivante** (`LEAD`, Blog 3), et la dernière version est ouverte jusqu'à une date lointaine.

```sql
CREATE TABLE dim_client_scd2 STORED AS PARQUET AS
SELECT
  md5(concat_ws('|', CAST(client_id AS STRING), CAST(date_effet AS STRING)))   AS client_sk,
  client_id,
  nom,
  pays,
  segment,
  date_effet                                                                    AS date_debut,
  COALESCE(LEAD(date_effet) OVER (PARTITION BY client_id ORDER BY date_effet),
           DATE '9999-12-31')                                                   AS date_fin,
  CASE WHEN LEAD(date_effet) OVER (PARTITION BY client_id ORDER BY date_effet) IS NULL
       THEN 1 ELSE 0 END                                                        AS est_courant
FROM clients_historique;
```

Résultat (colonnes lisibles, sans `client_sk`) :

| client_id | nom | pays | segment | date_debut | date_fin | est_courant |
|---|---|---|---|---|---|---|
| 1 | Alice Martin | FR | Premium | 2023-01-15 | 2024-03-15 | 0 |
| 1 | Alice Martin | FR | **Gold** | **2024-03-15** | 9999-12-31 | 1 |
| 2 | Bruno Diallo | SN | Standard | 2023-03-02 | 9999-12-31 | 1 |
| 3 | Chloé Benali | MA | Premium | 2023-05-20 | 9999-12-31 | 1 |
| 4 | David Chen | FR | NULL | 2024-01-10 | 2024-03-15 | 0 |
| 4 | David Chen | FR | Standard | 2024-03-15 | 9999-12-31 | 1 |
| 5 | Emma Rossi | IT | Standard | 2024-02-14 | 9999-12-31 | 1 |
| 6 | Farid Haddad | MA | Standard | 2024-06-01 | 9999-12-31 | 1 |

### 5.5 Les conventions d'intervalle

- **Intervalle semi-ouvert `[date_debut, date_fin[`** : la date de début est **incluse**, la date de fin **exclue**. La date de fin d'une version est égale à la date de début de la suivante : **sans trou ni chevauchement**. C'est la même règle que les bornes de dates du Blog 1 (`>= début AND < fin`).
- **Une date de fin lointaine** (`9999-12-31`) plutôt que `NULL` pour la version courante : la condition de jointure reste une simple comparaison, sans `OR date_fin IS NULL` (et sans piège de NULL).
- **L'indicateur `est_courant`** (ou date de fin = `9999-12-31`) permet de retrouver la version actuelle sans calcul.

### 5.6 Interroger : « segment à la date de la vente »

```sql
SELECT
  COALESCE(d.segment, 'INCONNU')   AS segment,
  COUNT(*)                         AS nb,
  SUM(v.montant)                   AS ca
FROM ventes_dedup v
JOIN dim_client_scd2 d
  ON  v.client_id  = d.client_id
  AND v.date_vente >= d.date_debut
  AND v.date_vente <  d.date_fin
WHERE v.statut = 'VALIDEE'
GROUP BY COALESCE(d.segment, 'INCONNU')
ORDER BY segment;
```

| segment | nb | ca |
|---|---|---|
| Gold | 1 | 80.00 |
| INCONNU | 1 | 1100.00 |
| Premium | 5 | 1525.00 |
| Standard | 2 | 325.00 |

Le total est **3 030** (jointure interne : les ventes 107 et 108 sont absentes). Détail pour Alice et David :

| vente_id | date_vente | segment à cette date |
|---|---|---|
| 101, 102 | 2024-03-01 | **Premium** (version `[2023-01-15, 2024-03-15[`) |
| 106 (David) | 2024-03-10 | **NULL** → `INCONNU` (version `[2024-01-10, 2024-03-15[`) |
| 201 | 2024-04-02 | **Gold** (version `[2024-03-15, 9999-12-31[`) |

Comparaison des deux visions, **sur le même total de 3 030** :

| Segment | Segment **actuel** (type 1) | Segment **à la date de la vente** (type 2) |
|---|---|---|
| Gold | 1305 | 80 |
| Premium | 300 | 1525 |
| Standard | 1425 | 325 |
| INCONNU | 0 | 1100 |
| **Total** | **3030** | **3030** |

L'**écart** est entièrement dû à Alice (1 225 de ventes de mars qui passent de Gold à Premium) et à David (1 100 qui passent de Standard à INCONNU). Aucun montant n'a été perdu : il a été **réaffecté**.

### 5.7 Où résoudre la version : à la requête ou au chargement ?

La jointure ci-dessus est une **jointure avec condition de plage** (`BETWEEN` sur dates). Elle fonctionne parce qu'elle contient aussi une **égalité** (`client_id`), ce qui permet un `SortMergeJoin` (Blog 6) puis le filtre de plage. **Sans égalité**, ce serait un produit cartésien filtré.

Deux stratégies :

| | **Résolution à la requête** (cette section) | **Résolution au chargement** (recommandée à grande échelle) |
|---|---|---|
| Principe | Le fait garde le `client_id`, on joint avec la plage de dates à chaque rapport | À l'insertion de chaque fait, on cherche la version valable **à la date de l'événement** et on stocke sa **clé de substitution** (`client_sk`) |
| Jointure à la lecture | Égalité + plage de dates | **Égalité simple** sur `client_sk` |
| Coût | Répété à chaque requête | Payé **une fois** au chargement |
| Cohérence | Dépend du code de chaque rapport | **Identique** pour tous les rapports |

```sql
-- Résolution au chargement : la clé de substitution est celle de la version valable à la date de la vente
INSERT OVERWRITE TABLE fait_ventes
SELECT
  v.vente_id,
  v.date_vente,
  COALESCE(d.client_sk, '-1')    AS client_sk,        -- '-1' : le membre « inconnu »
  p.produit_sk,
  v.statut,
  v.quantite,
  v.montant
FROM ventes_dedup v
LEFT JOIN dim_client_scd2 d
  ON  v.client_id  = d.client_id
  AND v.date_vente >= d.date_debut
  AND v.date_vente <  d.date_fin
LEFT JOIN dim_produit p ON v.produit = p.produit;
```

Un **`LEFT JOIN`** garantit qu'aucune ligne de fait n'est perdue (les ventes 107 et 108 reçoivent `'-1'`). Pour que l'histoire reste cohérente, les rapports peuvent **aussi** exposer le segment **actuel** : un fait peut porter à la fois la clé de la version historique **et** la clé de la version courante (ou le `client_id`, pour rejoindre `est_courant = 1`).

---

## 6. Maintenir une dimension SCD2 en SQL

Construire la dimension **une fois** depuis un historique complet est simple. La difficulté en production : **la mettre à jour** chaque jour à partir d'un **instantané** (*snapshot*) du référentiel source, sans perdre l'historique. (Avec `MERGE` et les tables transactionnelles, c'est le sujet du Blog 8 ; ici, une version **ensembliste en SQL pur**.)

### 6.1 Le principe en trois temps

À partir de la dimension actuelle et du nouvel instantané, **à la date de l'instantané** :

1. **Détecter** les clients **modifiés** (attributs différents de la version courante) et **nouveaux** (absents de la dimension) ;
2. **Fermer** la version courante des clients modifiés (`date_fin` = date de l'instantané, `est_courant` = 0) ;
3. **Ouvrir** une nouvelle version pour les clients modifiés et nouveaux (`date_debut` = date de l'instantané, `date_fin` = `9999-12-31`, `est_courant` = 1).

Tout le reste (versions historiques, clients inchangés) est **recopié tel quel**.

### 6.2 La requête

Situation de départ : la dimension **avant** le 15 mars (une seule version par client), et un instantané du 15 mars où Alice est passée à Gold et David à Standard.

```sql
-- dim_v0 : la dimension actuelle ; snap : le nouvel instantané du référentiel ; date : 2024-03-15
WITH courantes AS (
  SELECT * FROM dim_v0 WHERE est_courant = 1
),
a_changer AS (
  -- clients modifiés : au moins un attribut diffère (comparaison NULL-SAFE, voir 6.3)
  SELECT s.client_id
  FROM snap s
  JOIN courantes d ON s.client_id = d.client_id
  WHERE NOT (s.nom <=> d.nom AND s.pays <=> d.pays AND s.segment <=> d.segment)
  UNION
  -- clients nouveaux : absents de la dimension
  SELECT s.client_id
  FROM snap s
  LEFT JOIN courantes d ON s.client_id = d.client_id
  WHERE d.client_id IS NULL
)
-- 1) tout ce qui ne change pas : historique + versions courantes inchangées
SELECT client_id, nom, pays, segment, date_debut, date_fin, est_courant
FROM dim_v0
WHERE NOT (est_courant = 1 AND client_id IN (SELECT client_id FROM a_changer))

UNION ALL
-- 2) fermer les versions courantes des clients modifiés
SELECT client_id, nom, pays, segment, date_debut, DATE '2024-03-15' AS date_fin, 0 AS est_courant
FROM dim_v0
WHERE est_courant = 1 AND client_id IN (SELECT client_id FROM a_changer)

UNION ALL
-- 3) ouvrir les nouvelles versions (modifiés + nouveaux)
SELECT client_id, nom, pays, segment, DATE '2024-03-15', DATE '9999-12-31', 1
FROM snap
WHERE client_id IN (SELECT client_id FROM a_changer);
```

Résultat (les colonnes `client_sk` seraient recalculées avec le hachage de la section 4) :

| client_id | nom | pays | segment | date_debut | date_fin | est_courant |
|---|---|---|---|---|---|---|
| 1 | Alice Martin | FR | Premium | 2023-01-15 | **2024-03-15** | **0** |
| 1 | Alice Martin | FR | Gold | **2024-03-15** | 9999-12-31 | **1** |
| 2 | Bruno Diallo | SN | Standard | 2023-03-02 | 9999-12-31 | 1 |
| 3 | Chloé Benali | MA | Premium | 2023-05-20 | 9999-12-31 | 1 |
| 4 | David Chen | FR | NULL | 2024-01-10 | **2024-03-15** | **0** |
| 4 | David Chen | FR | Standard | **2024-03-15** | 9999-12-31 | **1** |
| 5 | Emma Rossi | IT | Standard | 2024-02-14 | 9999-12-31 | 1 |

On retrouve **exactement** la dimension construite d'un coup en 5.4 : Alice et David ont deux versions, les autres une seule.

### 6.3 Le piège : la détection de changement et le NULL

David passe d'un segment **NULL** à `Standard`. Si tu détectes le changement avec une comparaison classique :

```sql
WHERE s.segment <> d.segment      -- NULL <> 'Standard'  →  NULL  →  ligne ignorée !
```

alors `NULL <> 'Standard'` vaut **inconnu** (Blog 1), et **le changement de David n'est jamais détecté** : sa version reste « courante, segment NULL », et son historique est faux, **sans aucune erreur**. En rejouant le même scénario avec `<>`, la dimension contient David avec le segment `NULL` **ouvert jusqu'en 9999**, alors qu'Alice est correctement historisée.

La parade est la comparaison **null-safe** (`<=>`, ou `IS NOT DISTINCT FROM` selon le moteur), utilisée ci-dessus : `NULL <=> NULL` vaut vrai, `NULL <=> 'Standard'` vaut faux.

> **Variante** : si la dimension remplace les NULL par `'INCONNU'` (une bonne pratique pour éviter les NULL dans les rapports), applique **la même normalisation** (`COALESCE(segment, 'INCONNU')`) à l'instantané **avant** de comparer. Sinon, `NULL` (source) et `'INCONNU'` (dimension) seront vus comme différents **à chaque exécution**, et une nouvelle version sera créée tous les jours.

### 6.4 La rejouabilité

Cette requête est **idempotente** : si on la rejoue **avec le même instantané**, aucun client n'est « modifié » (les attributs de la version courante sont déjà ceux de l'instantané), donc rien ne change. Vérifié sur la miniature : rejouer l'instantané du 15 mars sur la dimension déjà mise à jour renvoie les **mêmes 7 lignes**. C'est une propriété essentielle pour un pipeline qui peut être relancé (Blog 8).

Deux précautions :

- **La date de l'instantané est un paramètre**, pas `current_date()` : rejouer le chargement d'hier doit produire les dates d'hier.
- **Les nouveaux clients** reçoivent ici la date de l'instantané comme date de début. En production, la **vraie** date d'effet est celle **de la source** (date d'inscription, date de création de la version), pas la date de chargement : sinon, les ventes antérieures au premier chargement ne trouvent aucune version (elles retombent sur « inconnu »).

---

## 7. Les contrôles d'intégrité d'une dimension historisée

Une dimension SCD2 mal maintenue est **silencieusement dangereuse** : elle ne plante pas, elle **fausse les chiffres**. Quatre contrôles à brancher à chaque exécution (Blog 9). Chacun doit renvoyer **zéro ligne** quand tout va bien.

### 7.1 Exactement une version courante par client

```sql
SELECT client_id, SUM(est_courant) AS nb_courants
FROM dim_client_scd2
GROUP BY client_id
HAVING SUM(est_courant) <> 1;
```

Aucune ligne sur notre dimension. Zéro version courante : un client « disparaît » des rapports sur le segment actuel. Deux : il est compté en double.

### 7.2 Aucun trou ni chevauchement entre versions

```sql
SELECT client_id, date_debut, date_fin, prochain_debut
FROM (
  SELECT client_id, date_debut, date_fin,
         LEAD(date_debut) OVER (PARTITION BY client_id ORDER BY date_debut) AS prochain_debut
  FROM dim_client_scd2
) t
WHERE prochain_debut IS NOT NULL
  AND date_fin <> prochain_debut;           -- >  : chevauchement ;  <  : trou
```

Avec l'intervalle semi-ouvert, la fin d'une version **doit** être égale au début de la suivante. Une valeur **supérieure** signale un **chevauchement**, une valeur **inférieure** un **trou**.

### 7.3 L'effet d'un chevauchement

Simulons une erreur : la version Premium d'Alice se termine le **2024-04-05** au lieu du 2024-03-15, donc elle chevauche la version Gold du 2024-03-15 au 2024-04-05. La vente 201 (2 avril, 80) tombe alors dans **deux** versions :

| | Lignes jointes | CA validé |
|---|---|---|
| Dimension correcte | 9 | **3 030** |
| Avec le chevauchement | **10** | **3 110** |

La vente est **comptée deux fois** (+ 80), sans erreur : c'est exactement la mécanique de la clé dupliquée du Blog 1, avec une plage de dates. Le contrôle 7.2 détecte la ligne fautive (Premium, fin 2024-04-05, suivie de Gold début 2024-03-15).

### 7.4 Chaque fait trouve exactement une version

Après la jointure fait → dimension, le nombre de lignes **doit rester égal** à celui du fait :

| Comptage | Valeur |
|---|---|
| Lignes de ventes (source) | 13 |
| Lignes après `LEFT JOIN` sur `dim_client_scd2` | **13** |

Plus de lignes : chevauchement (7.3). Moins de lignes : une jointure interne a perdu des faits (Blog 1). Toujours comparer **avant et après**.

### 7.5 Surveiller le membre « inconnu »

```sql
SELECT
  SUM(CASE WHEN d.client_id IS NULL THEN 1 ELSE 0 END)  AS faits_sans_version,
  COUNT(*)                                              AS total_faits
FROM ventes_dedup v
LEFT JOIN dim_client_scd2 d
  ON v.client_id = d.client_id AND v.date_vente >= d.date_debut AND v.date_vente < d.date_fin;
```

Résultat sur la miniature : **2 faits sur 13** (les ventes 107 et 108), soit environ **15 %**. C'est le signal de qualité à suivre dans le temps, avec un seuil d'alerte (Blog 9) : un chiffre qui grimpe signifie des clients qui arrivent **avant** leur dimension (arrivée tardive, section 10) ou une source qui se dégrade. À grande échelle, c'est aussi la clé chaude du Blog 6.

### 7.6 Réconciliation avec la source

```sql
SELECT
  (SELECT SUM(montant) FROM ventes_dedup WHERE statut = 'VALIDEE')        AS ca_source,
  (SELECT SUM(montant) FROM fait_ventes   WHERE statut = 'VALIDEE')       AS ca_fait;
```

Les deux valent **3 205** : le modèle ne perd ni ne crée de CA. Même principe qu'aux Blogs 1 à 4 : **réconcilier avec un total calculé autrement**.

---

## 8. La dimension calendrier

### 8.1 Pourquoi une table de dates

Les données de ventes ne contiennent que des jours **où il y a eu des ventes** (Blog 3, section 1.2). Pour les jours sans vente, il n'y a **aucune ligne** : un rapport « jour par jour » a des trous, et une fenêtre glissante calculée sur les **lignes** ne correspond pas à une durée (Blog 3, section 6). Une table `dim_date` règle ces deux problèmes et sert aussi à regrouper (par mois, trimestre, jour de la semaine).

### 8.2 La générer

```sql
-- Spark SQL
CREATE TABLE dim_date STORED AS PARQUET AS
SELECT
  d                                    AS date_jour,
  year(d)                              AS annee,
  quarter(d)                           AS trimestre,
  month(d)                             AS mois,
  date_format(d, 'yyyy-MM')            AS annee_mois,
  dayofmonth(d)                        AS jour,
  dayofweek(d)                         AS jour_semaine,       -- 1 = dimanche ... 7 = samedi
  date_format(d, 'EEEE')               AS nom_jour,
  dayofweek(d) IN (1, 7)               AS est_weekend,
  d = last_day(d)                      AS est_fin_de_mois
FROM (SELECT explode(sequence(DATE '2022-01-01', DATE '2026-12-31', INTERVAL 1 DAY)) AS d) t;

-- Hive : pas de fonction sequence, on génère les entiers autrement
--   SELECT date_add(DATE '2022-01-01', pos) AS d
--   FROM (SELECT posexplode(split(space(1825), ' ')) AS (pos, x)) t     -- 1 826 jours
```

Attention : la convention de `dayofweek` (**1 = dimanche**) n'est pas celle de la norme ISO (1 = lundi). Documente-la ou ajoute une colonne explicite.

### 8.3 Combler les jours sans vente

```sql
SELECT COUNT(*) AS jours_sans_vente
FROM dim_date d
LEFT JOIN ca_jour c ON d.date_jour = c.jour
WHERE d.date_jour BETWEEN DATE '2024-03-01' AND DATE '2024-04-08'
  AND c.jour IS NULL;
```

Du 1er mars au 8 avril, il y a **39 jours** dans le calendrier, dont **10** avec ventes : donc **29 jours sans vente** (le premier est le 2 mars). Les ajouter à 0 ne change **pas** le total (3 205) mais rend la série **complète**.

### 8.4 La somme sur 7 jours, enfin exacte avec `ROWS`

Sur un calendrier complet, `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` **est** une fenêtre de 7 jours calendaires, sans l'astuce du `RANGE` sur numéro de jour du Blog 3 :

```sql
SELECT jour, ca, ca_7j
FROM (
  SELECT
    d.date_jour                                    AS jour,
    COALESCE(c.ca, 0)                              AS ca,
    SUM(COALESCE(c.ca, 0)) OVER (ORDER BY d.date_jour
                                 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ca_7j
  FROM dim_date d
  LEFT JOIN ca_jour c ON d.date_jour = c.jour
  WHERE d.date_jour BETWEEN DATE '2024-03-01' AND DATE '2024-04-08'
) t
WHERE jour IN (SELECT jour FROM ca_jour)             -- on ne montre que les jours avec ventes
ORDER BY jour;
```

| jour | ca | ca_7j |
|---|---|---|
| 2024-03-01 | 1225.00 | 1225.00 |
| 2024-03-07 | 150.00 | 1375.00 |
| 2024-03-09 | 0.00 | 150.00 |
| 2024-03-10 | 1100.00 | 1250.00 |
| 2024-03-11 | 25.00 | 1275.00 |
| 2024-03-12 | 150.00 | 1425.00 |
| 2024-03-15 | 300.00 | 1575.00 |
| 2024-04-02 | 80.00 | 80.00 |
| 2024-04-05 | 150.00 | 230.00 |
| 2024-04-08 | 25.00 | 255.00 |

Ce sont **exactement** les valeurs du Blog 3 (section 6.2) : les deux méthodes (calendrier complet + `ROWS`, ou numéro de jour + `RANGE`) sont équivalentes.

### 8.5 Le piège : filtrer avant la fenêtre

Le filtre `WHERE jour IN (SELECT jour FROM ca_jour)` est placé **à l'extérieur**, **après** le calcul de la window. Si on le met **dans** la requête qui calcule la fenêtre (`... LEFT JOIN ca_jour c ... WHERE c.jour IS NOT NULL`), les jours sans vente disparaissent **avant** que la window les voie : on retombe sur une fenêtre de 7 **lignes**, et les résultats sont **faux** :

| jour | ca_7j correct | ca_7j avec le filtre **avant** la window |
|---|---|---|
| 2024-03-09 | 150.00 | **1375.00** |
| 2024-03-15 | 1575.00 | **2950.00** |
| 2024-04-02 | 80.00 | **1805.00** |
| 2024-04-08 | 255.00 | **1830.00** |

C'est l'**ordre d'exécution logique** du Blog 1 (section 3) : le `WHERE` s'exécute **avant** les window functions, donc il change les lignes qu'elles voient. Pour filtrer sur le résultat d'une window, on **encapsule** (CTE ou sous-requête) et on filtre à l'extérieur.

---

## 9. Les autres approches : table large, Data Vault, médaillon

### 9.1 La table large (*one big table*)

On pré-joint tout dans **une** table dénormalisée (faits + attributs des dimensions). Rapide à lire (aucune jointure, Blog 6), mais : la taille augmente, l'historique SCD2 se complique, et modifier un attribut oblige à **réécrire** de grandes quantités de lignes. À réserver à une **couche de consommation** précise, générée depuis l'étoile.

### 9.2 Le Data Vault (aperçu)

Le **Data Vault** sépare les données en trois types de tables, pensées pour l'**intégration de nombreuses sources** et l'**historisation complète** :

| Table | Contenu | Exemple |
|---|---|---|
| **Hub** | La **clé métier** unique d'une entité, plus sa date et sa source de chargement | `hub_client(client_hk, client_id, load_date, source)` |
| **Link** | Une **relation** entre hubs | `link_vente(vente_hk, client_hk, produit_hk, ...)` |
| **Satellite** | Les **attributs descriptifs**, versionnés par date de chargement (avec un hachage des attributs pour détecter les changements) | `sat_client(client_hk, load_date, nom, pays, segment, hashdiff)` |

| | Étoile | Data Vault |
|---|---|---|
| Conçu pour | La **consommation** (requêtes simples) | L'**intégration** de sources multiples, l'audit |
| Historique | SCD choisi par dimension | **Natif** (satellites) |
| Requêtes | Simples, peu de jointures | Beaucoup de jointures : on construit une étoile **par-dessus** |

En pratique, on peut avoir un Data Vault en couche d'intégration et des **étoiles** (ou des tables larges) en couche de reporting. Ne l'adopte que si le nombre de sources et les exigences d'audit le justifient.

### 9.3 L'architecture en médaillon

Une organisation courante en lac de données :

| Couche | Rôle | Contenu dans cette série |
|---|---|---|
| **Bronze** | Données **brutes**, telles que reçues | JSON bruts, `clients_historique`, `evenements_json` (Blog 4) |
| **Silver** | Données **nettoyées**, dédupliquées, typées, historisées | `ventes_dedup`, `clients_dedup` (Blog 2), **dimensions SCD2** (ce blog) |
| **Gold** | Données **prêtes à l'usage** : étoile, agrégats | `fait_ventes`, `dim_*`, tables de reporting |

La règle : **chaque couche a un contrat clair**, et les erreurs se corrigent **à la bonne couche** (un doublon se règle en silver, pas dans vingt rapports gold).

---

## 10. Les pièges de modélisation

| # | Piège | Conséquence | Remède |
|---|---|---|---|
| 1 | **Mélanger les grains** (par exemple, répéter le total de la commande sur chaque ligne) | Double comptage : Alice a 2 lignes le 1er mars ; si chacune porte un « total commande » de 1 225, la somme donne **2 450** au lieu de 1 225 | Un grain par table ; les totaux dans une table de grain « commande » |
| 2 | **Sommer une mesure non additive** (prix unitaire, ratio) | Chiffres absurdes | Stocker les composants, calculer le ratio à la lecture |
| 3 | **Sommer une mesure semi-additive dans le temps** (stock) | Un stock « additionné » sur 30 jours n'a aucun sens | Prendre le dernier état, ou la moyenne, par période |
| 4 | **Relation plusieurs-à-plusieurs** (un panier contient plusieurs produits, Blog 4) | Jointure qui multiplie les lignes | Fait au grain « ligne de panier », ou **table de pont** (*bridge*) |
| 5 | **Dimension qui arrive après le fait** (arrivée tardive) | Fait rattaché au membre « inconnu » | Ré-résoudre les clés quand la dimension arrive (retraitement) |
| 6 | **Fait qui arrive en retard** (SCD2) | Mauvaise version si on utilise la date de **chargement** | Résoudre avec la date de l'**événement**, jamais celle du chargement |
| 7 | **Dimension à rôles multiples** (date de vente et date de livraison) | Une seule dimension de dates utilisée deux fois | Joindre `dim_date` **deux fois** avec deux alias |
| 8 | **Beaucoup d'indicateurs à faible cardinalité** dans le fait | Table de faits large et peu lisible | Une **dimension « fourre-tout »** (*junk dimension*) |
| 9 | **NULL dans les attributs de dimension** | Groupes `NULL` ambigus dans les rapports | Remplacer par `'INCONNU'`, **de façon cohérente** des deux côtés d'une comparaison |
| 10 | **SCD2 partout** | Dimension qui grossit, complexité de maintenance | Historiser **uniquement** les attributs dont l'histoire a une valeur métier ; un attribut qui change très souvent va dans une **mini-dimension** |
| 11 | **Clé « inconnu » qui concentre trop de lignes** | Clé chaude (Blog 6) | Surveiller sa part (7.5), corriger la source |

---

## 11. Résolution de l'incident

La question de départ : *« Gold 1 305 ou Gold 80 ? »* Les deux sont justes. La bonne réponse de data engineer n'est pas de **choisir**, c'est de **rendre la définition explicite** et d'exposer **les deux vues**, avec leur lien.

### 11.1 La matrice de migration

On joint chaque vente à **la version valable à sa date** (segment « à la date ») **et** à la **version courante** (segment « actuel ») :

```sql
SELECT
  COALESCE(h.segment, 'INCONNU')   AS segment_a_la_date,
  COALESCE(k.segment, 'INCONNU')   AS segment_actuel,
  COUNT(*)                         AS nb,
  SUM(v.montant)                   AS ca
FROM ventes_dedup v
JOIN dim_client_scd2 h
  ON  v.client_id  = h.client_id
  AND v.date_vente >= h.date_debut
  AND v.date_vente <  h.date_fin
JOIN dim_client_scd2 k
  ON  v.client_id = k.client_id
  AND k.est_courant = 1
WHERE v.statut = 'VALIDEE'
GROUP BY COALESCE(h.segment, 'INCONNU'), COALESCE(k.segment, 'INCONNU')
ORDER BY segment_a_la_date, segment_actuel;
```

| segment_a_la_date | segment_actuel | nb | ca |
|---|---|---|---|
| Gold | Gold | 1 | 80.00 |
| INCONNU | Standard | 1 | 1100.00 |
| **Premium** | **Gold** | **2** | **1225.00** |
| Premium | Premium | 3 | 300.00 |
| Standard | Standard | 2 | 325.00 |

**Total : 3 030**, identique dans les deux vues. Cette table **explique tout** :

- Les **1 225** d'Alice en mars : Premium à l'époque, Gold aujourd'hui. C'est la différence entre « Gold 1 305 » (vue actuelle : 80 + 1 225) et « Gold 80 » (vue à la date).
- Les **1 100** de David : segment inconnu à l'époque (il n'était pas renseigné), Standard aujourd'hui.
- Les autres lignes ne changent pas.

### 11.2 Ce qu'on livre

1. **Deux colonnes explicites** dans la couche de reporting : `segment_a_la_date` (type 2) et `segment_actuel` (version courante). Plus de rapport qui change sans que personne sache pourquoi.
2. **Une dimension SCD2** `dim_client_scd2` (clés de substitution par hachage, intervalle `[début, fin[`), maintenue par la requête ensembliste de la section 6 (rejouable, comparaison null-safe).
3. **Les cinq contrôles** de la section 7 dans le pipeline.
4. **Un contrat écrit** : « *Le CA par segment à la date de la vente utilise la version du client valable à la date de la vente ; le CA par segment actuel utilise la version courante.* » (Blog 9 : data contract.)

> Le changement de segment d'Alice **n'a pas modifié le passé** : il a simplement ajouté une version. C'est tout l'intérêt du type 2.

---

## 12. 10 questions d'entretien

**1. Qu'est-ce que le grain d'une table de faits, et pourquoi le définir en premier ?**
Ce que représente **une ligne** (par exemple, une ligne de vente). Il détermine ce qu'on peut calculer et évite le double comptage : mélanger des grains dans une table est l'erreur la plus coûteuse.

**2. Différence entre une table de faits et une dimension ?**
Le fait contient les **événements mesurables** (mesures numériques et clés vers les dimensions), très volumineux. La dimension contient le **contexte descriptif** (qui, quoi, où, quand), plus petite et lentement changeante.

**3. Mesure additive, semi-additive, non additive ?**
Additive : sommable partout (`montant`). Semi-additive : sommable sauf sur le temps (stock). Non additive : jamais directement (prix, ratio) : on stocke les composants et on calcule à la lecture.

**4. Étoile ou flocon ?**
L'étoile dénormalise les dimensions (peu de jointures, requêtes simples). Le flocon les normalise (moins de redondance, plus de jointures). Dans un entrepôt colonnaire, l'étoile est le choix courant.

**5. Pourquoi une clé de substitution plutôt que la clé métier ?**
La clé métier n'est pas unique dans le temps (versions), appartient à la source et n'existe pas pour les cas spéciaux. La clé de substitution identifie une **version** de dimension, propre à l'entrepôt. Un hachage déterministe la rend rejouable.

**6. Explique les SCD de types 1, 2 et 3.**
Type 1 : on écrase (pas d'historique). Type 2 : une nouvelle ligne par version (début, fin, courant). Type 3 : une colonne « valeur précédente » (un seul changement conservé).

**7. Comment retrouves-tu le segment d'un client à la date d'une vente ?**
Jointure sur la clé métier **et** sur la plage : `date_vente >= date_debut AND date_vente < date_fin`. Mieux : résoudre la version **au chargement** et stocker la clé de substitution dans le fait.

**8. Pourquoi la détection de changement doit-elle être null-safe ?**
Parce que `NULL <> 'Standard'` vaut inconnu : un changement de `NULL` vers une valeur n'est jamais détecté avec `<>`. On utilise `<=>` ou `IS NOT DISTINCT FROM`.

**9. Que se passe-t-il si deux versions d'une dimension SCD2 se chevauchent ?**
Une ligne de fait trouve **deux** versions : elle est comptée deux fois (CA gonflé), sans erreur. D'où le contrôle « la fin d'une version égale le début de la suivante ».

**10. Étoile ou Data Vault ?**
L'étoile est conçue pour la consommation (simple, rapide). Le Data Vault pour l'intégration de nombreuses sources et l'audit (hubs, links, satellites, historique natif) mais plus de jointures. On les combine souvent : Data Vault en intégration, étoiles en reporting.

---

## 13. 5 exercices

### Exercice 1 : le grain et les colonnes
Une table de faits a pour grain « une ligne de vente ». Classe ces colonnes : `vente_id`, `date_vente`, `client_sk`, `prix_unitaire`, `quantite`, `montant`, `taux_remise`, `stock_restant`.

<details>
<summary>Solution</summary>

- `vente_id` : **dimension dégénérée** (identifiant sans dimension).
- `date_vente`, `client_sk` : **clés** vers `dim_date` et `dim_client`.
- `quantite`, `montant` : **mesures additives**.
- `prix_unitaire`, `taux_remise` : **non additives** (on ne les somme pas ; on stocke `montant` et `quantite`, et on calcule `montant / quantite` à la lecture).
- `stock_restant` : **semi-additif**, et surtout un autre **grain** (état du stock, pas une vente) : à sortir dans un **snapshot périodique** de stock.
</details>

### Exercice 2 : une nouvelle version
Bruno (client 2) passe de `Standard` à `Premium` le **2024-04-01**. Quelles lignes la dimension contient-elle pour lui ? Quel segment est retenu pour une vente du 2024-04-01 ? Et pour sa vente 109 du 15 mars ?

<details>
<summary>Solution</summary>

| client_id | segment | date_debut | date_fin | est_courant |
|---|---|---|---|---|
| 2 | Standard | 2023-03-02 | **2024-04-01** | 0 |
| 2 | Premium | **2024-04-01** | 9999-12-31 | 1 |

Une vente du 2024-04-01 relève de **Premium** (intervalle semi-ouvert : la fin de la première version est **exclue**, le début de la seconde **inclus**). La vente 109 du 15 mars reste **Standard** : le passé n'est pas réécrit.
</details>

### Exercice 3 : le changement invisible
Pourquoi le changement de David (NULL vers Standard) n'est-il pas détecté avec `WHERE s.segment <> d.segment` ? Que contient la dimension, et quelle correction ?

<details>
<summary>Solution</summary>

`NULL <> 'Standard'` vaut **inconnu**, donc la ligne est écartée du `WHERE` : David n'est jamais classé « modifié ». Sa version courante reste `segment = NULL`, ouverte jusqu'en `9999-12-31`, alors qu'Alice est correctement historisée. Correction : comparaison **null-safe** (`NOT (s.segment <=> d.segment)`, ou `IS DISTINCT FROM`), appliquée à **tous** les attributs suivis. Et si la dimension remplace les NULL par `'INCONNU'`, normaliser **aussi** l'instantané avant de comparer.
</details>

### Exercice 4 : le chevauchement
La version Premium d'Alice se termine par erreur le 2024-04-05 (au lieu du 2024-03-15). Quel est l'effet sur le CA validé par segment à la date de la vente, et comment détecter l'erreur ?

<details>
<summary>Solution</summary>

La vente 201 (2024-04-02, 80) tombe dans **deux** versions (Premium et Gold) : elle est comptée deux fois. Le CA validé passe de **3 030** à **3 110** (10 lignes jointes au lieu de 9). Détection : la requête de la section 7.2 : `date_fin` (2024-04-05) est **supérieure** au `prochain_debut` (2024-03-15). Le contrôle « nombre de lignes avant / après jointure » (7.4) la détecte aussi.
</details>

### Exercice 5 : le calendrier
Combien de jours du 1er mars au 8 avril 2024 n'ont aucune vente validée ? Que vaut la somme sur 7 jours au 15 mars sur un calendrier complet ? Pourquoi ne faut-il pas filtrer sur `c.jour IS NOT NULL` avant la window ?

<details>
<summary>Solution</summary>

39 jours dans la période, 10 avec ventes : **29 jours sans vente**. La somme sur 7 jours au 15 mars vaut **1 575** (jours du 9 au 15 : `0 + 1100 + 25 + 150 + 300`). Filtrer avant la window supprime les jours vides **avant** son calcul : elle porte alors sur 7 lignes et non 7 jours, et donne par exemple **2 950** au lieu de 1 575 (ordre d'exécution du Blog 1 : `WHERE` avant les windows). On filtre **à l'extérieur**.
</details>

---

## 14. Checklist avant de livrer un modèle

- [ ] Le **grain** de chaque table de faits est écrit en une phrase
- [ ] Chaque mesure est classée **additive / semi-additive / non additive** ; les ratios sont calculés à la lecture
- [ ] Les faits pointent vers des **clés de substitution** (hachage déterministe, avec séparateur)
- [ ] Chaque dimension a un membre **« inconnu »**, et les NULL d'attributs sont remplacés de façon cohérente
- [ ] Les attributs historisés en **type 2** sont choisis pour leur valeur métier
- [ ] Les intervalles sont **semi-ouverts** `[début, fin[`, sans trou ni chevauchement
- [ ] La version est résolue avec la **date de l'événement**, jamais celle du chargement
- [ ] La détection de changement est **null-safe** et normalisée des deux côtés
- [ ] La mise à jour de la dimension est **rejouable** (même instantané, même résultat) avec la date en **paramètre**
- [ ] Les contrôles sont automatisés : **une version courante**, **pas de chevauchement**, **comptes avant / après**, **part d'inconnu**, **réconciliation avec la source**
- [ ] Une dimension **calendrier** existe, et les filtres sont placés **hors** des windows
- [ ] La **définition** de chaque indicateur (« actuel » ou « à la date ») est écrite dans un contrat

---

## 15. Prochain blog et blogs restants

**Blog 8 : chargements et idempotence.** Comment alimenter ce modèle **chaque jour** sans doublons ni trous : chargement **incrémental**, `INSERT INTO` contre `INSERT OVERWRITE`, `MERGE` et **tables transactionnelles (ACID)**, **CDC** (capture des changements), **backfill** (rechargement de l'historique) et surtout **rejouabilité** : un job relancé après une panne doit donner le même résultat. On y retrouve la requête SCD2 de la section 6 sous forme de `MERGE`.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions |
| ✅ 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE`, pivot |
| ✅ 5 | Performance I : lire un plan | `EXPLAIN`, formats, partitionnement, bucketing, statistiques, petits fichiers |
| ✅ 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| ✅ 7 | Modélisation | Star schema, SCD 1 / 2 / 3, clés de substitution, calendrier, Data Vault |
| 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 8

Sans regarder la suite : (1) ton job de chargement quotidien plante à 80 % et il est **relancé** : comment garantir qu'il ne **duplique pas** les lignes déjà chargées ? (2) Quelle différence entre `INSERT INTO` et `INSERT OVERWRITE` sur une partition, et laquelle est **rejouable** ? (3) Comment ne charger que les lignes **modifiées depuis hier** ? Si tu hésites, le Blog 8 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de reconstruire la dimension d'un de tes propres référentiels, de provoquer chaque piège (changement NULL, chevauchement) et de vérifier que tes contrôles les détectent.*