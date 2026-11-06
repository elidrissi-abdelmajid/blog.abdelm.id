---
title: "SQL pour Data Engineers, Blog 4 : types complexes, JSON et agrégations avancées"
description: "Manipuler les types complexes, aplatir du JSON et produire des agrégations avancées en SQL."
weight: 4
---

# SQL pour Data Engineers, Blog 4 : types complexes, JSON et agrégations avancées

> **Série** : SQL, de basique à tech lead · **Niveau** : 3-4/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Sur ce sujet, **les dialectes divergent beaucoup** : les écarts sont signalés à chaque fois.
> **Prérequis** : [Blog 1](blog-01-sql-bases.md), [Blog 2](blog-02-sql-intermediaire.md), [Blog 3](blog-03-sql-window-functions.md). On réutilise `ventes_dedup` et `clients_dedup`.

---

## 0. Pourquoi ce blog existe

Dans un data lake, les données ne sont presque jamais « plates » :

- une commande contient **une liste** de produits (`ARRAY`) ;
- un client porte des attributs **clé-valeur** variables (`MAP`) ;
- une adresse est un objet **imbriqué** (`STRUCT`) ;
- une application envoie des événements en **JSON** brut dans une colonne texte.

Un data engineer passe une grande partie de son temps à **aplatir** ces structures, et à produire des rapports avec **sous-totaux** et **tableaux croisés**. C'est aussi un classique d'entretien : « comment tu mets à plat un tableau ? », « que fait `explode` d'un tableau vide ? », « comment tu calcules les sous-totaux en une requête ? ».

Le scénario :

> **Vendredi, 9h05.** Trois demandes arrivent en même temps :
> 1. **Marketing** : *« Combien de produits par pays de livraison ? Votre rapport donne 0 pour l'Italie et ne parle pas du panier 6. »*
> 2. **Produit** : *« Les événements applicatifs arrivent en JSON. Mettez-les à plat, et dites-nous combien sont illisibles. »*
> 3. **Finance** : *« Le CA par mois et catégorie, avec les sous-totaux et le total général, dans un seul rapport. En colonnes par mois si possible. »*

À la fin du blog, tu sauras traiter les trois, et expliquer chaque piège.

### Plan

1. [Les données de ce blog](#1-les-données-de-ce-blog)
2. [ARRAY, MAP, STRUCT : accéder et ne pas se tromper](#2-array-map-struct--accéder-et-ne-pas-se-tromper)
3. [Aplatir avec `LATERAL VIEW EXPLODE`](#3-aplatir-avec-lateral-view-explode)
4. [Regrouper avec `collect_list` et `collect_set`](#4-regrouper-avec-collect_list-et-collect_set)
5. [Filtrer à l'intérieur des collections](#5-filtrer-à-linterieur-des-collections)
6. [Lire du JSON](#6-lire-du-json)
7. [`GROUPING SETS`, `ROLLUP`, `CUBE`](#7-grouping-sets-rollup-cube)
8. [Pivot et unpivot](#8-pivot-et-unpivot)
9. [Performance et choix de modélisation](#9-performance-et-choix-de-modélisation)
10. [Résolution des trois demandes](#10-résolution-des-trois-demandes)
11. [10 questions d'entretien](#11-10-questions-dentretien)
12. [5 exercices](#12-5-exercices)
13. [Checklist avant de livrer une requête](#13-checklist-avant-de-livrer-une-requête)
14. [Prochain blog et blogs restants](#14-prochain-blog-et-blogs-restants)

---

## 1. Les données de ce blog

### 1.1 `paniers` : un `ARRAY`, un `MAP` et un `STRUCT`

```sql
USE blog_sql;

CREATE TABLE paniers (
  panier_id   INT,
  client_id   INT,
  date_panier DATE,
  produits    ARRAY<STRING>,
  attributs   MAP<STRING, STRING>,
  livraison   STRUCT<ville:STRING, pays:STRING>
) STORED AS PARQUET;

-- INSERT ... VALUES n'accepte pas les types complexes : on passe par INSERT ... SELECT
INSERT INTO paniers SELECT 1, 1, DATE '2024-03-01',
  array('Laptop', 'Souris'),
  map('canal', 'web', 'promo', 'PRINTEMPS'),
  named_struct('ville', 'Paris', 'pays', 'FR');

INSERT INTO paniers SELECT 2, 2, DATE '2024-03-05',
  array('Clavier'),
  map('canal', 'mobile'),
  named_struct('ville', 'Dakar', 'pays', 'SN');

INSERT INTO paniers SELECT 3, 3, DATE '2024-03-07',
  array('Casque', 'Enceinte'),
  map('canal', 'web'),
  named_struct('ville', 'Casablanca', 'pays', 'MA');

INSERT INTO paniers SELECT 4, 4, DATE '2024-03-10',
  array('Laptop'),
  map('canal', 'web', 'promo', 'PRINTEMPS'),
  named_struct('ville', 'Lyon', 'pays', 'FR');

-- Panier VIDE : le tableau existe mais ne contient rien
INSERT INTO paniers SELECT 5, 5, DATE '2024-04-08',
  CAST(array() AS ARRAY<STRING>),
  map('canal', 'mobile'),
  named_struct('ville', 'Milan', 'pays', 'IT');

-- Panier INCONNU : toutes les colonnes complexes sont NULL
INSERT INTO paniers SELECT 6, 6, DATE '2024-04-09',
  CAST(NULL AS ARRAY<STRING>),
  CAST(NULL AS MAP<STRING, STRING>),
  CAST(NULL AS STRUCT<ville:STRING, pays:STRING>);
```

Les deux derniers paniers sont **volontairement piégeux** : le panier 5 a un tableau **vide**, le panier 6 a un tableau **NULL**. Ce sont deux états différents, qui se comportent différemment dans presque toutes les fonctions.

### 1.2 `evenements_json` : des événements bruts

```sql
CREATE TABLE evenements_json (
  id      INT,
  payload STRING
) STORED AS PARQUET;

INSERT INTO evenements_json VALUES
  (1, '{"client_id":1,"type":"achat","montant":1200.0,"produits":["Laptop","Souris"],"appareil":{"os":"android","version":"14"}}'),
  (2, '{"client_id":2,"type":"vue_page","appareil":{"os":"ios","version":"17"}}'),
  (3, '{"client_id":3,"type":"achat","montant":150.0,"produits":["Casque"],"appareil":{"os":"android"}}'),
  (4, 'ERREUR_CAPTEUR');   -- JSON invalide
```

### 1.3 `ca_large` : un tableau déjà « en colonnes » (pour l'unpivot)

```sql
CREATE TABLE ca_large (
  categorie  STRING,
  ca_2024_03 DECIMAL(10,2),
  ca_2024_04 DECIMAL(10,2),
  ca_2024_05 DECIMAL(10,2)
) STORED AS PARQUET;

INSERT INTO ca_large VALUES
  ('Audio',        300.00, 150.00, NULL),
  ('Informatique', 2650.00, 105.00, 0.00);
```

> **Dialecte** : si le littéral `DATE '...'` est refusé, utilise `CAST('...' AS DATE)`. Sur Impala, les types complexes ne sont lisibles qu'en Parquet (et selon la version en ORC) : vérifie ta version.

---

## 2. ARRAY, MAP, STRUCT : accéder et ne pas se tromper

### 2.1 Les trois types

| Type | Idée | Accès | Exemple |
|---|---|---|---|
| `ARRAY<T>` | Liste **ordonnée** d'éléments du même type | `col[0]` (index à partir de 0) | `produits[0]` |
| `MAP<K,V>` | Dictionnaire clé → valeur | `col['cle']` | `attributs['canal']` |
| `STRUCT<a:T1, b:T2>` | Objet à **champs nommés** (types variés) | `col.champ` | `livraison.pays` |

### 2.2 Accéder aux éléments

```sql
SELECT
  panier_id,
  livraison.ville        AS ville,
  attributs['canal']     AS canal,
  produits[0]            AS premier_produit,
  size(produits)         AS nb_produits
FROM paniers
ORDER BY panier_id;
```

| panier_id | ville | canal | premier_produit | nb_produits |
|---|---|---|---|---|
| 1 | Paris | web | Laptop | 2 |
| 2 | Dakar | mobile | Clavier | 1 |
| 3 | Casablanca | web | Casque | 2 |
| 4 | Lyon | web | Laptop | 1 |
| 5 | Milan | mobile | NULL | 0 |
| 6 | NULL | NULL | NULL | **-1** |

### 2.3 Les pièges de la lecture

**Piège 1 : `size(NULL)` vaut -1** (Hive, et Spark par défaut dans ses versions courantes), alors qu'un tableau vide donne 0. Un `WHERE size(produits) > 0` écarte donc bien le panier 6, mais un `SUM(size(produits))` **soustrait 1** pour chaque tableau NULL. Protège avec `COALESCE(size(produits), 0)` ou teste `produits IS NULL` à part. (Avec le mode ANSI de Spark 4, le comportement évolue : vérifie sur ta version.)

**Piège 2 : l'index hors limites.** `produits[0]` sur le panier 5 (tableau vide) renvoie NULL sur Hive et sur Spark classique. **Avec le mode ANSI de Spark activé, c'est une erreur.** Pour un accès sûr : `element_at(produits, 1)` (index à partir de **1** !) ou `try_element_at` selon la version.

**Piège 3 : l'index commence à 0 avec `[ ]` mais à 1 avec `element_at`.** Une source classique de décalage silencieux.

**Piège 4 : trois états à ne jamais confondre.**

| État | Exemple | `produits IS NULL` | `size(produits)` |
|---|---|---|---|
| Rempli | `['Laptop', 'Souris']` | faux | 2 |
| **Vide** | `[]` (panier 5) | faux | 0 |
| **NULL** | (panier 6) | vrai | -1 |

Pour le métier, « panier vide » (le client n'a rien mis) et « panier inconnu » (la donnée est absente) sont deux informations différentes. Ne les fusionne pas sans décision explicite.

**Piège 5 : `struct(a, b)` sans noms.** Le résultat a des champs auto-nommés (`col1`, `col2`) : tu perds la lisibilité. Utilise toujours `named_struct('ville', ..., 'pays', ...)`.

**Piège 6 : un `STRUCT` NULL donne des champs NULL, sans erreur.** Pour le panier 6, `livraison.pays` vaut NULL : c'est correct, mais il faut s'en souvenir dans les `GROUP BY` (un groupe « NULL » apparaît).

### 2.4 Quelques fonctions utiles

| Besoin | Fonction | Exemple |
|---|---|---|
| Taille d'un tableau / map | `size(x)` | voir piège 1 |
| Tableau contient une valeur | `array_contains(arr, val)` | `array_contains(produits, 'Laptop')` |
| Trier un tableau | `sort_array(arr)` | `sort_array(array('b','a'))` → `['a','b']` |
| Tableau → texte | `concat_ws(sep, arr)` | `concat_ws(',', array('Laptop','Souris'))` → `'Laptop,Souris'` |
| Clés / valeurs d'un map | `map_keys(m)`, `map_values(m)` | l'ordre des clés n'est pas garanti |
| Créer un objet nommé | `named_struct('a', 1, 'b', 2)` | |
| Éclater un struct en colonnes | `livraison.*` | colonnes `ville`, `pays` |

---

## 3. Aplatir avec `LATERAL VIEW EXPLODE`

`explode` transforme **un tableau en plusieurs lignes** : une ligne par élément. Avec Hive et Spark, il s'utilise avec `LATERAL VIEW`, qui « colle » le résultat à la ligne d'origine.

### 3.1 Le cas de base

```sql
SELECT p.panier_id, p.client_id, produit
FROM paniers p
LATERAL VIEW explode(p.produits) t AS produit
ORDER BY p.panier_id, produit;
```

| panier_id | client_id | produit |
|---|---|---|
| 1 | 1 | Laptop |
| 1 | 1 | Souris |
| 2 | 2 | Clavier |
| 3 | 3 | Casque |
| 3 | 3 | Enceinte |
| 4 | 4 | Laptop |

**6 lignes**. Les paniers 5 (vide) et 6 (NULL) ont **disparu**, sans erreur : `explode` ne produit **aucune ligne** pour un tableau vide ou NULL. C'est exactement ce qui est arrivé au rapport marketing de notre scénario : l'Italie (panier 5) n'apparaît plus du tout.

### 3.2 `LATERAL VIEW OUTER` : conserver les paniers vides

```sql
SELECT p.panier_id, p.client_id, produit
FROM paniers p
LATERAL VIEW OUTER explode(p.produits) t AS produit
ORDER BY p.panier_id, produit;
```

→ **8 lignes** : les 6 précédentes, plus `(5, 5, NULL)` et `(6, 6, NULL)`. `OUTER` joue le même rôle qu'un `LEFT JOIN` (Blog 1) : la ligne d'origine est conservée avec un NULL quand il n'y a rien à éclater.

| | `LATERAL VIEW explode` | `LATERAL VIEW OUTER explode` |
|---|---|---|
| Tableau rempli | une ligne par élément | une ligne par élément |
| Tableau **vide** | **aucune** ligne | une ligne, élément NULL |
| Tableau **NULL** | **aucune** ligne | une ligne, élément NULL |
| Analogue | `INNER JOIN` | `LEFT JOIN` |

> **Réflexe** : si tu ne veux **perdre aucune ligne d'origine** (un rapport par pays, par client...), c'est `OUTER`. Sinon, tu as créé un `INNER JOIN` sans le savoir (Blog 1).

### 3.3 `posexplode` : récupérer la position

```sql
SELECT p.panier_id, pos, produit
FROM paniers p
LATERAL VIEW posexplode(p.produits) t AS pos, produit
ORDER BY p.panier_id, pos;
```

| panier_id | pos | produit |
|---|---|---|
| 1 | 0 | Laptop |
| 1 | 1 | Souris |
| 2 | 0 | Clavier |
| 3 | 0 | Casque |
| 3 | 1 | Enceinte |
| 4 | 0 | Laptop |

La position (à partir de 0) est indispensable quand l'**ordre compte** (par exemple, l'ordre des clics dans un parcours).

### 3.4 Éclater un `MAP`

`explode` sur un map donne **deux colonnes** : clé et valeur.

```sql
SELECT p.panier_id, cle, valeur
FROM paniers p
LATERAL VIEW explode(p.attributs) a AS cle, valeur
ORDER BY p.panier_id, cle;
```

7 lignes (panier 1 : `canal`, `promo` ; panier 2 : `canal` ; panier 3 : `canal` ; panier 4 : `canal`, `promo` ; panier 5 : `canal` ; le panier 6, map NULL, disparaît). Comptage par clé : `canal` → 5, `promo` → 2.

### 3.5 Le piège du double `explode` : le produit cartésien

Éclater **deux** collections de la même ligne les **multiplie** :

```sql
SELECT COUNT(*) AS nb_lignes
FROM paniers p
LATERAL VIEW explode(p.produits)  t1 AS produit
LATERAL VIEW explode(p.attributs) t2 AS cle, valeur;
```

→ **9 lignes** : panier 1 → 2 produits × 2 attributs = 4, panier 2 → 1, panier 3 → 2, panier 4 → 2. Les chiffres explosent sans erreur. Si tu veux associer **élément par élément** deux tableaux parallèles, utilise `posexplode` sur l'un puis accède à l'autre par position, ou `arrays_zip` (Spark), plutôt que deux `explode`.

### 3.6 Compter après un `explode`

Après un `explode`, **une ligne = un produit**, plus un panier. Pour compter des paniers, il faut `COUNT(DISTINCT panier_id)` (Blog 1, section 6).

```sql
SELECT
  p.livraison.pays         AS pays,
  COUNT(produit)           AS nb_produits,
  COUNT(*)                 AS nb_lignes
FROM paniers p
LATERAL VIEW OUTER explode(p.produits) t AS produit
GROUP BY p.livraison.pays
ORDER BY pays NULLS LAST;
```

| pays | nb_produits | nb_lignes |
|---|---|---|
| FR | 3 | 3 |
| IT | **0** | **1** |
| MA | 2 | 2 |
| SN | 1 | 1 |
| NULL | **0** | **1** |

Avec `OUTER`, le panier italien produit **une ligne** (`nb_lignes = 1`) mais **zéro produit** (`COUNT(produit) = 0`, car `COUNT(col)` ignore les NULL, Blog 1). C'est la différence entre `COUNT(*)` et `COUNT(colonne)`, que tu retrouves partout.

### 3.7 Et sur Impala ?

Impala **n'utilise pas** `LATERAL VIEW`. Pour une colonne de type collection, la syntaxe est une jointure sur la colonne elle-même (par exemple `FROM paniers p, p.produits t`, avec la pseudo-colonne `item` pour les éléments d'un `ARRAY`). Elle est plus restrictive (les types complexes ne peuvent pas apparaître directement dans le `SELECT` de la requête principale). Vérifie la documentation de ta version avant de réutiliser ces exemples.

---

## 4. Regrouper avec `collect_list` et `collect_set`

L'opération inverse : **réunir** plusieurs lignes en un tableau.

```sql
SELECT
  client_id,
  size(collect_set(produit))           AS nb_produits_distincts,
  sort_array(collect_set(produit))     AS produits_distincts,
  sort_array(collect_list(produit))    AS produits_achetes
FROM ventes_dedup
WHERE statut = 'VALIDEE' AND client_id IS NOT NULL
GROUP BY client_id
ORDER BY client_id;
```

| client_id | nb_produits_distincts | produits_distincts | produits_achetes |
|---|---|---|---|
| 1 | 3 | [Clavier, Laptop, Souris] | [Clavier, Laptop, Souris] |
| 2 | 1 | [Ecran] | [Ecran] |
| 3 | 2 | [Casque, Enceinte] | [Casque, Casque, Enceinte] |
| 4 | 1 | [Laptop] | [Laptop] |
| 5 | 1 | [Souris] | [Souris] |
| 99 | 1 | [Casque] | [Casque] |

`collect_list` **garde les doublons** (Chloé a acheté deux fois un casque), `collect_set` les supprime.

### Les pièges

1. **L'ordre n'est pas garanti.** Les données sont réparties sur des machines : l'ordre des éléments peut changer d'une exécution à l'autre. On passe par `sort_array(...)`, ou on trie d'abord avec une sous-requête (ce qui n'est lui-même pas toujours respecté par `collect_list` selon le moteur).
2. **Les NULL sont ignorés** : une valeur NULL n'entre pas dans le tableau (comme pour `SUM` ou `COUNT(col)`, Blog 1).
3. **La mémoire** : un tableau est construit **dans une seule tâche** par groupe. Un client avec des millions de lignes produit un tableau géant qui peut faire échouer le job (*out of memory*) : c'est un danger de *skew* (Blog 6).
4. **Pas de `COUNT(DISTINCT)` à la place** : si tu veux juste compter les valeurs distinctes, `COUNT(DISTINCT produit)` est plus léger que `size(collect_set(produit))`.

---

## 5. Filtrer à l'intérieur des collections

### 5.1 `array_contains`

```sql
SELECT panier_id, attributs['canal'] AS canal
FROM paniers
WHERE array_contains(produits, 'Laptop');
```

→ les paniers **1 et 4**, tous deux sur le canal `web`.

| panier_id | `array_contains(produits, 'Laptop')` |
|---|---|
| 1 | vrai |
| 2, 3 | faux |
| 4 | vrai |
| 5 (vide) | faux |
| 6 (NULL) | **NULL** (donc écarté par le `WHERE`) |

C'est la logique à trois valeurs du Blog 1 : sur un tableau NULL, la réponse est « inconnu », pas « faux ». Un `WHERE NOT array_contains(...)` **n'inclurait pas** le panier 6.

### 5.2 Les fonctions d'ordre supérieur (Spark)

Spark SQL permet de passer une **expression** à une fonction qui parcourt le tableau, sans `explode` :

```sql
SELECT
  panier_id,
  transform(produits, x -> upper(x))         AS produits_maj,
  filter(produits, x -> x <> 'Souris')       AS sans_souris,
  exists(produits, x -> x = 'Laptop')        AS a_un_laptop
FROM paniers
WHERE panier_id = 1;
```

| panier_id | produits_maj | sans_souris | a_un_laptop |
|---|---|---|---|
| 1 | [LAPTOP, SOURIS] | [Laptop] | vrai |

Avantage : **pas d'`explode` ni de regroupement**, donc pas de shuffle supplémentaire. Limite : disponible dans Spark (≥ 2.4) mais pas dans Hive classique. Quand tu cibles un job Hive, reste sur `explode`.

---

## 6. Lire du JSON

### 6.1 `get_json_object` : simple, mais coûteux

```sql
SELECT
  id,
  get_json_object(payload, '$.client_id')        AS client_id,
  get_json_object(payload, '$.type')             AS type_evt,
  get_json_object(payload, '$.montant')          AS montant,
  get_json_object(payload, '$.produits[0]')      AS premier_produit,
  get_json_object(payload, '$.appareil.os')      AS os
FROM evenements_json
ORDER BY id;
```

| id | client_id | type_evt | montant | premier_produit | os |
|---|---|---|---|---|---|
| 1 | 1 | achat | 1200.0 | Laptop | android |
| 2 | 2 | vue_page | NULL | NULL | ios |
| 3 | 3 | achat | 150.0 | Casque | android |
| 4 | **NULL** | **NULL** | NULL | NULL | **NULL** |

Trois choses à retenir :

1. **Tout est renvoyé en `STRING`.** Il faut caster : `CAST(get_json_object(payload, '$.montant') AS DECIMAL(10,2))`. (Et pour de l'argent, **jamais** `DOUBLE`, Blog 1.)
2. **Le chemin** s'écrit à la façon JSONPath : `$.champ`, `$.objet.champ`, `$.tableau[0]`.
3. **Un JSON invalide ne génère aucune erreur** : la ligne 4 renvoie simplement NULL partout. C'est le piège central de la section.

### 6.2 Distinguer « JSON invalide » de « clé absente »

Les lignes 2 et 4 ont toutes les deux `montant = NULL`, pour des raisons différentes. Pour faire la distinction, teste le document **entier** :

```sql
SELECT
  id,
  get_json_object(payload, '$') IS NULL                          AS json_invalide,
  get_json_object(payload, '$.montant') IS NULL                  AS montant_absent
FROM evenements_json
ORDER BY id;
```

| id | json_invalide | montant_absent |
|---|---|---|
| 1 | faux | faux |
| 2 | faux | vrai |
| 3 | faux | faux |
| 4 | **vrai** | vrai |

C'est la base d'un contrôle qualité : **« combien de payloads sont illisibles ? »** → `SUM(CASE WHEN get_json_object(payload,'$') IS NULL THEN 1 ELSE 0 END)` = **1** sur nos 4 lignes. Ces lignes ne doivent pas disparaître : elles partent en **quarantaine** (Blog 9).

### 6.3 `json_tuple` : parser une fois, extraire plusieurs champs

`get_json_object` **re-parse le document à chaque appel** : 5 champs, 5 parsings. `json_tuple` parse une fois et renvoie plusieurs champs de premier niveau :

```sql
SELECT e.id, j.client_id, j.type_evt, j.montant
FROM evenements_json e
LATERAL VIEW json_tuple(e.payload, 'client_id', 'type', 'montant') j AS client_id, type_evt, montant;
```

Limite : uniquement les champs **de premier niveau** (pas `appareil.os`). Pour du JSON imbriqué avec Spark, `from_json` est meilleur.

### 6.4 `from_json` (Spark) : un schéma, des types

```sql
WITH analyses AS (
  SELECT
    id,
    from_json(payload,
      'client_id INT, type STRING, montant DOUBLE, produits ARRAY<STRING>,
       appareil STRUCT<os:STRING, version:STRING>') AS j
  FROM evenements_json
)
SELECT
  id,
  j.client_id,
  j.type,
  j.montant,
  j.produits[0]      AS premier_produit,
  j.appareil.os      AS os,
  j IS NULL          AS json_invalide
FROM analyses
ORDER BY id;
```

| id | client_id | type | montant | premier_produit | os | json_invalide |
|---|---|---|---|---|---|---|
| 1 | 1 | achat | 1200.0 | Laptop | android | faux |
| 2 | 2 | vue_page | NULL | NULL | ios | faux |
| 3 | 3 | achat | 150.0 | Casque | android | faux |
| 4 | NULL | NULL | NULL | NULL | NULL | **vrai** |

`from_json` renvoie un **objet typé** que tu manipules avec les notations des sections 2 et 3 (`j.appareil.os`, `explode(j.produits)`...). En mode tolérant (par défaut), un JSON invalide donne un objet NULL ; c'est un `IS NULL` sur l'objet entier qui le détecte.

### 6.5 Bonnes pratiques pour le JSON

1. **Garde toujours le JSON brut** (`payload`) dans la couche d'ingestion, et crée une couche typée à côté. Si le schéma évolue, tu peux relire l'historique.
2. **Parse une seule fois**, à l'ingestion, vers des colonnes typées (`STRUCT`, `ARRAY`, colonnes plates). Ne garde pas du `get_json_object` dans vingt requêtes de reporting.
3. **Ne cache pas les erreurs** : compte les JSON invalides et mets-les en quarantaine.
4. **Un champ qui sert de clé de jointure ou de filtre fréquent doit devenir une vraie colonne** : un `MAP` ou un JSON n'a ni statistiques ni partitionnement efficaces (section 9).

---

## 7. `GROUPING SETS`, `ROLLUP`, `CUBE`

Un `GROUP BY` classique calcule **un seul niveau** d'agrégation. Un rapport de direction en demande plusieurs à la fois : le détail, les sous-totaux, le total général. On pourrait faire trois requêtes reliées par `UNION ALL`, mais chacune relirait la table. Ces trois clauses calculent **tous les niveaux en une seule passe**.

### 7.1 Le détail de départ

Le CA validé par mois et par catégorie (sur `ventes_dedup`) :

```sql
WITH base AS (
  SELECT date_format(date_vente, 'yyyy-MM') AS mois, categorie, montant
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
)
SELECT mois, categorie, SUM(montant) AS ca
FROM base
GROUP BY mois, categorie;
```

| mois | categorie | ca |
|---|---|---|
| 2024-03 | Audio | 300.00 |
| 2024-03 | Informatique | 2650.00 |
| 2024-04 | Audio | 150.00 |
| 2024-04 | Informatique | 105.00 |

### 7.2 `ROLLUP` : détail + sous-totaux hiérarchiques + total

`ROLLUP(mois, categorie)` produit les niveaux : `(mois, categorie)`, puis `(mois)`, puis `()` (le total). Il suit la **hiérarchie** de gauche à droite.

```sql
WITH base AS (
  SELECT date_format(date_vente, 'yyyy-MM') AS mois, categorie, montant
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
),
agrege AS (
  SELECT
    mois, categorie,
    SUM(montant)       AS ca,
    GROUPING(mois)     AS g_mois,
    GROUPING(categorie) AS g_cat
  FROM base
  GROUP BY ROLLUP(mois, categorie)
)
SELECT
  CASE WHEN g_mois = 1 THEN 'TOUS'   ELSE mois      END AS mois_lbl,
  CASE WHEN g_cat  = 1 THEN 'TOUTES' ELSE categorie END AS categorie_lbl,
  ca
FROM agrege
ORDER BY g_mois, mois, g_cat, categorie;
```

| mois_lbl | categorie_lbl | ca |
|---|---|---|
| 2024-03 | Audio | 300.00 |
| 2024-03 | Informatique | 2650.00 |
| 2024-03 | **TOUTES** | **2950.00** |
| 2024-04 | Audio | 150.00 |
| 2024-04 | Informatique | 105.00 |
| 2024-04 | **TOUTES** | **255.00** |
| **TOUS** | **TOUTES** | **3205.00** |

**7 lignes** en une passe. Les sous-totaux **se réconcilient** : `2950` et `255` sont les totaux mensuels du Blog 2 et du Blog 3, `3205` est le CA validé total (le dernier `cumul_global` du Blog 3).

> **Syntaxe selon le moteur** : `GROUP BY mois, categorie WITH ROLLUP` est l'écriture historique de Hive ; `GROUP BY ROLLUP(mois, categorie)` est la forme SQL standard (Spark, et Hive selon la version). Sur Impala, le support de ces clauses dépend de la version : sinon, reviens à un `UNION ALL` de requêtes.

### 7.3 La fonction `GROUPING()` : indispensable

Dans le résultat brut, les lignes de sous-total ont des **NULL** dans les colonnes « remontées ». Problème : **comment distinguer un NULL de sous-total d'un vrai NULL dans les données ?** (par exemple une vente dont la catégorie est inconnue).

`GROUPING(col)` renvoie **1** si la colonne a été « remontée » (la ligne est un sous-total sur cette colonne), **0** sinon. C'est elle qui permet les libellés `TOUS` / `TOUTES` ci-dessus et qui évite de confondre un total avec une donnée manquante.

> Hive propose aussi `GROUPING__ID`, un entier bitmask dont **la définition a changé selon les versions**. Préfère `GROUPING(col)`, dont le sens est stable.

### 7.4 `CUBE` : toutes les combinaisons

`CUBE(mois, categorie)` ajoute les niveaux **non hiérarchiques** : les sous-totaux par catégorie (tous mois confondus).

| mois | categorie | ca | g_mois | g_cat |
|---|---|---|---|---|
| *(les 7 lignes de ROLLUP)* | | | | |
| NULL | Audio | 450.00 | 1 | 0 |
| NULL | Informatique | 2755.00 | 1 | 0 |

**9 lignes** au total. Avec `n` colonnes, `CUBE` calcule `2^n` niveaux : 3 colonnes → 8, 5 colonnes → 32. Sur de gros volumes, Spark **duplique** chaque ligne d'entrée autant de fois que de niveaux : un `CUBE` à 6 colonnes multiplie les données par 64. Ne l'utilise que sur des dimensions de faible cardinalité.

### 7.5 `GROUPING SETS` : choisir exactement les niveaux

```sql
GROUP BY GROUPING SETS ((mois, categorie), (categorie))
```

Calcule **uniquement** le détail et le sous-total par catégorie : 4 + 2 = **6 lignes**, sans sous-total par mois ni total général. `ROLLUP` et `CUBE` ne sont que des raccourcis :

| Écriture | Équivaut à |
|---|---|
| `ROLLUP(a, b)` | `GROUPING SETS ((a, b), (a), ())` |
| `CUBE(a, b)` | `GROUPING SETS ((a, b), (a), (b), ())` |

---

## 8. Pivot et unpivot

### 8.1 Pivot : des lignes vers des colonnes

On veut le CA validé avec **un mois par colonne**.

**Méthode portable : l'agrégation conditionnelle** (Blog 1, section 7.2) :

```sql
WITH base AS (
  SELECT date_format(date_vente, 'yyyy-MM') AS mois, categorie, montant
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
)
SELECT
  categorie,
  SUM(CASE WHEN mois = '2024-03' THEN montant ELSE 0 END) AS ca_2024_03,
  SUM(CASE WHEN mois = '2024-04' THEN montant ELSE 0 END) AS ca_2024_04,
  SUM(montant)                                            AS ca_total
FROM base
GROUP BY categorie
ORDER BY categorie;
```

| categorie | ca_2024_03 | ca_2024_04 | ca_total |
|---|---|---|---|
| Audio | 300.00 | 150.00 | 450.00 |
| Informatique | 2650.00 | 105.00 | 2755.00 |

**Variante Spark : la clause `PIVOT`**

```sql
SELECT *
FROM (
  SELECT categorie, date_format(date_vente, 'yyyy-MM') AS mois, montant
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
)
PIVOT (
  SUM(montant) FOR mois IN ('2024-03' AS mars, '2024-04' AS avril)
);
```

Même résultat, plus court. Mais une combinaison sans donnée vaut **NULL** (et non 0), alors que `SUM(CASE ... ELSE 0)` renvoie 0 : choisis consciemment.

### 8.2 Les limites du pivot

- **Les colonnes sont figées à l'écriture.** Si un mois `2024-05` apparaît, le pivot ne le voit pas, sans erreur ni avertissement. Un pivot « dynamique » exige de **générer la requête** (récupérer la liste des mois, puis construire le SQL), donc une étape de plus dans le pipeline.
- **Le pivot est une opération de présentation.** Stocke les données **en format long** (une ligne par mois et catégorie), et pivote tout à la fin, dans la couche de reporting.
- **Hive et Impala n'ont pas de clause `PIVOT`** : utilise l'agrégation conditionnelle.

### 8.3 Unpivot : des colonnes vers des lignes

L'opération inverse, très fréquente en ingestion de fichiers Excel ou de tables « larges » comme `ca_large`. La fonction `stack(n, libellé1, valeur1, libellé2, valeur2, ...)` produit `n` lignes par ligne d'entrée :

```sql
SELECT l.categorie, u.mois, u.ca
FROM ca_large l
LATERAL VIEW stack(3,
  '2024-03', l.ca_2024_03,
  '2024-04', l.ca_2024_04,
  '2024-05', l.ca_2024_05
) u AS mois, ca
ORDER BY l.categorie, u.mois;
```

| categorie | mois | ca |
|---|---|---|
| Audio | 2024-03 | 300.00 |
| Audio | 2024-04 | 150.00 |
| Audio | 2024-05 | **NULL** |
| Informatique | 2024-03 | 2650.00 |
| Informatique | 2024-04 | 105.00 |
| Informatique | 2024-05 | **0.00** |

`stack` **conserve les NULL**. Ici, `Audio` en mai est NULL (donnée absente) et `Informatique` en mai vaut 0 (aucune vente) : deux informations différentes, qu'il faut préserver ou trancher explicitement. Spark ≥ 3.4 propose aussi une clause `UNPIVOT`, qui **exclut les NULL par défaut** (option pour les inclure) : lis la documentation de ta version.

---

## 9. Performance et choix de modélisation

### 9.1 Les coûts cachés

| Opération | Coût | À surveiller |
|---|---|---|
| `explode` | Multiplie les lignes | Un tableau de 1 000 éléments sur 10 millions de lignes = 10 milliards de lignes. Filtre **avant** d'éclater |
| Double `explode` | Produit cartésien par ligne | Section 3.5 |
| `collect_list` / `collect_set` | Tout le groupe en mémoire dans **une** tâche | *Skew* et risque d'*out of memory* |
| `get_json_object` répété | Un parsing par appel | `json_tuple` ou `from_json` |
| `CUBE` | `2^n` niveaux | Spark duplique les lignes en entrée |
| `ROLLUP` | `n + 1` niveaux | Très rentable face à des `UNION ALL` |

### 9.2 Lecture des colonnes imbriquées

Avec des formats colonnaires (Parquet, ORC), lire `livraison.pays` **ne devrait lire que la colonne `pays`** du `STRUCT` (*nested column pruning*), mais ce comportement dépend du moteur, de la version et d'un paramètre de configuration. Ne le suppose pas : vérifie avec `EXPLAIN` (Blog 5) et mesure.

### 9.3 Quand utiliser (ou non) un type complexe

| Situation | Recommandation |
|---|---|
| Liste d'éléments d'une même entité qu'on lit **ensemble** | `ARRAY` : pratique, compact |
| Attributs variables peu filtrés | `MAP` : souple |
| Sous-objet bien défini (adresse, appareil) | `STRUCT` |
| Champ utilisé en **filtre**, **jointure** ou **partitionnement** | **Colonne plate typée**, pas un champ de `MAP` ou de JSON |
| Relation « un-à-plusieurs » interrogée par des agrégats | Table **relationnelle** séparée (une ligne par produit), souvent préférable à un `ARRAY` |

Un `MAP` est tentant (« je n'ai pas à faire évoluer le schéma ») mais il cache des problèmes : pas de typage par clé, pas de statistiques, clés fantômes (`promo`, `Promo`, `PROMO`...). La flexibilité se paie en qualité de données.

---

## 10. Résolution des trois demandes

### 10.1 Marketing : le rapport par pays de livraison

Le rapport initial utilisait `LATERAL VIEW explode` (sans `OUTER`) : les paniers 5 (vide) et 6 (NULL) disparaissaient, l'Italie avec. La version corrigée :

```sql
SELECT
  COALESCE(p.livraison.pays, 'INCONNU')                                        AS pays,
  COUNT(DISTINCT p.panier_id)                                                  AS nb_paniers,
  COUNT(produit)                                                               AS nb_produits,
  COUNT(DISTINCT CASE WHEN produit IS NULL THEN p.panier_id END)               AS nb_paniers_sans_produit
FROM paniers p
LATERAL VIEW OUTER explode(p.produits) t AS produit
GROUP BY COALESCE(p.livraison.pays, 'INCONNU')
ORDER BY pays;
```

| pays | nb_paniers | nb_produits | nb_paniers_sans_produit |
|---|---|---|---|
| FR | 2 | 3 | 0 |
| INCONNU | 1 | 0 | 1 |
| IT | 1 | 0 | 1 |
| MA | 1 | 2 | 0 |
| SN | 1 | 1 | 0 |

Chaque panier est désormais **présent** (6 au total) et on distingue ce qui est **vide** de ce qui est **absent** de la liste, grâce à `OUTER` et à des comptages précis (`COUNT(DISTINCT panier_id)` pour les paniers, `COUNT(produit)` pour les produits).

### 10.2 Produit : les événements JSON

On parse une fois, on aplatit les achats et on **compte les illisibles** (Spark) :

```sql
WITH analyses AS (
  SELECT
    id,
    from_json(payload,
      'client_id INT, type STRING, montant DOUBLE, produits ARRAY<STRING>,
       appareil STRUCT<os:STRING, version:STRING>') AS j
  FROM evenements_json
)
SELECT
  SUM(CASE WHEN j IS NULL THEN 1 ELSE 0 END)                         AS nb_invalides,
  COUNT(*)                                                           AS nb_total,
  ROUND(SUM(CASE WHEN j IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)    AS taux_invalides
FROM analyses;
```

| nb_invalides | nb_total | taux_invalides |
|---|---|---|
| 1 | 4 | 0.25 |

Les lignes valides alimentent la couche propre ; la ligne invalide (id 4) part en **quarantaine** avec le payload brut, pour analyse. Les produits des achats s'obtiennent ensuite par `LATERAL VIEW explode(j.produits)` : 3 lignes (Laptop, Souris pour l'événement 1 ; Casque pour l'événement 3).

> **Un taux d'illisibles de 25 % sur un échantillon est un signal** : on met en place un **seuil d'alerte** (par exemple, le job échoue si plus de 1 % des payloads sont invalides) plutôt que de les ignorer.

### 10.3 Finance : les sous-totaux et le tableau en colonnes

On combine `ROLLUP` (section 7) et le pivot conditionnel (section 8) :

```sql
WITH base AS (
  SELECT date_format(date_vente, 'yyyy-MM') AS mois, categorie, montant
  FROM ventes_dedup
  WHERE statut = 'VALIDEE'
),
agrege AS (
  SELECT
    categorie,
    GROUPING(categorie)                                              AS g_cat,
    SUM(CASE WHEN mois = '2024-03' THEN montant ELSE 0 END)          AS ca_2024_03,
    SUM(CASE WHEN mois = '2024-04' THEN montant ELSE 0 END)          AS ca_2024_04,
    SUM(montant)                                                     AS ca_total
  FROM base
  GROUP BY ROLLUP(categorie)
)
SELECT
  CASE WHEN g_cat = 1 THEN 'TOTAL' ELSE categorie END AS categorie_lbl,
  ca_2024_03, ca_2024_04, ca_total
FROM agrege
ORDER BY g_cat, categorie;
```

| categorie_lbl | ca_2024_03 | ca_2024_04 | ca_total |
|---|---|---|---|
| Audio | 300.00 | 150.00 | 450.00 |
| Informatique | 2650.00 | 105.00 | 2755.00 |
| **TOTAL** | **2950.00** | **255.00** | **3205.00** |

Le rapport demandé en **une seule passe**. Contrôle : `2950 + 255 = 3205`, qui est bien le CA validé total du Blog 3.

### Récapitulatif

| Demande | Cause du problème | Outil |
|---|---|---|
| Italie à 0 et panier 6 absent | `explode` supprime les tableaux vides et NULL | `LATERAL VIEW OUTER explode` |
| Événements JSON | JSON invalide = NULL silencieux | `from_json`, compte des invalides, quarantaine |
| Sous-totaux + colonnes par mois | Trois requêtes ou une seule passe | `ROLLUP` + `GROUPING()` + pivot conditionnel |

---

## 11. 10 questions d'entretien

**1. Quelle différence entre `ARRAY`, `MAP` et `STRUCT` ?**
`ARRAY` : liste ordonnée d'éléments de même type, accès par index. `MAP` : dictionnaire clé-valeur, accès par clé. `STRUCT` : objet à champs nommés de types possiblement différents, accès par `.champ`.

**2. Que fait `explode` sur un tableau vide ou NULL ?**
Aucune ligne n'est produite : la ligne d'origine **disparaît** du résultat. Pour la conserver, on utilise `LATERAL VIEW OUTER explode`, qui renvoie une ligne avec un NULL (comportement d'un `LEFT JOIN`).

**3. Comment mettre à plat un tableau de structs ?**
`LATERAL VIEW explode(articles) t AS a`, puis on accède aux champs : `a.produit`, `a.prix`. On peut aussi développer le struct avec `a.*`.

**4. Que renvoie `size(NULL)` ?**
-1 sur Hive et, par défaut, sur Spark (le comportement évolue avec le mode ANSI). À distinguer de `size(tableau_vide) = 0`. On protège avec `COALESCE` ou un test `IS NULL`.

**5. `collect_list` ou `collect_set` ? Et l'ordre ?**
`collect_list` garde les doublons, `collect_set` les supprime. Dans les deux cas, **l'ordre n'est pas garanti** : on trie avec `sort_array`. Les NULL sont ignorés et tout le groupe est gardé en mémoire dans une tâche.

**6. Pourquoi `get_json_object` est-il peu efficace et que faire ?**
Il re-parse le document à chaque appel. `json_tuple` parse une fois (premier niveau uniquement), `from_json` (Spark) parse une fois avec un schéma typé et gère l'imbrication. Idéalement on parse **à l'ingestion**.

**7. Comment distinguer un JSON invalide d'une clé absente ?**
Un JSON invalide donne NULL pour **tous** les champs, y compris la racine : `get_json_object(payload, '$') IS NULL`. Une clé absente d'un JSON valide ne donne NULL que pour ce champ.

**8. `ROLLUP`, `CUBE`, `GROUPING SETS` : quelles différences ?**
`ROLLUP(a, b)` : détail + sous-totaux hiérarchiques + total. `CUBE(a, b)` : toutes les combinaisons (2^n niveaux). `GROUPING SETS` : la liste exacte des niveaux voulus. Les deux premiers sont des raccourcis du troisième.

**9. À quoi sert `GROUPING()` ?**
À distinguer un NULL de sous-total (la colonne a été « remontée », valeur 1) d'un vrai NULL dans les données (valeur 0), et à produire des libellés comme `TOTAL`.

**10. Comment faire un pivot dynamique ? Et un unpivot ?**
Les colonnes d'un pivot sont figées par la requête : un pivot dynamique oblige à **générer le SQL** à partir de la liste des valeurs. L'unpivot s'écrit avec `stack(n, ...)` (Hive, Spark) ou une clause `UNPIVOT` (Spark récent) ; `stack` conserve les NULL.

---

## 12. 5 exercices

### Exercice 1 : le statut des paniers
Pour chaque panier, affiche `INCONNU` si le tableau est NULL, `VIDE` s'il est vide, `REMPLI` sinon.

<details>
<summary>Solution</summary>

```sql
SELECT
  panier_id,
  CASE
    WHEN produits IS NULL    THEN 'INCONNU'
    WHEN size(produits) = 0  THEN 'VIDE'
    ELSE                          'REMPLI'
  END AS statut_panier
FROM paniers
ORDER BY panier_id;
```

Paniers 1 à 4 : `REMPLI` ; panier 5 : `VIDE` ; panier 6 : `INCONNU`. L'ordre des `WHEN` compte : on teste `IS NULL` **avant** `size`, sinon `size(NULL) = -1` tomberait dans `ELSE`.
</details>

### Exercice 2 : les produits les plus présents dans les paniers
Pour chaque produit, le nombre de **paniers** qui le contiennent.

<details>
<summary>Solution</summary>

```sql
SELECT produit, COUNT(DISTINCT p.panier_id) AS nb_paniers
FROM paniers p
LATERAL VIEW explode(p.produits) t AS produit
GROUP BY produit
ORDER BY nb_paniers DESC, produit;
```

| produit | nb_paniers |
|---|---|
| Laptop | 2 |
| Casque | 1 |
| Clavier | 1 |
| Enceinte | 1 |
| Souris | 1 |

Le `DISTINCT` compte des paniers et non des lignes éclatées (utile si un produit apparaît deux fois dans le même panier).
</details>

### Exercice 3 : le taux de paniers avec promotion
Calcule la part des paniers qui ont une clé `promo` dans leurs attributs. Quel est le dénominateur ?

<details>
<summary>Solution</summary>

```sql
SELECT
  COUNT(*)                                                    AS nb_paniers,
  SUM(CASE WHEN attributs['promo'] IS NOT NULL THEN 1 ELSE 0 END) AS nb_avec_promo,
  ROUND(SUM(CASE WHEN attributs['promo'] IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS taux_sur_tous,
  ROUND(SUM(CASE WHEN attributs['promo'] IS NOT NULL THEN 1 ELSE 0 END)
        / COUNT(attributs), 4)                                AS taux_sur_attributs_connus
FROM paniers;
```

`nb_paniers = 6`, `nb_avec_promo = 2` (paniers 1 et 4). `taux_sur_tous = 0.3333` (2/6) et `taux_sur_attributs_connus = 0.4` (2/5, car `COUNT(attributs)` ignore le panier 6 dont le map est NULL). **Le dénominateur est une décision métier** : le panier 6 est-il « sans promo » ou « inconnu » ? Il faut la documenter.
</details>

### Exercice 4 : un CUBE pays × catégorie
Calcule le CA validé par pays du client et catégorie, avec tous les sous-totaux (ventes avec client connu, via `clients_dedup`).

<details>
<summary>Solution</summary>

```sql
WITH base AS (
  SELECT c.pays, v.categorie, v.montant
  FROM ventes_dedup v
  JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE'
)
SELECT pays, categorie, SUM(montant) AS ca,
       GROUPING(pays) AS g_pays, GROUPING(categorie) AS g_cat
FROM base
GROUP BY CUBE(pays, categorie);
```

**11 lignes** : 4 détails (`FR / Informatique 2405`, `IT / Informatique 25`, `MA / Audio 300`, `SN / Informatique 300`), 4 sous-totaux par pays (`FR 2405`, `IT 25`, `MA 300`, `SN 300`), 2 sous-totaux par catégorie (`Audio 300`, `Informatique 2730`) et le total **3030**, qui se réconcilie avec les ventes validées de clients connus (Blog 3).
</details>

### Exercice 5 : le système d'exploitation des événements
Compte les événements par système (`android`, `ios`...), en isolant les JSON invalides et les systèmes absents.

<details>
<summary>Solution</summary>

```sql
SELECT
  CASE
    WHEN get_json_object(payload, '$') IS NULL THEN 'INVALIDE'
    ELSE COALESCE(get_json_object(payload, '$.appareil.os'), 'OS_ABSENT')
  END AS os,
  COUNT(*) AS nb
FROM evenements_json
GROUP BY
  CASE
    WHEN get_json_object(payload, '$') IS NULL THEN 'INVALIDE'
    ELSE COALESCE(get_json_object(payload, '$.appareil.os'), 'OS_ABSENT')
  END
ORDER BY nb DESC, os;
```

| os | nb |
|---|---|
| android | 2 |
| INVALIDE | 1 |
| ios | 1 |

(L'ordre des deux ex æquo dépend de la casse.) Sans le test sur `'$'`, le JSON invalide et un JSON valide sans clé `os` seraient confondus dans le même `NULL`.
</details>

---

## 13. Checklist avant de livrer une requête

- [ ] Je sais, pour chaque tableau, si **vide** et **NULL** sont deux cas différents, et je les traite séparément
- [ ] `explode` sans `OUTER` n'est utilisé que si perdre les lignes vides est **voulu**
- [ ] Après un `explode`, je compte les entités avec `COUNT(DISTINCT id)`, pas `COUNT(*)`
- [ ] Un seul `explode` par requête (ou j'ai vérifié le produit cartésien)
- [ ] `size()` est protégé contre `size(NULL) = -1` ; les accès par index sont sûrs (`element_at`) si le mode ANSI est actif
- [ ] `collect_list` / `collect_set` est trié (`sort_array`) et appliqué à des groupes de **taille bornée**
- [ ] Le JSON est parsé **une fois**, avec un schéma typé, et les invalides sont comptés et mis en **quarantaine**
- [ ] Les valeurs extraites d'un JSON sont **castées** (pas de `STRING` pour un montant)
- [ ] `GROUPING()` distingue les sous-totaux des NULL réels
- [ ] Les sous-totaux se **réconcilient** avec un total calculé autrement
- [ ] `CUBE` n'est utilisé que sur peu de colonnes à faible cardinalité
- [ ] Les colonnes de filtre ou de jointure ne sont pas cachées dans un `MAP` ou un JSON

---

## 14. Prochain blog et blogs restants

**Blog 5 : performance I, lire un plan.** `EXPLAIN` (logique, physique, estimé), formats Parquet vs ORC, partitioning, bucketing, statistiques et *cost-based optimizer*, petits fichiers. On y vérifiera enfin les affirmations « à vérifier avec `EXPLAIN` » semées dans les quatre premiers blogs : pruning de partitions, nested column pruning, semi-jointures.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions, forward fill |
| ✅ 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE` / `GROUPING SETS`, pivot |
| 5 | Performance I : lire un plan | `EXPLAIN`, Parquet vs ORC, partitioning, bucketing, statistiques, petits fichiers |
| 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| 7 | Modélisation | Star schema, SCD 1 / 2 / 3, calendrier, clés de substitution, Data Vault |
| 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 5

Sans regarder la suite : (1) une requête sur une table de **2 milliards de lignes** met 40 minutes. Quelles sont **les trois premières choses** que tu regardes, dans l'ordre ? (2) Que cherches-tu dans la sortie d'un `EXPLAIN` : quels mots-clés te font dire « c'est ici que ça coûte cher » ? Si tu hésites, le Blog 5 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de l'adapter à ton propre jeu de données et de provoquer toi-même chaque piège.*