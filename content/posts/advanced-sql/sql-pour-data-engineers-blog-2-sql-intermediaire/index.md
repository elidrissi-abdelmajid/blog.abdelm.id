---
title: "SQL pour Data Engineers, Blog 2 : SQL intermédiaire"
description: "Sous-requêtes, CTE, opérations ensemblistes, dates et premières window functions."
weight: 2
aliases:
  - /posts/advanced-sql/sql-mastery-for-data-engineers-phase-2-intermediate-to-advanced/
---
> **Série** : SQL, de basique à tech lead · **Niveau** : 2/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Les différences de dialecte sont signalées quand elles comptent.
> **Prérequis** : le [Blog 1](blog-01-sql-bases.md) et ses tables `clients` et `ventes` (garde-les).

---

## 0. Pourquoi ce blog existe

Au Blog 1, on a appris à lire et à interroger une table sans se faire piéger. En production, une requête n'est presque jamais « un `SELECT` sur une table » : c'est une **chaîne d'étapes** (nettoyer, dédupliquer, combiner plusieurs sources, enrichir, agréger). Ce blog donne les outils pour construire cette chaîne proprement.

Le scénario continue :

> **Mercredi, 10h14.** Deux alertes de la Finance sur le rapport mensuel :
> 1. *« Le CA validé de mars **par segment** affiche 5 100. Hier, la requête de référence donnait 2 775. »*
> 2. *« Depuis qu'on charge avril, la vente 110 apparaît deux fois dans le rapport mars + avril. »*

Deux bugs, deux causes classiques : un **référentiel rechargé sans déduplication** et une **union de deux sources qui se chevauchent**. À la fin du blog, tu sauras les diagnostiquer, les corriger, et les éviter.

### Plan

1. [Les données de ce blog](#1-les-données-de-ce-blog)
2. [Les sous-requêtes](#2-les-sous-requêtes)
3. [Les CTE](#3-les-cte)
4. [EXISTS, IN ou JOIN ?](#4-exists-in-ou-join-)
5. [Opérations ensemblistes : UNION, INTERSECT, EXCEPT](#5-opérations-ensemblistes--union-intersect-except)
6. [Les fonctions de dates](#6-les-fonctions-de-dates)
7. [Les fonctions de chaînes](#7-les-fonctions-de-chaînes)
8. [Première rencontre avec les window functions](#8-première-rencontre-avec-les-window-functions)
9. [Résolution de l'incident](#9-résolution-de-lincident)
10. [10 questions d'entretien](#10-10-questions-dentretien)
11. [5 exercices](#11-5-exercices)
12. [Checklist avant de livrer une requête](#12-checklist-avant-de-livrer-une-requête)
13. [Prochain blog et feuille de route](#13-prochain-blog-et-feuille-de-route)

---

## 1. Les données de ce blog

On garde `clients` et `ventes` du Blog 1 et on ajoute deux tables.

### 1.1 `clients_raw` : le référentiel tel qu'il arrive de la source

Après deux chargements (le 1er et le 15 mars) puis un troisième en juin, on trouve des **doublons de clé** : les clients 1 et 4 ont été mis à jour à la source, et chaque version a été empilée au lieu de remplacer la précédente.

```sql
USE blog_sql;

CREATE TABLE clients_raw (
  client_id        INT,
  nom              STRING,
  pays             STRING,
  segment          STRING,
  date_inscription DATE,
  date_chargement  DATE
) STORED AS PARQUET;

INSERT INTO clients_raw VALUES
  (1, 'Alice Martin', 'FR', 'Premium',  DATE '2023-01-15', DATE '2024-03-01'),
  (1, 'Alice Martin', 'FR', 'Gold',     DATE '2023-01-15', DATE '2024-03-15'),  -- mise à jour
  (2, 'Bruno Diallo', 'SN', 'Standard', DATE '2023-03-02', DATE '2024-03-01'),
  (3, 'Chloé Benali', 'MA', 'Premium',  DATE '2023-05-20', DATE '2024-03-01'),
  (4, 'David Chen',   'FR', NULL,       DATE '2024-01-10', DATE '2024-03-01'),
  (4, 'David Chen',   'FR', 'Standard', DATE '2024-01-10', DATE '2024-03-15'),  -- segment renseigné
  (5, 'Emma Rossi',   'IT', 'Standard', DATE '2024-02-14', DATE '2024-03-01'),
  (6, 'Farid Haddad', 'MA', 'Standard', DATE '2024-06-01', DATE '2024-06-02');
```

8 lignes pour 6 clients distincts.

### 1.2 `ventes_avril` : le second chargement

Le chargement d'avril a été configuré avec une fenêtre qui **recouvre la fin de mars** : la vente 110 s'y retrouve, identique à celle de la table de mars.

```sql
CREATE TABLE ventes_avril LIKE ventes;

INSERT INTO ventes_avril VALUES
  (201, 1,    'Clavier',  'Informatique',  80.00, 1, 'VALIDEE', DATE '2024-04-02'),
  (202, 3,    'Casque',   'Audio',        150.00, 1, 'VALIDEE', DATE '2024-04-05'),
  (203, 5,    'Souris',   'Informatique',  25.00, 2, 'VALIDEE', DATE '2024-04-08'),
  (110, 1,    'Enceinte', 'Audio',         90.00, 3, NULL,      DATE '2024-03-20');  -- chevauchement
```

> **Dialecte** : `CREATE TABLE ... LIKE` copie la structure (et le format de stockage) sur Hive, Spark et Impala. Si un littéral `DATE '...'` est refusé sur ta version, remplace-le par `CAST('...' AS DATE)`.

### 1.3 Ce que les deux tables cachent

| Anomalie | Où | Conséquence attendue |
|---|---|---|
| Clé `client_id` dupliquée | `clients_raw` (clients 1 et 4) | Jointure : CA gonflé |
| Ligne identique dans deux sources | vente 110 dans `ventes` et `ventes_avril` | `UNION ALL` : double comptage |
| Segment NULL puis renseigné | client 4 | Il faut garder la **dernière** version |
| Nouveau client actif | client 5 (vente 203) | Utile pour `EXCEPT` et les anti-jointures |

---

## 2. Les sous-requêtes

Une sous-requête est une requête **imbriquée** dans une autre. Il existe cinq formes, et la différence entre elles compte autant que la syntaxe.

### 2.1 La sous-requête scalaire (1 ligne, 1 colonne)

```sql
-- Ventes dont le montant dépasse la moyenne générale
SELECT vente_id, montant
FROM ventes
WHERE montant > (SELECT AVG(montant) FROM ventes);
```

La moyenne est `3120 / 9 = 346.67`, donc on obtient **2 lignes** : les ventes 101 (1200) et 106 (1100).

Règles et pièges :

- Elle doit renvoyer **exactement une valeur**. Plusieurs lignes provoquent une **erreur** ; zéro ligne renvoie **NULL**.
- Si elle renvoie NULL, la comparaison vaut `UNKNOWN`, donc **aucune ligne** ne passe. Pas d'erreur, juste un résultat vide : un classique silencieux.
- Elle est calculée une seule fois (non corrélée), ce qui est peu coûteux.

### 2.2 La table dérivée (sous-requête dans le `FROM`)

La sous-requête devient une table temporaire de la requête. **L'alias est obligatoire.**

```sql
-- Ventes supérieures à la moyenne de LEUR catégorie
SELECT v.vente_id, v.categorie, v.montant, m.moyenne_cat
FROM ventes v
JOIN (
  SELECT categorie, AVG(montant) AS moyenne_cat
  FROM ventes
  GROUP BY categorie
) m ON v.categorie = m.categorie
WHERE v.montant > m.moyenne_cat;
```

| vente_id | categorie | montant | moyenne_cat |
|---|---|---|---|
| 101 | Informatique | 1200.00 | 455.00 |
| 106 | Informatique | 1100.00 | 455.00 |
| 104 | Audio | 150.00 | 130.00 |
| 108 | Audio | 150.00 | 130.00 |

Le schéma « agréger dans une sous-requête puis rejoindre » est un classique. Au Blog 3, on le remplacera par une window function (un seul passage sur les données).

### 2.3 La sous-requête avec `IN`

```sql
-- Clients ayant au moins une vente validée
SELECT client_id, nom
FROM clients
WHERE client_id IN (SELECT client_id FROM ventes WHERE statut = 'VALIDEE');
```

→ clients 1, 2, 3 et 4 (4 lignes). La liste contient un NULL (vente 107), mais **`IN` n'est pas touché par ce piège** : il ne fait que chercher une correspondance. C'est le `NOT IN` qui casse (Blog 1, section 5).

### 2.4 La sous-requête avec `EXISTS`

```sql
SELECT c.client_id, c.nom
FROM clients c
WHERE EXISTS (
  SELECT 1
  FROM ventes v
  WHERE v.client_id = c.client_id
  
    AND v.statut = 'VALIDEE'
);
```

Même résultat : clients 1, 2, 3, 4. `EXISTS` ne s'intéresse qu'à **l'existence** d'au moins une ligne ; ce qu'on met dans le `SELECT` interne est ignoré (`SELECT 1` est une convention).

### 2.5 La sous-requête corrélée

Une sous-requête est **corrélée** quand elle référence une colonne de la requête externe (ici `c.client_id`). Conceptuellement, elle est réévaluée **pour chaque ligne** externe.

En pratique, les optimiseurs modernes la **décorrèlent** en jointure (semi-jointure pour `EXISTS`). Mais :

- Hive et Spark n'acceptent pas toutes les formes de sous-requêtes corrélées, et ça varie selon la version. Si une forme est refusée, réécris-la avec une jointure sur une table dérivée (comme en 2.2).
- Ne compte jamais sur l'optimiseur sans vérifier : lis le plan avec `EXPLAIN` (Blog 5).

### 2.6 Résumé

| Forme | Renvoie | Où l'utiliser | Piège principal |
|---|---|---|---|
| Scalaire | 1 valeur | `WHERE`, `SELECT` | Plusieurs lignes = erreur, zéro ligne = NULL |
| Table dérivée | Une table | `FROM` / `JOIN` | Alias obligatoire |
| `IN` | Une liste | `WHERE` | Aucun avec `IN` ; **`NOT IN` + NULL = 0 ligne** |
| `EXISTS` | Vrai / faux | `WHERE` | Doit être corrélée pour avoir un sens |
| Corrélée | Dépend de la ligne externe | `WHERE`, `SELECT` | Support variable selon version, coût potentiel |

---

## 3. Les CTE

Une CTE (*Common Table Expression*, clause `WITH`) donne un **nom** à une sous-requête, définie avant la requête principale.

### 3.1 Pourquoi les utiliser

Reprenons la requête de la section 2.2 avec une CTE :

```sql
WITH moyenne_par_categorie AS (
  SELECT categorie, AVG(montant) AS moyenne_cat
  FROM ventes
  GROUP BY categorie
)
SELECT v.vente_id, v.categorie, v.montant, m.moyenne_cat
FROM ventes v
JOIN moyenne_par_categorie m ON v.categorie = m.categorie
WHERE v.montant > m.moyenne_cat;
```

Le résultat est identique, mais le code se lit **de haut en bas**, comme un pipeline : d'abord une étape qui calcule les moyennes, ensuite l'étape qui les utilise.

### 3.2 Enchaîner plusieurs CTE

C'est là qu'elles deviennent indispensables. Chaque CTE peut utiliser celles définies avant elle :

```sql
WITH
ventes_validees AS (
  SELECT vente_id, client_id, categorie, montant
  FROM ventes
  WHERE statut = 'VALIDEE'
),
ca_par_client AS (
  SELECT client_id, SUM(montant) AS ca
  FROM ventes_validees
  GROUP BY client_id
)
SELECT c.nom, k.ca
FROM ca_par_client k
JOIN clients c ON k.client_id = c.client_id
ORDER BY k.ca DESC;
```

| nom | ca |
|---|---|
| Alice Martin | 1225.00 |
| David Chen | 1100.00 |
| Bruno Diallo | 300.00 |
| Chloé Benali | 150.00 |

(Les ventes 107 et 108 n'apparaissent pas : le `JOIN` avec `clients` les écarte, voir Blog 1.)

### 3.3 Les idées reçues sur les CTE

**Une CTE n'est pas une table temporaire.** C'est un alias pour une requête. Dans Hive comme dans Spark, une CTE référencée **plusieurs fois** peut être **recalculée à chaque référence** : rien ne garantit qu'elle soit calculée une seule fois et mise de côté. Quelques conséquences :

- Une CTE coûteuse utilisée trois fois peut coûter trois fois. Si c'est le cas, matérialise explicitement : `CREATE TABLE ... AS` ou `CACHE TABLE` (Spark) plutôt que d'espérer.
- Une CTE ne sert pas de « barrière d'optimisation » : l'optimiseur peut réordonner des opérations à travers elle (*predicate pushdown*, comme au Blog 1).
- Le comportement exact de matérialisation dépend de la version et des paramètres. Vérifie avec `EXPLAIN`.

**La lisibilité est la raison principale d'utiliser une CTE**, pas la performance.

### 3.4 CTE vs sous-requête vs vue : quel choix ?

| Besoin | Outil |
|---|---|
| Structurer une requête lisible | **CTE** |
| Petite étape unique, non réutilisée | Table dérivée |
| Réutiliser la logique dans **plusieurs requêtes** | **Vue** (`CREATE VIEW`) |
| Réutiliser un résultat **coûteux** sans le recalculer | **Table** (`CREATE TABLE AS`) |

> Les CTE **récursives** (`WITH RECURSIVE`) ne sont pas disponibles sur toutes les versions de Hive et de Spark. Vérifie avant d'en faire une dépendance.

### 3.5 Bonnes pratiques

1. **Un nom qui dit ce que contient l'étape** : `ventes_validees`, pas `t1`.
2. **Une CTE = une responsabilité** (filtrer, dédupliquer, enrichir, agréger).
3. **Pas de `SELECT *`** dans une CTE : on perd la maîtrise du schéma qui circule.
4. **Teste chaque étape isolément** en remplaçant temporairement la requête finale par `SELECT * FROM etape LIMIT 20`.

---

## 4. EXISTS, IN ou JOIN ?

Trois manières d'exprimer « les clients qui ont au moins une vente validée ». Elles ne sont **pas équivalentes**.

```sql
-- A) JOIN
SELECT c.client_id, c.nom
FROM clients c
JOIN ventes v ON c.client_id = v.client_id
WHERE v.statut = 'VALIDEE';
-- → 6 lignes (les doublons apparaissent)

-- B) IN
SELECT client_id, nom FROM clients
WHERE client_id IN (SELECT client_id FROM ventes WHERE statut = 'VALIDEE');
-- → 4 lignes

-- C) EXISTS
SELECT c.client_id, c.nom FROM clients c
WHERE EXISTS (SELECT 1 FROM ventes v
              WHERE v.client_id = c.client_id AND v.statut = 'VALIDEE');
-- → 4 lignes
```

Le `JOIN` renvoie **une ligne par vente** : Alice (ventes 101 et 102) apparaît deux fois, Chloé (104 et 105) aussi : 2 + 1 + 2 + 1 = 6 lignes. `IN` et `EXISTS` sont des **semi-jointures** : ils répondent « existe-t-il au moins une correspondance ? » et ne multiplient jamais les lignes de gauche.

| | `JOIN` | `IN` | `EXISTS` |
|---|---|---|---|
| Multiplie les lignes de gauche si plusieurs correspondances | **Oui** | Non | Non |
| Permet de lire des colonnes de l'autre table | **Oui** | Non | Non |
| Sensible au NULL | Clé NULL ne matche pas | Inoffensif | Inoffensif |
| Version « négative » | `LEFT JOIN ... IS NULL` | **`NOT IN` : piège du NULL** | `NOT EXISTS` : sûr |

### Comment choisir

- **Tu as besoin de colonnes de l'autre table** → `JOIN`.
- **Tu testes seulement l'existence** → `EXISTS` (ou `IN` pour une liste courte et non nulle).
- **Tu cherches l'absence** → `NOT EXISTS` ou `LEFT JOIN ... IS NULL`, **jamais `NOT IN`** sur une colonne nullable.
- **Tu réponds « performance »** en entretien : pour `IN` / `EXISTS`, les moteurs modernes produisent souvent la **même semi-jointure** ; la différence se vérifie dans le plan (`EXPLAIN`), pas dans une règle apprise par cœur.

> Réflexe : si tu écris `SELECT DISTINCT` après un `JOIN` uniquement pour supprimer des doublons que le `JOIN` vient de créer, remplace-le par un `EXISTS`. Tu exprimes mieux l'intention et tu évites un shuffle de déduplication.

---

## 5. Opérations ensemblistes : UNION, INTERSECT, EXCEPT

Les jointures combinent des tables **horizontalement** (colonnes). Les opérations ensemblistes les combinent **verticalement** (lignes).

### 5.1 Les règles communes

- Les deux requêtes ont **le même nombre de colonnes**.
- Les colonnes sont associées **par position, pas par nom**.
- Les types doivent être **compatibles** (ou convertibles).
- Les noms de colonnes viennent de la **première** requête.
- `ORDER BY` s'applique au résultat final et se place à la fin.

> **Piège de la position** : si tu inverses deux colonnes de même type entre les deux `SELECT` (par exemple `quantite` et `client_id`, tous deux `INT`), il n'y a **aucune erreur**, juste des données mélangées. Liste toujours les colonnes dans le même ordre, et sans `SELECT *`.

### 5.2 UNION ALL vs UNION

```sql
-- UNION ALL : empile tout, 14 lignes (10 + 4)
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes
UNION ALL
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes_avril;

-- UNION : empile et supprime les lignes totalement identiques, 13 lignes
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes
UNION
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes_avril;
```

La vente 110 est présente dans les deux tables avec **exactement les mêmes valeurs** : `UNION ALL` la garde deux fois (14 lignes), `UNION` la déduplique (13 lignes).

| | `UNION ALL` | `UNION` |
|---|---|---|
| Doublons | Conservés | Supprimés (ligne entière identique) |
| Coût | Faible | Plus élevé : un **shuffle** pour dédupliquer |
| Usage recommandé | **Par défaut** | Quand la déduplication est voulue et que les lignes sont strictement identiques |

> **Règle** : écris `UNION ALL` par défaut, puis déduplique **explicitement et sur la bonne clé**. Un `UNION` ne supprime que les lignes **parfaitement identiques** : si la vente 110 avait un `montant` corrigé dans le second chargement, `UNION` garderait les deux versions. La vraie déduplication se fait par clé métier avec `ROW_NUMBER` (section 8).

### 5.3 INTERSECT et EXCEPT

```sql
-- Lignes présentes dans les deux tables : 1 ligne (la vente 110)
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes
INTERSECT
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes_avril;

-- Lignes d'avril qui n'existent pas dans mars : 3 lignes (201, 202, 203)
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes_avril
EXCEPT
SELECT vente_id, client_id, categorie, montant, statut, date_vente FROM ventes;
```

`INTERSECT` et `EXCEPT` comparent des **lignes entières**. Les versions DISTINCT sont le comportement standard. Selon la version de Hive ou d'Impala, ces opérateurs peuvent ne pas être disponibles ; `NOT EXISTS` et `LEFT JOIN ... IS NULL` sont des alternatives universelles.

### 5.4 Le NULL se comporte différemment ici

Dans les opérations ensemblistes (et dans `DISTINCT` et `GROUP BY`), **deux NULL sont considérés comme égaux**, contrairement à `=`. C'est pour ça que la vente 110 (statut NULL) est bien reconnue comme identique entre les deux tables. Sans cette règle, `UNION` ne la dédupliquerait pas.

**Cas d'usage : « les clients actifs en avril mais pas en mars ».**

```sql
SELECT client_id FROM ventes_avril
EXCEPT
SELECT client_id FROM ventes;
```

→ **un seul résultat : le client 5** (Emma Rossi, première vente en avril). Avec `NOT IN`, la même idée renverrait **0 ligne**, car la liste de mars contient un `client_id` NULL (vente 107) :

```sql
-- ❌ 0 ligne, piège du NULL (Blog 1)
SELECT DISTINCT client_id FROM ventes_avril
WHERE client_id NOT IN (SELECT client_id FROM ventes);
```

---

## 6. Les fonctions de dates

Les dates sont la première source de bugs silencieux d'un rapport mensuel : mauvaises bornes, mauvais fuseau, ordre des arguments inversé.

### 6.1 La boîte à outils (Hive / Spark SQL)

| Besoin | Fonction | Exemple → résultat |
|---|---|---|
| Extraire une partie | `year(d)`, `month(d)`, `day(d)`, `dayofweek(d)` | `month(DATE '2024-03-20')` → 3 |
| Formater en texte | `date_format(d, 'yyyy-MM')` | → `'2024-03'` |
| Tronquer au début du mois | `trunc(d, 'MM')` | → `2024-03-01` |
| Dernier jour du mois | `last_day(d)` | `2024-03-20` → `2024-03-31` |
| Ajouter des jours | `date_add(d, n)` | `date_add(DATE '2024-03-20', 12)` → `2024-04-01` |
| Ajouter des mois | `add_months(d, n)` | `add_months(DATE '2024-01-31', 1)` → `2024-02-29` |
| Différence en jours | `datediff(fin, debut)` | `datediff(DATE '2024-03-20', DATE '2024-03-01')` → 19 |
| Date du jour | `current_date()` | |
| Texte → date | `to_date(s)`, `CAST(s AS DATE)` | |

> **Les noms de fonctions de dates sont ce qui diffère le plus d'un moteur à l'autre.** Sur Impala en particulier, vérifie la syntaxe exacte de `trunc`, `date_trunc` et du formatage dans la documentation de ta version avant de copier ces exemples.

### 6.2 Exemple : le délai entre l'inscription et la première vente

```sql
SELECT
  c.client_id,
  c.nom,
  MIN(v.date_vente)                                    AS premiere_vente,
  datediff(MIN(v.date_vente), c.date_inscription)      AS jours_avant_1ere_vente
FROM clients c
JOIN ventes v ON c.client_id = v.client_id
GROUP BY c.client_id, c.nom, c.date_inscription
ORDER BY c.client_id;
```

| client_id | nom | premiere_vente | jours_avant_1ere_vente |
|---|---|---|---|
| 1 | Alice Martin | 2024-03-01 | 411 |
| 2 | Bruno Diallo | 2024-03-05 | 369 |
| 3 | Chloé Benali | 2024-03-07 | 292 |
| 4 | David Chen | 2024-03-10 | 60 |

Notes : (a) on a groupé sur `date_inscription` car elle n'est pas agrégée (règle du Blog 1) ; (b) le résultat de 369 jours pour Bruno inclut le 29 février 2024, une bonne occasion de se rappeler que `datediff` compte des jours réels, pas des « années × 365 ».

### 6.3 Agréger par mois

```sql
SELECT
  date_format(date_vente, 'yyyy-MM') AS mois,
  COUNT(*)                           AS nb_ventes,
  SUM(montant)                       AS ca
FROM ventes
GROUP BY date_format(date_vente, 'yyyy-MM')
ORDER BY mois;
```

Deux variantes : `date_format(...)` renvoie un **texte** (`'2024-03'`, pratique pour l'affichage) ; `trunc(date_vente, 'MM')` renvoie une **date** (`2024-03-01`), mieux pour les calculs et les jointures avec un calendrier.

### 6.4 Les pièges des dates

1. **`BETWEEN` sur un timestamp** (Blog 1) : `BETWEEN '2024-03-01' AND '2024-03-31'` perd la journée du 31. Utilise `>= debut AND < fin_exclusive`.
2. **L'ordre des arguments de `datediff`** : Hive et Spark prennent `(fin, debut)`, d'autres moteurs (SQL Server) prennent `(unité, debut, fin)`. Teste avec deux dates connues avant de faire confiance.
3. **Les dates stockées en texte** : `'2024-3-1'` et `'2024-03-01'` ne se comparent pas correctement (tri lexicographique). Convertis en `DATE` à l'ingestion.
4. **Le fuseau horaire des timestamps** : un `TIMESTAMP` lu depuis Parquet peut être interprété en UTC ou en heure locale selon le moteur (Hive, Impala, Spark) et ses paramètres. Pour un rapport quotidien, un décalage de quelques heures déplace des lignes d'un jour à l'autre. Vérifie sur un exemple connu.
5. **Une fonction sur la colonne de filtre** : `WHERE year(date_vente) = 2024` est lisible mais peut empêcher le pruning de partitions ou l'usage de statistiques. Préfère `date_vente >= DATE '2024-01-01' AND date_vente < DATE '2025-01-01'`.

---

## 7. Les fonctions de chaînes

Dans un pipeline réel, 50 % des problèmes de jointure sont des problèmes de chaînes : espaces, casse, accents, formats.

### 7.1 L'essentiel

| Besoin | Fonction | Exemple |
|---|---|---|
| Casse | `UPPER`, `LOWER` | `UPPER('Audio')` → `'AUDIO'` |
| Nettoyer les espaces | `TRIM`, `LTRIM`, `RTRIM` | `TRIM(' abc ')` → `'abc'` |
| Extraire | `substr(s, pos, len)` | `substr('Informatique', 1, 4)` → `'Info'` (**position à partir de 1**) |
| Longueur | `length(s)` | |
| Concaténer | `concat(a, b)`, `concat_ws(sep, a, b)` | voir ci-dessous |
| Découper | `split(s, regex)` | `split('Alice Martin', ' ')[0]` → `'Alice'` |
| Remplacer | `regexp_replace(s, regex, rempl)` | `regexp_replace('06-12.34', '[^0-9]', '')` → `'061234'` |
| Chercher | `instr(s, sous_chaine)`, `LIKE`, `RLIKE` | |
| Compléter | `lpad(s, n, c)` | `lpad('7', 3, '0')` → `'007'` |

### 7.2 `concat` et le NULL

Sur la table `clients` du Blog 1, le client 4 a un `segment` NULL :

```sql
SELECT
  nom,
  concat(nom, ' - ', segment)     AS avec_concat,
  concat_ws(' - ', nom, segment)  AS avec_concat_ws
FROM clients
WHERE client_id IN (1, 4);
```

| nom | avec_concat | avec_concat_ws |
|---|---|---|
| Alice Martin | Alice Martin - Premium | Alice Martin - Premium |
| David Chen | **NULL** | David Chen |

`concat` renvoie NULL dès qu'un argument est NULL (le NULL « contamine » tout, comme au Blog 1). `concat_ws` **ignore** les NULL. Utilise `concat_ws` ou `COALESCE(segment, '')` pour ne pas perdre des lignes de libellé.

### 7.3 Les pièges des chaînes

1. **Les espaces et la casse invisibles** : `'FR '` n'est pas égal à `'FR'`. Normalise avec `UPPER(TRIM(col))` **à l'ingestion**, pas à chaque jointure.
2. **`split` utilise une expression régulière** : pour découper sur `.` ou `|`, il faut échapper (`'\\.'`, `'\\|'`), sinon le résultat est surprenant.
3. **Les accents** : `'Chloé'` et `'Chloe'` sont différents. Pour comparer sans accents, passe par `translate` ou normalise en amont ; ne le fais pas dans une clé de jointure.
4. **`LIKE` et ses caractères spéciaux** : `%` et `_` sont des jokers. Pour chercher un `_` littéral, il faut l'échapper.
5. **Les jointures sur des chaînes transformées** : `ON UPPER(TRIM(a.code)) = UPPER(TRIM(b.code))` fonctionne, mais empêche les optimisations et coûte cher à grande échelle.

---

## 8. Première rencontre avec les window functions

Une window function calcule une valeur **pour chaque ligne en regardant les lignes voisines**, sans réduire le nombre de lignes (contrairement à `GROUP BY`). Ce blog donne les bases ; le Blog 3 est entièrement consacré aux usages avancés.

### 8.1 L'anatomie

```sql
fonction() OVER (
  PARTITION BY colonne      -- les « groupes » : la fonction repart de zéro dans chacun
  ORDER BY colonne          -- l'ordre à l'intérieur du groupe
)
```

- **`PARTITION BY`** découpe les lignes en groupes (optionnel : sans lui, tout le jeu de données est un seul groupe).
- **`ORDER BY`** donne l'ordre utilisé par la fonction.
- Une window function s'exécute au stade `SELECT` de l'ordre logique (Blog 1, section 3) : **on ne peut donc pas l'utiliser dans un `WHERE`**. Pour filtrer sur son résultat, on l'encapsule dans une CTE ou une sous-requête.

### 8.2 `ROW_NUMBER`, `RANK` et `DENSE_RANK`

Classons les ventes de la plus grosse à la plus petite :

```sql
SELECT
  vente_id,
  montant,
  ROW_NUMBER() OVER (ORDER BY montant DESC NULLS LAST, vente_id) AS rn,
  RANK()       OVER (ORDER BY montant DESC NULLS LAST)           AS rk,
  DENSE_RANK() OVER (ORDER BY montant DESC NULLS LAST)           AS drk
FROM ventes
ORDER BY rn;
```

| vente_id | montant | rn | rk | drk |
|---|---|---|---|---|
| 101 | 1200.00 | 1 | 1 | 1 |
| 106 | 1100.00 | 2 | 2 | 2 |
| 109 | 300.00 | 3 | 3 | 3 |
| 104 | 150.00 | 4 | **4** | **4** |
| 108 | 150.00 | 5 | **4** | **4** |
| 110 | 90.00 | 6 | 6 | 5 |
| 103 | 80.00 | 7 | 7 | 6 |
| 102 | 25.00 | 8 | **8** | **7** |
| 107 | 25.00 | 9 | **8** | **7** |
| 105 | NULL | 10 | 10 | 8 |

| Fonction | Comportement sur les ex æquo | Usage typique |
|---|---|---|
| `ROW_NUMBER` | Numérote **sans ex æquo** (arbitraire si l'ordre ne suffit pas à départager) | Dédupliquer, « garder une seule ligne » |
| `RANK` | Ex æquo = même rang, **puis un trou** (4, 4, 6) | Classement sportif |
| `DENSE_RANK` | Ex æquo = même rang, **sans trou** (4, 4, 5) | « Les N valeurs les plus élevées » |

Deux points essentiels :

- **Ajoute toujours un critère de départage unique** (ici `vente_id`) à `ROW_NUMBER`. Sans lui, 104 et 108 (tous deux à 150) peuvent changer de rang d'une exécution à l'autre : le résultat n'est pas déterministe.
- **Précise `NULLS LAST`**. Sinon la position du NULL dépend du moteur (Blog 1, section 5).

### 8.3 Un agrégat en window : la part du total

On combine `GROUP BY` et window function en une requête :

```sql
SELECT
  categorie,
  SUM(montant)                                  AS ca,
  SUM(montant) / SUM(SUM(montant)) OVER ()      AS part_du_total
FROM ventes
GROUP BY categorie;
```

| categorie | ca | part_du_total |
|---|---|---|
| Informatique | 2730.00 | 0.875 |
| Audio | 390.00 | 0.125 |

Le `SUM(SUM(montant)) OVER ()` se lit de l'intérieur : le `SUM` interne est l'agrégat par catégorie (étape `GROUP BY`), le `SUM ... OVER ()` externe additionne ces résultats sur **toutes** les lignes (3120). Ça fonctionne parce que les window functions s'exécutent **après** `GROUP BY` : leur matière première est déjà agrégée.

### 8.4 Le cas d'usage n°1 en data engineering : dédupliquer

« Garde, pour chaque `client_id`, la version la plus récente (`date_chargement`). »

```sql
WITH classees AS (
  SELECT
    client_id, nom, pays, segment, date_inscription, date_chargement,
    ROW_NUMBER() OVER (
      PARTITION BY client_id
      ORDER BY date_chargement DESC
    ) AS rn
  FROM clients_raw
)
SELECT client_id, nom, pays, segment, date_inscription
FROM classees
WHERE rn = 1;
```

| client_id | nom | pays | segment |
|---|---|---|---|
| 1 | Alice Martin | FR | **Gold** |
| 2 | Bruno Diallo | SN | Standard |
| 3 | Chloé Benali | MA | Premium |
| 4 | David Chen | FR | **Standard** |
| 5 | Emma Rossi | IT | Standard |
| 6 | Farid Haddad | MA | Standard |

6 lignes pour 6 clients : la clé est maintenant unique, et Alice est bien passée de Premium à Gold, David de NULL à Standard.

Trois règles pour une déduplication fiable :

1. **`PARTITION BY` = la clé métier** (ce qui définit « la même entité »).
2. **`ORDER BY` = le critère de fraîcheur, `DESC`** (la ligne `rn = 1` est la plus récente).
3. **Un critère de départage unique en dernier recours.** Si deux lignes ont la même `date_chargement`, `ROW_NUMBER` en choisira une arbitrairement. Ajoute une colonne comme un identifiant de chargement ou un `ingestion_ts`.

> **Ce n'est pas un `SELECT DISTINCT`.** `DISTINCT` supprime des lignes *entièrement* identiques ; ici, les deux versions d'Alice diffèrent par leur segment. Seule la déduplication par clé avec un critère de fraîcheur règle le problème correctement.

---

## 9. Résolution de l'incident

Retour à mercredi, 10h14. Deux alertes, deux diagnostics.

### 9.1 Alerte n°1 : « le CA par segment affiche 5 100 au lieu de 2 775 »

**Étape 1, suspecter la clé de jointure.** C'est le premier réflexe (Blog 1, section 8.6) :

```sql
SELECT client_id, COUNT(*) AS n
FROM clients_raw
GROUP BY client_id
HAVING COUNT(*) > 1;
```

→ les clients 1 et 4, avec 2 lignes chacun. La clé n'est pas unique.

**Étape 2, mesurer l'effet.** La requête de reporting joignait `clients_raw` directement :

```sql
SELECT c.segment, SUM(v.montant) AS ca
FROM ventes v
JOIN clients_raw c ON v.client_id = c.client_id
WHERE v.statut = 'VALIDEE'
GROUP BY c.segment;
```

| segment | ca |
|---|---|
| Premium | 1375.00 |
| Gold | 1225.00 |
| Standard | 1400.00 |
| NULL | 1100.00 |

Total : **5 100**. Chaque vente d'Alice (1 225) est comptée **deux fois**, une fois dans Premium et une fois dans Gold, et celle de David (1 100) apparaît dans les segments NULL et Standard. Les ventes ne sont pas fausses : c'est la jointure qui les multiplie.

**Étape 3, corriger à la source du problème :** dédupliquer le référentiel avant de joindre (section 8.4). Après correction : Gold 1 225, Premium 150, Standard 1 400, soit **2 775**, le chiffre de référence.

### 9.2 Alerte n°2 : « la vente 110 apparaît deux fois dans mars + avril »

```sql
-- Version fautive : UNION ALL
WITH toutes AS (
  SELECT vente_id, client_id, montant, statut, date_vente FROM ventes
  UNION ALL
  SELECT vente_id, client_id, montant, statut, date_vente FROM ventes_avril
)
SELECT date_format(date_vente, 'yyyy-MM') AS mois, COUNT(*) AS nb_lignes, SUM(montant) AS ca
FROM toutes
GROUP BY date_format(date_vente, 'yyyy-MM')
ORDER BY mois;
```

| mois | nb_lignes | ca |
|---|---|---|
| 2024-03 | **11** | **3210.00** |
| 2024-04 | 3 | 255.00 |

Mars devrait avoir 10 lignes et 3 120 de CA : la vente 110 (90) est comptée deux fois car le chargement d'avril l'a ré-ingérée. Avec `UNION`, on retrouve 10 lignes et 3 120 ; mais comme on l'a vu en 5.2, `UNION` ne protège que si les lignes sont **strictement identiques**. La correction robuste déduplique sur la clé `vente_id` avec `ROW_NUMBER`.

### 9.3 Le pipeline corrigé, avec garde-fous

```sql
WITH
clients_dedup AS (
  SELECT client_id, nom, pays, segment
  FROM (
    SELECT client_id, nom, pays, segment,
           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_chargement DESC) AS rn
    FROM clients_raw
  ) t
  WHERE rn = 1
),
ventes_union AS (
  SELECT vente_id, client_id, montant, statut, date_vente FROM ventes
  UNION ALL
  SELECT vente_id, client_id, montant, statut, date_vente FROM ventes_avril
),
ventes_dedup AS (
  SELECT vente_id, client_id, montant, statut, date_vente
  FROM (
    SELECT vente_id, client_id, montant, statut, date_vente,
           ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY date_vente DESC) AS rn
    FROM ventes_union
  ) t
  WHERE rn = 1
),
enrichi AS (
  SELECT
    date_format(v.date_vente, 'yyyy-MM') AS mois,
    COALESCE(c.segment, 'INCONNU')       AS segment,
    v.montant
  FROM ventes_dedup v
  LEFT JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE'
)
SELECT mois, segment, COUNT(*) AS nb_ventes, SUM(montant) AS ca
FROM enrichi
GROUP BY mois, segment
ORDER BY mois, segment;
```

| mois | segment | nb_ventes | ca |
|---|---|---|---|
| 2024-03 | Gold | 2 | 1225.00 |
| 2024-03 | INCONNU | 2 | 175.00 |
| 2024-03 | Premium | 2 | 150.00 |
| 2024-03 | Standard | 2 | 1400.00 |
| 2024-04 | Gold | 1 | 80.00 |
| 2024-04 | Premium | 1 | 150.00 |
| 2024-04 | Standard | 1 | 25.00 |

Le total de mars (`1225 + 175 + 150 + 1400`) est **2 950**, soit exactement le CA validé de l'ERP du Blog 1. C'est la **réconciliation** qui valide le pipeline. Notes :

- Les ventes sans client connu (107 et 108, soit 175) sont conservées dans un segment `INCONNU` grâce au `LEFT JOIN` : on ne cache pas les écarts, on les rend visibles.
- Le segment `INCONNU` n'est pas un bug : c'est un signal de qualité de données à traiter (Blog 9).

### 9.4 Les garde-fous à brancher

```sql
-- 1) Unicité de la clé après déduplication : doit renvoyer 0
SELECT COUNT(*) - COUNT(DISTINCT client_id) AS doublons FROM clients_dedup;

-- 2) Aucune ligne perdue ni dupliquée par la jointure : les deux comptes doivent être égaux
SELECT
  (SELECT COUNT(*) FROM ventes_dedup WHERE statut = 'VALIDEE') AS avant_jointure,
  (SELECT COUNT(*) FROM enrichi)                               AS apres_jointure;
```

> En pratique, ces tests tournent **à chaque exécution du pipeline** et font échouer le job s'ils ne sont pas verts. Mieux vaut un job rouge qu'un dashboard faux et vert.

---

## 10. 10 questions d'entretien

**1. Quelle différence entre CTE, sous-requête, vue et table temporaire ?**
Une CTE et une sous-requête sont des constructions **dans une requête** (la CTE se lit de haut en bas, la sous-requête s'imbrique). Une vue est une requête **stockée** et nommée, recalculée à chaque lecture. Une table temporaire ou `CREATE TABLE AS` **matérialise** le résultat. La CTE n'est pas garantie d'être calculée une seule fois.

**2. `UNION` vs `UNION ALL` ?**
`UNION ALL` empile sans toucher aux doublons (rapide). `UNION` supprime les lignes entièrement identiques, ce qui coûte un shuffle. On utilise `UNION ALL` par défaut, puis on déduplique explicitement sur la clé métier.

**3. `IN`, `EXISTS` ou `JOIN` pour trouver les clients ayant une vente validée ?**
`EXISTS` (ou `IN`) si on teste seulement l'existence : ils ne dupliquent pas les lignes de gauche. `JOIN` si on a besoin de colonnes de l'autre table. Les optimiseurs produisent souvent la même semi-jointure : on le vérifie dans le plan.

**4. Comment dédupliquer une table en gardant la ligne la plus récente par clé ?**
`ROW_NUMBER() OVER (PARTITION BY cle ORDER BY date DESC, critere_unique)`, puis on filtre `rn = 1` dans une CTE ou une sous-requête.

**5. `ROW_NUMBER`, `RANK` et `DENSE_RANK` ?**
`ROW_NUMBER` attribue un numéro unique par ligne. `RANK` donne le même rang aux ex æquo puis laisse un trou. `DENSE_RANK` donne le même rang sans trou.

**6. Peut-on utiliser une window function dans un `WHERE` ? Pourquoi ?**
Non : elles s'exécutent au stade `SELECT`, après `WHERE`, `GROUP BY` et `HAVING`. On encapsule la requête dans une CTE ou une sous-requête, puis on filtre à l'extérieur.

**7. Que se passe-t-il si une sous-requête scalaire renvoie plusieurs lignes ? Aucune ligne ?**
Plusieurs lignes : erreur. Aucune ligne : la sous-requête vaut NULL, donc la comparaison est inconnue et le `WHERE` ne retient aucune ligne, sans message d'erreur.

**8. Comment calculer le nombre de jours entre deux dates, et quel piège sur les timestamps ?**
`datediff(fin, debut)` (Hive, Spark). Piège : `BETWEEN debut AND fin` sur un `TIMESTAMP` exclut la dernière journée ; on utilise `>= debut AND < fin_exclusive`. Autre piège : le fuseau horaire.

**9. Pourquoi `concat(a, b)` renvoie NULL et comment l'éviter ?**
Parce que tout argument NULL contamine le résultat. On utilise `concat_ws(sep, a, b)` qui ignore les NULL, ou `COALESCE`.

**10. Comment `INTERSECT` / `EXCEPT` traitent-ils les NULL, et que faire si l'opérateur n'existe pas sur la version ?**
Deux NULL sont considérés comme égaux (contrairement à `=`). Si l'opérateur n'est pas disponible, on utilise `NOT EXISTS` ou `LEFT JOIN ... IS NULL` pour `EXCEPT`, et `EXISTS` ou `JOIN` pour `INTERSECT`.

---

## 11. 5 exercices

### Exercice 1 : la promesse du Blog 1
Réécris la requête de réconciliation du Blog 1 (section 9) **sans CTE**, uniquement avec une sous-requête. Qu'est-ce qui devient moins lisible ?

<details>
<summary>Solution</summary>

```sql
SELECT
  seau,
  COUNT(*)                   AS nb_lignes,
  COUNT(montant)             AS nb_montants_renseignes,
  COALESCE(SUM(montant), 0)  AS ca
FROM (
  SELECT
    v.vente_id,
    v.montant,
    CASE
      WHEN v.statut IS NULL      THEN 'STATUT_INCONNU'
      WHEN v.statut = 'ANNULEE'  THEN 'ANNULEE'
      WHEN v.client_id IS NULL   THEN 'VALIDEE_SANS_CLIENT'
      WHEN c.client_id IS NULL   THEN 'VALIDEE_CLIENT_ORPHELIN'
      ELSE                            'VALIDEE_OK'
    END AS seau
  FROM ventes v
  LEFT JOIN clients c ON v.client_id = c.client_id
) base
GROUP BY seau
ORDER BY ca DESC;
```

Le résultat est identique. Ce qui se dégrade : on lit la requête **de l'intérieur vers l'extérieur**, et la logique « à quoi sert chaque étape » n'est plus nommée. Avec 3 ou 4 niveaux d'imbrication, c'est illisible ; avec des CTE nommées, chaque étape se teste isolément.
</details>

### Exercice 2 : les clients actifs en avril mais pas en mars
Donne le résultat avec trois méthodes.

<details>
<summary>Solution</summary>

```sql
-- 1) EXCEPT
SELECT client_id FROM ventes_avril
EXCEPT
SELECT client_id FROM ventes;

-- 2) NOT EXISTS
SELECT DISTINCT a.client_id
FROM ventes_avril a
WHERE NOT EXISTS (SELECT 1 FROM ventes m WHERE m.client_id = a.client_id);

-- 3) LEFT JOIN + IS NULL
SELECT DISTINCT a.client_id
FROM ventes_avril a
LEFT JOIN ventes m ON a.client_id = m.client_id
WHERE m.vente_id IS NULL;
```

Résultat : **le client 5**. La version `NOT IN` renverrait 0 ligne à cause du `client_id` NULL de la vente 107.
</details>

### Exercice 3 : les 2 plus grosses ventes par catégorie
Sur la table `ventes`, de façon déterministe.

<details>
<summary>Solution</summary>

```sql
WITH classees AS (
  SELECT
    vente_id, categorie, montant,
    ROW_NUMBER() OVER (
      PARTITION BY categorie
      ORDER BY montant DESC NULLS LAST, vente_id
    ) AS rn
  FROM ventes
)
SELECT vente_id, categorie, montant, rn
FROM classees
WHERE rn <= 2
ORDER BY categorie, rn;
```

| vente_id | categorie | montant | rn |
|---|---|---|---|
| 104 | Audio | 150.00 | 1 |
| 108 | Audio | 150.00 | 2 |
| 101 | Informatique | 1200.00 | 1 |
| 106 | Informatique | 1100.00 | 2 |

Le critère `vente_id` départage 104 et 108 (ex æquo à 150). Sans lui, le résultat ne serait pas stable.
</details>

### Exercice 4 : le délai d'activation
Pour chaque client ayant des ventes, calcule le nombre de jours entre son inscription et sa **première vente** (tous statuts confondus).

<details>
<summary>Solution</summary>

Voir la requête de la section 6.2. Résultats : client 1 → 411 jours, client 2 → 369, client 3 → 292, client 4 → 60. Variante « première vente **validée** » : le client 2 passe à **379 jours**, car sa première vente (103) est annulée et la première validée est la 109 du 15 mars.
</details>

### Exercice 5 : la part du CA validé par catégorie
Calcule, pour chaque catégorie, son CA validé et sa part du CA validé total, arrondie à 4 décimales.

<details>
<summary>Solution</summary>

```sql
SELECT
  categorie,
  SUM(montant) AS ca_valide,
  ROUND(SUM(montant) / SUM(SUM(montant)) OVER (), 4) AS part
FROM ventes
WHERE statut = 'VALIDEE'
GROUP BY categorie;
```

| categorie | ca_valide | part |
|---|---|---|
| Informatique | 2650.00 | 0.8983 |
| Audio | 300.00 | 0.1017 |

Le `WHERE` s'exécute avant l'agrégation : le total de référence (2 950) ne contient que les ventes validées.
</details>

---

## 12. Checklist avant de livrer une requête

- [ ] Chaque étape de plus de 10 lignes a sa **CTE nommée** par ce qu'elle fait
- [ ] Aucune CTE n'est référencée plusieurs fois si elle est **coûteuse** (sinon matérialiser)
- [ ] `EXISTS` ou `IN` quand on teste l'existence, **jamais `NOT IN`** sur une colonne nullable
- [ ] `UNION ALL` par défaut, déduplication explicite sur la clé métier
- [ ] Mêmes colonnes dans le même ordre dans chaque branche d'un `UNION`, sans `SELECT *`
- [ ] Toute table de référence est **dédupliquée** (`ROW_NUMBER` + critère de fraîcheur + départage unique) avant d'être jointe
- [ ] `NULLS LAST` / `NULLS FIRST` précisé dans tout `ORDER BY` d'une window function
- [ ] Bornes de dates en intervalle semi-ouvert (`>=` et `<`), fuseau horaire vérifié
- [ ] Pas de fonction sur la colonne de partition dans le filtre
- [ ] `concat_ws` ou `COALESCE` quand une colonne peut être NULL
- [ ] Garde-fous : unicité de la clé, comptes avant/après jointure, réconciliation avec une source de référence
- [ ] Aucun `SELECT DISTINCT` posé pour « réparer » une jointure

---

## 13. Prochain blog et feuille de route

**Blog 3 : window functions avancées.** On sort des bases de la section 8 pour couvrir les patterns que les entretiens et la production exigent : cadres de fenêtre (`ROWS` / `RANGE`), `LAG` / `LEAD`, cumuls, moyennes glissantes, top-N par groupe, *gaps & islands*, sessionisation et *forward fill*. Les tables `clients_dedup` et `ventes_dedup` de la section 9 seront réutilisées.

### Les blogs restants de la série SQL

| # | Titre | Contenu principal | Question d'entretien type |
|---|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique | WHERE vs HAVING, `NOT IN` et NULL |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows | Dédupliquer une table, `EXISTS` vs `JOIN` |
| 3 | Window functions avancées | Frames `ROWS` / `RANGE`, `LAG` / `LEAD`, cumuls, fenêtres glissantes, gaps & islands, sessionisation, forward fill | « Calcule le cumul sur 7 jours glissants », « détecte les sessions » |
| 4 | Types complexes et agrégations avancées | `ARRAY`, `MAP`, `STRUCT`, `LATERAL VIEW EXPLODE`, JSON, `GROUPING SETS` / `ROLLUP` / `CUBE`, pivot et unpivot | « Aplatis ce JSON imbriqué », « sous-totaux et totaux en une requête » |
| 5 | Performance I : lire un plan | `EXPLAIN`, Parquet vs ORC, partitioning, bucketing, statistiques et CBO, petits fichiers | « Ta requête est lente, que fais-tu ? » |
| 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, data skew, AQE, salting, Hive vs Spark vs Impala | « Une clé concentre 80 % des données, que fais-tu ? » |
| 7 | Modélisation | Star schema, tables de faits et dimensions, SCD types 1 / 2 / 3, clés de substitution, Data Vault | « Modélise ce domaine », « implémente un SCD2 en SQL » |
| 8 | Chargements et idempotence | Chargement incrémental, `MERGE`, tables ACID, CDC, backfill, rejouabilité | « Comment rendre ce pipeline rejouable sans doublons ? » |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, tests SQL, observabilité, data contracts | « Comment sais-tu que tes données sont justes ? » |
| 10 | Tech lead | Standards SQL d'équipe, code review, gouvernance, coûts, choix d'architecture et trade-offs | « Comment organises-tu une équipe data autour du SQL ? » |

> Si tu as déjà une série Spark en parallèle (jointures, skew, windows), les blogs 3 et 6 peuvent **fusionner ou se compléter** : le SQL pur d'un côté, l'exécution distribuée et le tuning de l'autre.

### Pour t'entraîner avant le Blog 3

Reprends `ventes_dedup` et écris, **sans regarder** la suite : (1) le CA validé par jour ; (2) le cumul de ce CA depuis le début du mois ; (3) le jour où le CA a le plus progressé par rapport au précédent. Si tu bloques sur la 2 ou la 3, le Blog 3 est fait pour toi.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de l'adapter à ton propre jeu de données et de provoquer toi-même chaque piège.*