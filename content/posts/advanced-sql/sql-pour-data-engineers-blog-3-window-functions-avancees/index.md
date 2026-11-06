---
title: "SQL pour Data Engineers, Blog 3 : window functions avancées"
description: "Maîtriser les window functions avancées avec des exemples Hive, Spark SQL et Impala."
weight: 3
---

# SQL pour Data Engineers, Blog 3 : window functions avancées

> **Série** : SQL, de basique à tech lead · **Niveau** : 3/5 · **Temps de lecture** : ~40 min (avec exécution des exemples)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Les différences de dialecte sont signalées quand elles comptent.
> **Prérequis** : [Blog 1](blog-01-sql-bases.md) et [Blog 2](blog-02-sql-intermediaire.md) (dont l'introduction aux window functions, section 8).

---

## 0. Pourquoi ce blog existe

Les window functions sont **la** partie du SQL qui sépare un profil junior d'un profil confirmé en entretien. Elles servent à :

- calculer des cumuls et des moyennes glissantes ;
- comparer une ligne à la précédente ou à la suivante ;
- classer, dédupliquer, garder le « top N » ;
- détecter des séries consécutives, des sessions, combler des trous.

Presque tous les exercices de *live coding* data engineering sont l'un de ces cas. Le scénario de ce blog les couvre tous :

> **Jeudi, 9h30.** La direction commerciale veut un tableau de bord « santé commerciale » pour demain matin :
> 1. le CA quotidien et son **cumul du mois** ;
> 2. l'**évolution** par rapport à la dernière journée d'activité ;
> 3. une **moyenne glissante** pour lisser le bruit ;
> 4. le **classement des clients** et une classification ABC ;
> 5. les **séries de jours consécutifs** avec ventes ;
> 6. les **sessions** de navigation (avec ou sans achat) ;
> 7. un historique de **prix sans trous**.

Sept demandes, sept patterns de window functions.

### Plan

1. [Les données de ce blog](#1-les-données-de-ce-blog)
2. [L'anatomie complète d'une window function](#2-lanatomie-complète-dune-window-function)
3. [Le catalogue des fonctions](#3-le-catalogue-des-fonctions)
4. [Pattern 1 : comparer à la ligne précédente (`LAG` / `LEAD`)](#4-pattern-1--comparer-à-la-ligne-précédente-lag--lead)
5. [Pattern 2 : les cumuls](#5-pattern-2--les-cumuls)
6. [Pattern 3 : moyennes et sommes glissantes](#6-pattern-3--moyennes-et-sommes-glissantes)
7. [Pattern 4 : top N, ex æquo et classification ABC](#7-pattern-4--top-n-ex-æquo-et-classification-abc)
8. [Pattern 5 : gaps & islands](#8-pattern-5--gaps--islands)
9. [Pattern 6 : la sessionisation](#9-pattern-6--la-sessionisation)
10. [Pattern 7 : le forward fill](#10-pattern-7--le-forward-fill)
11. [Performance et pièges](#11-performance-et-pièges)
12. [Le tableau de bord complet](#12-le-tableau-de-bord-complet)
13. [10 questions d'entretien](#13-10-questions-dentretien)
14. [5 exercices](#14-5-exercices)
15. [Checklist avant de livrer une requête](#15-checklist-avant-de-livrer-une-requête)
16. [Prochain blog et blogs restants](#16-prochain-blog-et-blogs-restants)

---

## 1. Les données de ce blog

On réutilise les résultats du pipeline du Blog 2, **matérialisés en tables** pour ne pas recalculer à chaque exemple, et on ajoute deux petites tables (événements web, prix).

### 1.1 Les tables issues du Blog 2

```sql
USE blog_sql;

-- Référentiel clients dédupliqué (6 lignes)
CREATE TABLE clients_dedup STORED AS PARQUET AS
SELECT client_id, nom, pays, segment, date_inscription
FROM (
  SELECT client_id, nom, pays, segment, date_inscription,
         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_chargement DESC) AS rn
  FROM clients_raw
) t
WHERE rn = 1;

-- Ventes de mars + avril, dédupliquées sur vente_id (13 lignes)
CREATE TABLE ventes_dedup STORED AS PARQUET AS
SELECT vente_id, client_id, produit, categorie, montant, quantite, statut, date_vente
FROM (
  SELECT vente_id, client_id, produit, categorie, montant, quantite, statut, date_vente,
         ROW_NUMBER() OVER (PARTITION BY vente_id ORDER BY date_vente DESC) AS rn
  FROM (
    SELECT vente_id, client_id, produit, categorie, montant, quantite, statut, date_vente FROM ventes
    UNION ALL
    SELECT vente_id, client_id, produit, categorie, montant, quantite, statut, date_vente FROM ventes_avril
  ) u
) t
WHERE rn = 1;
```

### 1.2 Le CA quotidien validé

```sql
CREATE TABLE ca_jour STORED AS PARQUET AS
SELECT
  date_vente                  AS jour,
  COUNT(*)                    AS nb_ventes,
  COALESCE(SUM(montant), 0)   AS ca
FROM ventes_dedup
WHERE statut = 'VALIDEE'
GROUP BY date_vente;
```

Contenu de `ca_jour` (10 jours avec au moins une vente validée) :

| jour | nb_ventes | ca |
|---|---|---|
| 2024-03-01 | 2 | 1225.00 |
| 2024-03-07 | 1 | 150.00 |
| 2024-03-09 | 1 | 0.00 |
| 2024-03-10 | 1 | 1100.00 |
| 2024-03-11 | 1 | 25.00 |
| 2024-03-12 | 1 | 150.00 |
| 2024-03-15 | 1 | 300.00 |
| 2024-04-02 | 1 | 80.00 |
| 2024-04-05 | 1 | 150.00 |
| 2024-04-08 | 1 | 25.00 |

Remarques : le 9 mars correspond à la vente 105 dont le montant est NULL (d'où le `COALESCE` à 0, un choix à documenter). Les jours **sans vente** (le 2, 3, 4... mars) **n'existent pas** dans la table : c'est un détail qui va compter pour les moyennes glissantes (section 6) et les séries (section 8).

### 1.3 Les événements web (sessionisation)

```sql
CREATE TABLE evenements (
  client_id INT,
  ts        TIMESTAMP,
  type_evt  STRING
) STORED AS PARQUET;

INSERT INTO evenements VALUES
  (1, TIMESTAMP '2024-03-10 09:00:00', 'vue_page'),
  (1, TIMESTAMP '2024-03-10 09:10:00', 'ajout_panier'),
  (1, TIMESTAMP '2024-03-10 09:25:00', 'achat'),
  (1, TIMESTAMP '2024-03-10 11:00:00', 'vue_page'),
  (1, TIMESTAMP '2024-03-10 11:20:00', 'vue_page'),
  (2, TIMESTAMP '2024-03-10 09:05:00', 'vue_page'),
  (2, TIMESTAMP '2024-03-10 09:50:00', 'vue_page'),
  (2, TIMESTAMP '2024-03-10 10:10:00', 'achat');
```

### 1.4 L'historique de prix (avec des trous)

```sql
CREATE TABLE prix_produit_jour (
  produit STRING,
  jour    DATE,
  prix    DECIMAL(10,2)
) STORED AS PARQUET;

INSERT INTO prix_produit_jour VALUES
  ('Laptop', DATE '2024-03-01', 1200.00),
  ('Laptop', DATE '2024-03-02', NULL),
  ('Laptop', DATE '2024-03-03', NULL),
  ('Laptop', DATE '2024-03-04', 1150.00),
  ('Laptop', DATE '2024-03-05', NULL),
  ('Souris', DATE '2024-03-01', NULL),
  ('Souris', DATE '2024-03-02', 25.00),
  ('Souris', DATE '2024-03-03', NULL);
```

> **Dialecte** : si le littéral `TIMESTAMP '...'` ou `DATE '...'` est refusé sur ta version, utilise `CAST('...' AS TIMESTAMP)` / `CAST('...' AS DATE)`.

---

## 2. L'anatomie complète d'une window function

Au Blog 2, on a vu `fonction() OVER (PARTITION BY ... ORDER BY ...)`. Voici la forme complète :

```sql
fonction(...) OVER (
  PARTITION BY colonnes          -- 1) les groupes indépendants
  ORDER BY colonnes              -- 2) l'ordre dans chaque groupe
  ROWS | RANGE BETWEEN ... AND ...   -- 3) le cadre (frame) : quelles lignes la fonction « voit »
)
```

Chaque ligne a donc sa **propre fenêtre** : l'ensemble des lignes (dans sa partition, dans le cadre) sur lesquelles la fonction est calculée.

### 2.1 Le cadre (*frame*), la notion qui piège tout le monde

Le cadre définit la fenêtre de **chaque ligne** relativement à sa position.

| Bornes possibles | Signification |
|---|---|
| `UNBOUNDED PRECEDING` | Depuis la première ligne de la partition |
| `n PRECEDING` | `n` lignes (ROWS) ou `n` unités de valeur (RANGE) avant la ligne courante |
| `CURRENT ROW` | La ligne courante |
| `n FOLLOWING` | `n` lignes ou unités après |
| `UNBOUNDED FOLLOWING` | Jusqu'à la dernière ligne de la partition |

```
Partition triée :   L1  L2  L3  L4  L5  L6  L7
Ligne courante :                L4
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW  →  L2 L3 L4
ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  →  L3 L4 L5
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW → L1 L2 L3 L4
```

### 2.2 `ROWS` vs `RANGE`

- **`ROWS`** compte des **lignes physiques** : « les 2 lignes précédentes ».
- **`RANGE`** raisonne sur les **valeurs** de la colonne de tri : « toutes les lignes dont la valeur est comprise entre `v - 6` et `v` », et il inclut toutes les lignes **ex æquo** (les « pairs ») sur la valeur de tri.

### 2.3 Le cadre par défaut : l'origine de bugs silencieux

| Écriture | Cadre appliqué |
|---|---|
| `OVER (PARTITION BY x)` (sans `ORDER BY`) | **Toute la partition** |
| `OVER (PARTITION BY x ORDER BY y)` (sans cadre) | **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`** |

La deuxième ligne est le piège : dès qu'on ajoute un `ORDER BY` sans préciser de cadre, le cadre par défaut est un `RANGE` qui **inclut les ex æquo**. Démonstration sur les premières ventes validées :

```sql
SELECT
  vente_id,
  date_vente,
  montant,
  SUM(montant) OVER (ORDER BY date_vente)                                    AS cumul_range_defaut,
  SUM(montant) OVER (ORDER BY date_vente, vente_id
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cumul_rows
FROM ventes_dedup
WHERE statut = 'VALIDEE'
ORDER BY date_vente, vente_id;
```

| vente_id | date_vente | montant | cumul_range_defaut | cumul_rows |
|---|---|---|---|---|
| 101 | 2024-03-01 | 1200.00 | **1225.00** | 1200.00 |
| 102 | 2024-03-01 | 25.00 | **1225.00** | 1225.00 |
| 104 | 2024-03-07 | 150.00 | 1375.00 | 1375.00 |
| 105 | 2024-03-09 | NULL | 1375.00 | 1375.00 |

Les ventes 101 et 102 ont la même date : avec le cadre `RANGE` par défaut, elles sont des **pairs**, donc chacune « voit » l'autre et affiche le même cumul (1225). Avec `ROWS` et un critère de départage, le cumul progresse ligne par ligne.

> **Règle d'équipe** : écris **toujours le cadre explicitement** (`ROWS BETWEEN ...`) pour un cumul ou une fenêtre glissante. Cela rend l'intention visible et évite l'effet des ex æquo.

---

## 3. Le catalogue des fonctions

### 3.1 Quatre familles

| Famille | Fonctions | Question à laquelle elle répond |
|---|---|---|
| **Classement** | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE(n)` | « Quelle position ? » |
| **Valeur** | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` | « Que vaut la ligne d'avant, d'après, la première, la dernière ? » |
| **Agrégat** | `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` avec `OVER` | « Quel total, quelle moyenne sur cette fenêtre ? » |
| **Distribution** | `PERCENT_RANK`, `CUME_DIST` | « Quelle position relative (en %) ? » |

Les fonctions de **classement** et `LAG` / `LEAD` exigent un `ORDER BY` dans le `OVER`. Elles **ignorent** le cadre : le cadre ne concerne que les fonctions d'agrégat et `FIRST_VALUE` / `LAST_VALUE`.

### 3.2 `LAG` et `LEAD`

```sql
LAG(colonne, n, valeur_par_defaut)    -- la valeur n lignes AVANT (n = 1 par défaut)
LEAD(colonne, n, valeur_par_defaut)   -- la valeur n lignes APRÈS
```

Sans valeur par défaut, la première ligne (pour `LAG`) ou la dernière (pour `LEAD`) renvoie **NULL** : à traiter explicitement.

### 3.3 `FIRST_VALUE` et `LAST_VALUE` : le piège du cadre

Pour le client 3 (ventes validées 104, 105, 202), on veut connaître la première et la dernière vente :

```sql
SELECT
  vente_id,
  date_vente,
  FIRST_VALUE(vente_id) OVER (PARTITION BY client_id ORDER BY date_vente)  AS premiere,
  LAST_VALUE(vente_id)  OVER (PARTITION BY client_id ORDER BY date_vente)  AS derniere_piege,
  LAST_VALUE(vente_id)  OVER (PARTITION BY client_id ORDER BY date_vente
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS derniere_ok
FROM ventes_dedup
WHERE statut = 'VALIDEE' AND client_id = 3
ORDER BY date_vente;
```

| vente_id | date_vente | premiere | derniere_piege | derniere_ok |
|---|---|---|---|---|
| 104 | 2024-03-07 | 104 | **104** | 202 |
| 105 | 2024-03-09 | 104 | **105** | 202 |
| 202 | 2024-04-05 | 104 | 202 | 202 |

`LAST_VALUE` avec le cadre par défaut (jusqu'à la ligne **courante**) renvoie simplement… la ligne courante. Pour obtenir la vraie dernière valeur de la partition, il faut étendre le cadre jusqu'à `UNBOUNDED FOLLOWING`. C'est une question d'entretien très fréquente.

### 3.4 Distribution : `NTILE`

`NTILE(n)` découpe les lignes ordonnées en `n` tranches de taille aussi égale que possible (les premières tranches reçoivent le surplus). On l'utilisera en section 7.

---

## 4. Pattern 1 : comparer à la ligne précédente (`LAG` / `LEAD`)

**Demande n°2 du dashboard : l'évolution du CA par rapport à la dernière journée d'activité.**

```sql
SELECT
  jour,
  ca,
  LAG(ca) OVER (ORDER BY jour)                  AS ca_precedent,
  ca - LAG(ca) OVER (ORDER BY jour)             AS variation
FROM ca_jour
ORDER BY jour;
```

| jour | ca | ca_precedent | variation |
|---|---|---|---|
| 2024-03-01 | 1225.00 | NULL | NULL |
| 2024-03-07 | 150.00 | 1225.00 | -1075.00 |
| 2024-03-09 | 0.00 | 150.00 | -150.00 |
| 2024-03-10 | 1100.00 | 0.00 | 1100.00 |
| 2024-03-11 | 25.00 | 1100.00 | -1075.00 |
| 2024-03-12 | 150.00 | 25.00 | 125.00 |
| 2024-03-15 | 300.00 | 150.00 | 150.00 |
| 2024-04-02 | 80.00 | 300.00 | -220.00 |
| 2024-04-05 | 150.00 | 80.00 | 70.00 |
| 2024-04-08 | 25.00 | 150.00 | -125.00 |

### Les pièges

1. **La première ligne vaut NULL** : une variation de NULL n'est pas une variation de 0. Utilise `LAG(ca, 1, 0)` seulement si « 0 » a un sens métier ; sinon laisse NULL et gère l'affichage.
2. **« Précédent » veut dire « ligne précédente dans la table », pas « jour précédent ».** Ici, le 7 mars est comparé au 1er mars (la dernière journée d'activité), pas au 6. Les jours sans vente ne sont pas dans `ca_jour`. Si la demande est « par rapport à la **veille calendaire** », il faut d'abord générer un calendrier complet (jointure avec une table de dates, Blog 7) et **remplir** les jours vides à 0.
3. **Variation en pourcentage** : `(ca - precedent) / precedent` échoue ou donne NULL dès que `precedent = 0` (le 10 mars). Protège avec `NULLIF(precedent, 0)` : `(ca - LAG(ca) OVER w) / NULLIF(LAG(ca) OVER w, 0)`.
4. **`LAG` sur une partition** : pour comparer chaque client à **sa propre** ligne précédente, il faut `PARTITION BY client_id`. Sans lui, on compare à la ligne d'un autre client.

`LEAD` fonctionne symétriquement : par exemple, `LEAD(jour) OVER (ORDER BY jour)` donne la prochaine journée d'activité, utile pour calculer des durées entre événements.

---

## 5. Pattern 2 : les cumuls

**Demande n°1 : le CA cumulé, global puis par mois.**

```sql
SELECT
  jour,
  ca,
  SUM(ca) OVER (ORDER BY jour
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cumul_global,
  SUM(ca) OVER (PARTITION BY date_format(jour, 'yyyy-MM')
                ORDER BY jour
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cumul_mois
FROM ca_jour
ORDER BY jour;
```

| jour | ca | cumul_global | cumul_mois |
|---|---|---|---|
| 2024-03-01 | 1225.00 | 1225.00 | 1225.00 |
| 2024-03-07 | 150.00 | 1375.00 | 1375.00 |
| 2024-03-09 | 0.00 | 1375.00 | 1375.00 |
| 2024-03-10 | 1100.00 | 2475.00 | 2475.00 |
| 2024-03-11 | 25.00 | 2500.00 | 2500.00 |
| 2024-03-12 | 150.00 | 2650.00 | 2650.00 |
| 2024-03-15 | 300.00 | 2950.00 | 2950.00 |
| 2024-04-02 | 80.00 | 3030.00 | **80.00** |
| 2024-04-05 | 150.00 | 3180.00 | 230.00 |
| 2024-04-08 | 25.00 | 3205.00 | 255.00 |

`PARTITION BY` remet le cumul à zéro à chaque changement de mois. Le dernier `cumul_global` (3 205) est le CA validé total, et le dernier `cumul_mois` de chaque mois (2 950 et 255) retrouve les chiffres du Blog 2 : **un cumul doit toujours se réconcilier avec un total calculé autrement.**

### Les pièges des cumuls

- **Le cadre par défaut `RANGE`** (section 2.3) : avec des ex æquo sur la colonne de tri, deux lignes affichent le même cumul. Écris `ROWS`.
- **Les NULL** : `SUM` ignore les NULL (Blog 1), donc le cumul « saute » la ligne. Ici, on avait `COALESCE`-é le montant en amont.
- **Un cumul sur une grosse table sans `PARTITION BY`** envoie toutes les lignes vers **une seule partition** (section 11). C'est le premier problème de performance des window functions.

---

## 6. Pattern 3 : moyennes et sommes glissantes

**Demande n°3 : lisser le bruit avec une moyenne glissante.**

### 6.1 Glissant par nombre de lignes

```sql
SELECT
  jour,
  ca,
  ROUND(AVG(ca) OVER (ORDER BY jour
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moyenne_3_lignes
FROM ca_jour
ORDER BY jour;
```

| jour | ca | moyenne_3_lignes |
|---|---|---|
| 2024-03-01 | 1225.00 | 1225.00 |
| 2024-03-07 | 150.00 | 687.50 |
| 2024-03-09 | 0.00 | 458.33 |
| 2024-03-10 | 1100.00 | 416.67 |
| 2024-03-11 | 25.00 | 375.00 |
| 2024-03-12 | 150.00 | 425.00 |
| 2024-03-15 | 300.00 | 158.33 |
| 2024-04-02 | 80.00 | 176.67 |
| 2024-04-05 | 150.00 | 176.67 |
| 2024-04-08 | 25.00 | 85.00 |

Deux observations :

1. **Au début, la fenêtre est incomplète.** La première ligne n'a pas 2 lignes précédentes : la moyenne porte sur 1 puis 2 valeurs. Si tu veux une moyenne « uniquement quand la fenêtre est pleine », ajoute `COUNT(*) OVER (même fenêtre) = 3` et masque les autres lignes.
2. **`ROWS` compte des lignes, pas des jours.** La moyenne du 12 mars porte sur les 3 dernières **journées avec vente** (10, 11 et 12), mais celle du 15 mars sur le 11, 12 et 15 : elle « saute » les 13 et 14 mars, sans vente. Sur des données avec trous, ce n'est **pas** une moyenne sur 3 jours calendaires.

### 6.2 Glissant sur une durée calendaire : `RANGE`

Pour « les 7 derniers jours **calendaires** » avec des trous dans les dates, on utilise `RANGE` sur une clé **numérique** : le numéro de jour. Cette astuce est portable (Hive, Spark, Impala), contrairement aux `RANGE` avec intervalles de dates, dont le support varie.

```sql
WITH base AS (
  SELECT
    jour,
    ca,
    datediff(jour, DATE '1970-01-01') AS num_jour      -- un entier croissant d'un jour à l'autre
  FROM ca_jour
)
SELECT
  jour,
  ca,
  SUM(ca) OVER (ORDER BY num_jour
                RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS ca_7_jours
FROM base
ORDER BY jour;
```

| jour | ca | ca_7_jours | Jours couverts par la fenêtre |
|---|---|---|---|
| 2024-03-01 | 1225.00 | 1225.00 | 24/02 → 01/03 |
| 2024-03-07 | 150.00 | 1375.00 | 01/03 → 07/03 |
| 2024-03-09 | 0.00 | 150.00 | 03/03 → 09/03 |
| 2024-03-10 | 1100.00 | 1250.00 | 04/03 → 10/03 |
| 2024-03-11 | 25.00 | 1275.00 | 05/03 → 11/03 |
| 2024-03-12 | 150.00 | 1425.00 | 06/03 → 12/03 |
| 2024-03-15 | 300.00 | 1575.00 | 09/03 → 15/03 |
| 2024-04-02 | 80.00 | 80.00 | 27/03 → 02/04 |
| 2024-04-05 | 150.00 | 230.00 | 30/03 → 05/04 |
| 2024-04-08 | 25.00 | 255.00 | 02/04 → 08/04 |

Le 7 mars, la fenêtre `[1er mars, 7 mars]` inclut bien le 1er (7 jours inclusifs). Le 15 mars, la fenêtre `[9, 15]` n'inclut plus le 7 mars (150), contrairement à la version `ROWS`.

| Besoin | Cadre |
|---|---|
| « Les 3 dernières **lignes** » | `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` |
| « Les 7 derniers **jours** (avec des trous dans les dates) » | `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` sur un numéro de jour |
| « Les 7 derniers jours (données complètes, 1 ligne/jour) » | `ROWS` suffit, **si** la table est complète |

> **La vraie solution aux trous** : générer un calendrier complet (une ligne par jour) et joindre. Alors `ROWS` devient exact, et les jours vides valent 0. C'est ce qu'on fera en modélisation (Blog 7).

---

## 7. Pattern 4 : top N, ex æquo et classification ABC

**Demande n°4 : classer les clients par CA validé.**

### 7.1 Le CA par client

```sql
WITH ca_client AS (
  SELECT c.client_id, c.nom, SUM(v.montant) AS ca
  FROM ventes_dedup v
  JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE'
  GROUP BY c.client_id, c.nom
)
SELECT
  nom, ca,
  ROW_NUMBER() OVER (ORDER BY ca DESC, client_id) AS rn,
  RANK()       OVER (ORDER BY ca DESC)            AS rk,
  DENSE_RANK() OVER (ORDER BY ca DESC)            AS drk,
  NTILE(4)     OVER (ORDER BY ca DESC, client_id) AS quartile
FROM ca_client
ORDER BY rn;
```

| nom | ca | rn | rk | drk | quartile |
|---|---|---|---|---|---|
| Alice Martin | 1305.00 | 1 | 1 | 1 | 1 |
| David Chen | 1100.00 | 2 | 2 | 2 | 1 |
| Bruno Diallo | 300.00 | 3 | 3 | 3 | 2 |
| Chloé Benali | 300.00 | 4 | **3** | **3** | 3 |
| Emma Rossi | 25.00 | 5 | 5 | 4 | 4 |

Bruno et Chloé ont le même CA (300). `NTILE(4)` répartit 5 lignes en 4 tranches de tailles 2, 1, 1, 1 : la première tranche reçoit la ligne en surplus.

### 7.2 « Le top 3 » : quelle définition ?

C'est **la** question de cadrage à poser en entretien comme au métier : que faire des ex æquo à la frontière ?

```sql
-- Version 1 : exactement 3 lignes (coupe les ex æquo, arbitrairement)
WHERE rn <= 3       -- Alice, David, Bruno (Chloé, pourtant à égalité, est exclue)

-- Version 2 : tous les clients dont le rang est <= 3 (garde les ex æquo)
WHERE rk <= 3       -- Alice, David, Bruno, Chloé (4 lignes)
```

Aucune n'est « la bonne » : `ROW_NUMBER` donne un nombre de lignes garanti mais **un classement arbitraire à égalité** (Chloé est exclue uniquement à cause du critère de départage `client_id`), `RANK` est équitable mais le nombre de lignes varie. **Documente la règle.**

### 7.3 Classification ABC (Pareto)

On classe les clients par part cumulée du CA : **A** = ceux qui font les premiers 80 %, **B** = jusqu'à 95 %, **C** = le reste.

```sql
WITH ca_client AS (
  SELECT c.client_id, c.nom, SUM(v.montant) AS ca
  FROM ventes_dedup v
  JOIN clients_dedup c ON v.client_id = c.client_id
  WHERE v.statut = 'VALIDEE'
  GROUP BY c.client_id, c.nom
),
cumul AS (
  SELECT
    client_id, nom, ca,
    SUM(ca) OVER (ORDER BY ca DESC, client_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      / SUM(ca) OVER ()                                         AS part_cumulee
  FROM ca_client
)
SELECT
  nom, ca,
  ROUND(part_cumulee, 4) AS part_cumulee,
  CASE
    WHEN part_cumulee <= 0.80 THEN 'A'
    WHEN part_cumulee <= 0.95 THEN 'B'
    ELSE 'C'
  END AS classe
FROM cumul
ORDER BY ca DESC, client_id;
```

| nom | ca | part_cumulee | classe |
|---|---|---|---|
| Alice Martin | 1305.00 | 0.4307 | A |
| David Chen | 1100.00 | 0.7937 | A |
| Bruno Diallo | 300.00 | 0.8927 | B |
| Chloé Benali | 300.00 | 0.9917 | C |
| Emma Rossi | 25.00 | 1.0000 | C |

Ce qu'il faut savoir expliquer :

- **Le total de référence** (`SUM(ca) OVER ()`) est calculé sur toute la table : `3030`, qui se réconcilie avec les ventes validées de clients connus du Blog 2 (2 775 en mars + 255 en avril).
- **La convention aux frontières** : ici, un client est classé selon sa part cumulée **après** lui. D'autres équipes utilisent la part **avant** lui (le client qui fait franchir le seuil reste dans la classe supérieure). Les deux sont valides, mais il faut **choisir et documenter**.
- **La fragilité des ex æquo** : Bruno et Chloé ont le même CA, mais Chloé tombe en C et Bruno en B uniquement à cause de `client_id` dans le `ORDER BY`. Si le métier juge ça gênant, il faut une règle de regroupement des ex æquo.

---

## 8. Pattern 5 : gaps & islands

**Demande n°5 : les séries de jours consécutifs avec ventes (« îlots »), et les trous entre elles (« gaps »).**

Problème classique d'entretien : repérer des **séquences ininterrompues** dans des données ordonnées (jours d'activité, abonnements actifs, pannes, matchs gagnés d'affilée).

### 8.1 Méthode 1 : date moins numéro de ligne

L'idée : pour des jours **consécutifs**, la date augmente de 1 à chaque ligne et `ROW_NUMBER` aussi. Leur **différence reste constante** dans une série et **change** dès qu'il y a un trou.

```sql
WITH numerotes AS (
  SELECT jour, ROW_NUMBER() OVER (ORDER BY jour) AS rn
  FROM ca_jour
),
groupes AS (
  SELECT jour, rn, date_sub(jour, rn) AS grp
  FROM numerotes
)
SELECT MIN(jour) AS debut, MAX(jour) AS fin, COUNT(*) AS nb_jours
FROM groupes
GROUP BY grp
ORDER BY debut;
```

Résultat intermédiaire de la CTE `groupes` :

| jour | rn | grp = jour − rn |
|---|---|---|
| 2024-03-01 | 1 | 2024-02-29 |
| 2024-03-07 | 2 | 2024-03-05 |
| 2024-03-09 | 3 | **2024-03-06** |
| 2024-03-10 | 4 | **2024-03-06** |
| 2024-03-11 | 5 | **2024-03-06** |
| 2024-03-12 | 6 | **2024-03-06** |
| 2024-03-15 | 7 | 2024-03-08 |
| 2024-04-02 | 8 | 2024-03-25 |
| 2024-04-05 | 9 | 2024-03-27 |
| 2024-04-08 | 10 | 2024-03-29 |

Résultat final :

| debut | fin | nb_jours |
|---|---|---|
| 2024-03-01 | 2024-03-01 | 1 |
| 2024-03-07 | 2024-03-07 | 1 |
| **2024-03-09** | **2024-03-12** | **4** |
| 2024-03-15 | 2024-03-15 | 1 |
| 2024-04-02 | 2024-04-02 | 1 |
| 2024-04-05 | 2024-04-05 | 1 |
| 2024-04-08 | 2024-04-08 | 1 |

La seule vraie série est **du 9 au 12 mars (4 jours)**. Rappel : le 9 mars compte comme jour d'activité parce qu'il contient une vente validée, même si son montant est inconnu.

**Limite de la méthode 1** : elle exige **une seule ligne par jour** (sinon `ROW_NUMBER` avance sans que la date avance : utilise `DENSE_RANK`) et ne gère que « exactement consécutif ».

### 8.2 Méthode 2 : marquer les ruptures avec `LAG`, puis cumuler

Plus générale, et c'est **le même schéma que la sessionisation** (section 9) :

1. calculer l'écart avec la ligne précédente (`LAG`) ;
2. marquer `1` chaque ligne qui **démarre** une nouvelle série ;
3. **cumuler** ces marqueurs : le cumul est l'identifiant de série.

```sql
WITH ecarts AS (
  SELECT jour, datediff(jour, LAG(jour) OVER (ORDER BY jour)) AS ecart
  FROM ca_jour
),
marques AS (
  SELECT jour, ecart,
         CASE WHEN ecart IS NULL OR ecart > 1 THEN 1 ELSE 0 END AS debut_serie
  FROM ecarts
),
series AS (
  SELECT jour, ecart, debut_serie,
         SUM(debut_serie) OVER (ORDER BY jour
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS serie_id
  FROM marques
)
SELECT serie_id, MIN(jour) AS debut, MAX(jour) AS fin, COUNT(*) AS nb_jours
FROM series
GROUP BY serie_id
ORDER BY serie_id;
```

| jour | ecart | debut_serie | serie_id |
|---|---|---|---|
| 2024-03-01 | NULL | 1 | 1 |
| 2024-03-07 | 6 | 1 | 2 |
| 2024-03-09 | 2 | 1 | 3 |
| 2024-03-10 | 1 | 0 | 3 |
| 2024-03-11 | 1 | 0 | 3 |
| 2024-03-12 | 1 | 0 | 3 |
| 2024-03-15 | 3 | 1 | 4 |
| 2024-04-02 | 18 | 1 | 5 |
| 2024-04-05 | 3 | 1 | 6 |
| 2024-04-08 | 3 | 1 | 7 |

Même résultat : 7 séries dont une de 4 jours. L'avantage : le seuil est **paramétrable**. Remplace `ecart > 1` par `ecart > 3` et tu obtiens « une série tolère jusqu'à 2 jours de pause ».

### 8.3 Les trous (« gaps »)

```sql
SELECT
  date_add(precedent, 1)  AS debut_trou,
  date_sub(jour, 1)       AS fin_trou,
  ecart - 1               AS nb_jours_sans_vente
FROM (
  SELECT jour,
         LAG(jour) OVER (ORDER BY jour)                          AS precedent,
         datediff(jour, LAG(jour) OVER (ORDER BY jour))          AS ecart
  FROM ca_jour
) t
WHERE ecart > 1
ORDER BY debut_trou;
```

| debut_trou | fin_trou | nb_jours_sans_vente |
|---|---|---|
| 2024-03-02 | 2024-03-06 | 5 |
| 2024-03-08 | 2024-03-08 | 1 |
| 2024-03-13 | 2024-03-14 | 2 |
| 2024-03-16 | 2024-04-01 | 17 |
| 2024-04-03 | 2024-04-04 | 2 |
| 2024-04-06 | 2024-04-07 | 2 |

Le plus long trou (17 jours, du 16 mars au 1er avril) est un signal à investiguer : vraie absence de ventes ou chargement manquant ? C'est ce genre de requête qui détecte un **pipeline silencieusement cassé** (Blog 9).

---

## 9. Pattern 6 : la sessionisation

**Demande n°6 : regrouper les événements d'un client en sessions, une nouvelle session démarrant après plus de 30 minutes d'inactivité.**

C'est le pattern de la méthode 2 ci-dessus, appliqué à des timestamps :

```sql
WITH ecarts AS (
  SELECT
    client_id, ts, type_evt,
    (unix_timestamp(ts)
       - unix_timestamp(LAG(ts) OVER (PARTITION BY client_id ORDER BY ts))) / 60 AS minutes_depuis_precedent
  FROM evenements
),
marques AS (
  SELECT
    client_id, ts, type_evt, minutes_depuis_precedent,
    CASE WHEN minutes_depuis_precedent IS NULL
           OR minutes_depuis_precedent > 30 THEN 1 ELSE 0 END AS nouvelle_session
  FROM ecarts
),
sessions AS (
  SELECT
    client_id, ts, type_evt, minutes_depuis_precedent, nouvelle_session,
    SUM(nouvelle_session) OVER (PARTITION BY client_id ORDER BY ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
  FROM marques
)
SELECT * FROM sessions ORDER BY client_id, ts;
```

| client_id | ts | type_evt | minutes_depuis_precedent | nouvelle_session | session_id |
|---|---|---|---|---|---|
| 1 | 09:00 | vue_page | NULL | 1 | 1 |
| 1 | 09:10 | ajout_panier | 10 | 0 | 1 |
| 1 | 09:25 | achat | 15 | 0 | 1 |
| 1 | 11:00 | vue_page | **95** | **1** | **2** |
| 1 | 11:20 | vue_page | 20 | 0 | 2 |
| 2 | 09:05 | vue_page | NULL | 1 | 1 |
| 2 | 09:50 | vue_page | **45** | **1** | **2** |
| 2 | 10:10 | achat | 20 | 0 | 2 |

(Tous les événements ont lieu le 10 mars 2024 ; seule l'heure est affichée.)

Puis on agrège par session :

```sql
SELECT
  client_id,
  session_id,
  MIN(ts)                                                   AS debut,
  MAX(ts)                                                   AS fin,
  COUNT(*)                                                  AS nb_evenements,
  (unix_timestamp(MAX(ts)) - unix_timestamp(MIN(ts))) / 60  AS duree_min,
  MAX(CASE WHEN type_evt = 'achat' THEN 1 ELSE 0 END)       AS a_achete
FROM sessions
GROUP BY client_id, session_id
ORDER BY client_id, session_id;
```

| client_id | session_id | debut | fin | nb_evenements | duree_min | a_achete |
|---|---|---|---|---|---|---|
| 1 | 1 | 09:00 | 09:25 | 3 | 25 | 1 |
| 1 | 2 | 11:00 | 11:20 | 2 | 20 | 0 |
| 2 | 1 | 09:05 | 09:05 | 1 | 0 | 0 |
| 2 | 2 | 09:50 | 10:10 | 2 | 20 | 1 |

### Points d'attention

1. **L'identifiant de session n'est unique que par client.** Pour un identifiant global, concatène : `concat(client_id, '-', session_id)`.
2. **Le seuil est strict** : exactement 30 minutes ne coupe pas la session (`> 30`). C'est une règle métier à écrire.
3. **Les ex æquo sur `ts`** : si deux événements ont le même timestamp, `LAG` est non déterministe. Ajoute un identifiant d'événement au `ORDER BY`.
4. **Le skew** : un robot ou un client avec des millions d'événements met tout dans **une seule partition** (section 11). Les identifiants de bots se filtrent en amont.
5. **Une session qui passe minuit** : elle reste une seule session, ce qui est bien l'intérêt de raisonner par écart et non par jour.

---

## 10. Pattern 7 : le forward fill

**Demande n°7 : un historique de prix sans trous : quand le prix est manquant, on reprend la dernière valeur connue.**

### 10.1 Méthode portable : `COUNT` comme compteur de blocs

```sql
WITH groupes AS (
  SELECT
    produit, jour, prix,
    COUNT(prix) OVER (PARTITION BY produit ORDER BY jour
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM prix_produit_jour
)
SELECT
  produit, jour, prix,
  MAX(prix) OVER (PARTITION BY produit, grp) AS prix_rempli
FROM groupes
ORDER BY produit, jour;
```

| produit | jour | prix | grp | prix_rempli |
|---|---|---|---|---|
| Laptop | 2024-03-01 | 1200.00 | 1 | 1200.00 |
| Laptop | 2024-03-02 | NULL | 1 | 1200.00 |
| Laptop | 2024-03-03 | NULL | 1 | 1200.00 |
| Laptop | 2024-03-04 | 1150.00 | 2 | 1150.00 |
| Laptop | 2024-03-05 | NULL | 2 | 1150.00 |
| Souris | 2024-03-01 | NULL | **0** | **NULL** |
| Souris | 2024-03-02 | 25.00 | 1 | 25.00 |
| Souris | 2024-03-03 | NULL | 1 | 25.00 |

**Pourquoi ça marche** : `COUNT(prix)` ignore les NULL (Blog 1). Le compteur n'augmente donc que sur les lignes **avec valeur** : chaque « bloc » est une valeur suivie de ses NULL. Dans chaque bloc, il n'y a qu'une valeur non NULL, et `MAX` la propage à tous.

Selon le moteur, il existe aussi `LAST_VALUE(prix, true)` (Spark) ou `LAST_VALUE(prix) IGNORE NULLS` (SQL standard, support variable selon la version de Hive, Spark et Impala) : vérifie ta plateforme.

### 10.2 Les points d'attention

- **Les premières lignes restent NULL** (Souris au 1er mars) : il n'existe pas de « valeur précédente ». Un *backward fill* éventuel est une décision métier.
- **Combien de temps reporter une valeur ?** Un prix repris 3 jours est raisonnable, 3 mois non. Ajoute une limite d'ancienneté (par exemple, ne pas combler si `datediff(jour, derniere_date_connue) > 7`).
- **Le forward fill masque une panne de collecte.** Garde la colonne d'origine (`prix`) à côté de `prix_rempli`, et ajoute un indicateur `est_impute`.

---

## 11. Performance et pièges

Une window function est coûteuse : elle exige un **shuffle** (regrouper les lignes d'une partition sur une même machine) **puis un tri** dans chaque partition.

| # | Piège | Conséquence | Remède |
|---|---|---|---|
| 1 | **Pas de `PARTITION BY`** | Toutes les lignes sur **une seule partition** : une seule tâche travaille, la mémoire sature. Spark journalise un avertissement du type « No Partition Defined for Window operation » | Toujours partitionner ; si c'est vraiment global, réduire d'abord les données (agréger avant) |
| 2 | **Clé de partition déséquilibrée** (un client avec 80 % des lignes) | Une tâche très longue pendant que les autres attendent (*skew*) | Filtrer les cas aberrants, saler la clé, ou changer de découpage (Blog 6) |
| 3 | **Plusieurs fenêtres différentes** dans la même requête | Un shuffle + un tri **par fenêtre distincte** | Réutiliser la même spécification quand c'est possible ; la clause `WINDOW w AS (...)` améliore la lisibilité (support selon version) |
| 4 | **Cadre `UNBOUNDED PRECEDING` à `UNBOUNDED FOLLOWING`** sur une grosse partition | La partition entière est gardée en mémoire (risque de *spill* sur disque) | Limiter le cadre, ou utiliser un agrégat classique + jointure |
| 5 | **Window function dans un `WHERE`** | Erreur : elle s'exécute après (Blog 1, section 3) | Encapsuler dans une CTE et filtrer dehors (`QUALIFY` n'est pas disponible partout) |
| 6 | **Déduplication « dernière ligne par clé » avec `ROW_NUMBER` sur de très gros volumes** | Shuffle + tri complets | Quand c'est possible, un agrégat `max_by(col, date)` (Spark) ou `max(struct(date, col))` évite le tri complet |
| 7 | **`COUNT(DISTINCT x) OVER (...)`** | Non supporté par Hive ni Spark | `size(collect_set(x) OVER (...))`, avec prudence sur la taille des ensembles |
| 8 | **`ORDER BY` non unique** | Résultat non déterministe (ex : `ROW_NUMBER`, `LAG`) | Ajouter un critère de départage unique |
| 9 | **NULL dans la colonne de tri** | Position variable selon le moteur (Blog 1) | `NULLS FIRST` / `NULLS LAST` explicite |

> **Réflexe** : avant de livrer une requête à window functions sur une grosse table, lis le plan (`EXPLAIN`) et repère le nombre d'`Exchange` et de `Sort`. C'est le sujet du Blog 5.

---

## 12. Le tableau de bord complet

On assemble les demandes 1, 2 et 3 en une requête :

```sql
WITH base AS (
  SELECT jour, ca, date_format(jour, 'yyyy-MM') AS mois
  FROM ca_jour
)
SELECT
  jour,
  ca,
  SUM(ca) OVER (PARTITION BY mois ORDER BY jour
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS cumul_mois,
  ca - LAG(ca) OVER (ORDER BY jour)                                     AS variation,
  ROUND(AVG(ca) OVER (ORDER BY jour
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)     AS moyenne_3_lignes
FROM base
ORDER BY jour;
```

| jour | ca | cumul_mois | variation | moyenne_3_lignes |
|---|---|---|---|---|
| 2024-03-01 | 1225.00 | 1225.00 | NULL | 1225.00 |
| 2024-03-07 | 150.00 | 1375.00 | -1075.00 | 687.50 |
| 2024-03-09 | 0.00 | 1375.00 | -150.00 | 458.33 |
| 2024-03-10 | 1100.00 | 2475.00 | 1100.00 | 416.67 |
| 2024-03-11 | 25.00 | 2500.00 | -1075.00 | 375.00 |
| 2024-03-12 | 150.00 | 2650.00 | 125.00 | 425.00 |
| 2024-03-15 | 300.00 | 2950.00 | 150.00 | 158.33 |
| 2024-04-02 | 80.00 | 80.00 | -220.00 | 176.67 |
| 2024-04-05 | 150.00 | 230.00 | 70.00 | 176.67 |
| 2024-04-08 | 25.00 | 255.00 | -125.00 | 85.00 |

### Le contrôle de réconciliation

```sql
SELECT mois, MAX(cumul_mois) AS dernier_cumul, SUM(ca) AS total_mois
FROM (
  SELECT date_format(jour, 'yyyy-MM') AS mois, ca,
         SUM(ca) OVER (PARTITION BY date_format(jour, 'yyyy-MM') ORDER BY jour
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumul_mois
  FROM ca_jour
) t
GROUP BY mois;
```

→ `2024-03 : 2950 = 2950` et `2024-04 : 255 = 255`. Le cumul du dernier jour de chaque mois **doit** être égal au total du mois : si ce n'est pas le cas, le cadre ou la partition est faux.

### Récapitulatif des demandes

| # | Demande | Pattern | Section |
|---|---|---|---|
| 1 | Cumul du mois | `SUM() OVER (PARTITION BY mois ORDER BY jour ROWS ...)` | 5 |
| 2 | Évolution vs dernière activité | `LAG` | 4 |
| 3 | Moyenne glissante | `AVG() OVER (ROWS BETWEEN 2 PRECEDING ...)` | 6 |
| 4 | Classement et ABC | `RANK`, `NTILE`, part cumulée | 7 |
| 5 | Séries de jours consécutifs | Gaps & islands | 8 |
| 6 | Sessions | `LAG` + marqueur + cumul | 9 |
| 7 | Prix sans trous | `COUNT` + `MAX` OVER | 10 |

---

## 13. 10 questions d'entretien

**1. Quelle différence entre `GROUP BY` et une window function ?**
`GROUP BY` réduit les lignes (une par groupe). Une window function calcule une valeur **sans réduire** le nombre de lignes : chaque ligne garde son identité et reçoit en plus un résultat calculé sur sa fenêtre.

**2. `ROWS` ou `RANGE` ?**
`ROWS` compte des lignes physiques. `RANGE` raisonne sur les valeurs de la colonne de tri et inclut tous les ex æquo de la ligne courante.

**3. Quel est le cadre par défaut avec un `ORDER BY` ?**
`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Conséquence : avec des ex æquo sur la colonne de tri, un cumul donne la même valeur à toutes les lignes ex æquo. On écrit un cadre `ROWS` explicite.

**4. Pourquoi `LAST_VALUE` renvoie-t-il la ligne courante ?**
Parce que le cadre par défaut s'arrête à la ligne courante : la « dernière » valeur visible est la sienne. Il faut étendre le cadre à `UNBOUNDED FOLLOWING`.

**5. Comment calculer un cumul qui repart à zéro chaque mois ?**
`SUM(x) OVER (PARTITION BY mois ORDER BY jour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. Le `PARTITION BY` remet le cumul à zéro.

**6. Comment obtenir une moyenne sur 7 jours calendaires quand il manque des jours ?**
`ROWS` compterait des lignes. Soit on génère d'abord un calendrier complet et on remplit les jours vides, soit on utilise `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` sur un **numéro de jour** (par exemple `datediff(jour, '1970-01-01')`).

**7. Comment gérer les ex æquo dans un « top N » ?**
`ROW_NUMBER` donne exactement N lignes mais départage arbitrairement. `RANK` / `DENSE_RANK` gardent les ex æquo, donc le nombre de lignes varie. C'est une question de définition métier à clarifier avant d'écrire la requête.

**8. Explique le principe des « gaps & islands ».**
On marque les lignes qui démarrent une nouvelle série (écart avec la ligne précédente supérieur à un seuil, via `LAG`), puis on **cumule** ces marqueurs pour obtenir un identifiant de série. Variante pour des jours consécutifs : `date - ROW_NUMBER()` est constant dans une série.

**9. Comment sessionises-tu des événements ?**
`LAG` sur `ts` par utilisateur pour l'écart, marqueur `1` si écart nul ou supérieur au seuil d'inactivité, cumul des marqueurs pour l'identifiant de session, puis agrégation par `(utilisateur, session)`.

**10. Pourquoi une window function sans `PARTITION BY` sur une grosse table est-elle dangereuse ?**
Toutes les lignes doivent être regroupées sur une seule partition : une seule tâche travaille, la mémoire peut saturer et le job devient séquentiel. On partitionne, ou on agrège avant.

---

## 14. 5 exercices

### Exercice 1 : le nombre de jours depuis la dernière activité
Pour chaque ligne de `ca_jour`, calcule le nombre de jours écoulés depuis la précédente journée d'activité.

<details>
<summary>Solution</summary>

```sql
SELECT
  jour,
  datediff(jour, LAG(jour) OVER (ORDER BY jour)) AS jours_depuis_derniere_activite
FROM ca_jour
ORDER BY jour;
```

Résultats dans l'ordre : `NULL, 6, 2, 1, 1, 1, 3, 18, 3, 3`.
</details>

### Exercice 2 : la variation en pourcentage
Calcule `(ca - ca_precedent) / ca_precedent`, arrondie à 4 décimales, sans erreur de division par zéro.

<details>
<summary>Solution</summary>

```sql
SELECT
  jour, ca,
  ROUND((ca - LAG(ca) OVER (ORDER BY jour))
        / NULLIF(LAG(ca) OVER (ORDER BY jour), 0), 4) AS variation_pct
FROM ca_jour
ORDER BY jour;
```

| jour | variation_pct |
|---|---|
| 2024-03-01 | NULL |
| 2024-03-07 | -0.8776 |
| 2024-03-09 | -1.0000 |
| 2024-03-10 | **NULL** (précédent = 0) |
| 2024-03-11 | -0.9773 |
| 2024-03-12 | 5.0000 |
| 2024-03-15 | 1.0000 |
| 2024-04-02 | -0.7333 |
| 2024-04-05 | 0.8750 |
| 2024-04-08 | -0.8333 |

`NULLIF(x, 0)` transforme le zéro en NULL : la division renvoie NULL au lieu d'échouer.
</details>

### Exercice 3 : la plus grosse vente validée de chaque client
Donne, pour chaque `client_id` non NULL, sa plus grosse vente validée. En cas d'égalité, retiens la plus ancienne.

<details>
<summary>Solution</summary>

```sql
WITH classees AS (
  SELECT
    client_id, vente_id, date_vente, montant,
    ROW_NUMBER() OVER (
      PARTITION BY client_id
      ORDER BY montant DESC NULLS LAST, date_vente, vente_id
    ) AS rn
  FROM ventes_dedup
  WHERE statut = 'VALIDEE' AND client_id IS NOT NULL
)
SELECT client_id, vente_id, date_vente, montant
FROM classees
WHERE rn = 1
ORDER BY client_id;
```

| client_id | vente_id | date_vente | montant |
|---|---|---|---|
| 1 | 101 | 2024-03-01 | 1200.00 |
| 2 | 109 | 2024-03-15 | 300.00 |
| 3 | 104 | 2024-03-07 | 150.00 |
| 4 | 106 | 2024-03-10 | 1100.00 |
| 5 | 203 | 2024-04-08 | 25.00 |
| 99 | 108 | 2024-03-12 | 150.00 |

Le client 3 a deux ventes à 150 (104 et 202) : `date_vente` départage et garde la 104. Le client 99 (orphelin du Blog 1) apparaît : filtre-le avec une jointure sur `clients_dedup` si besoin. `NULLS LAST` évite que le montant NULL de la vente 105 ne soit considéré comme la « plus grande ».
</details>

### Exercice 4 : les sessions avec achat
Pour chaque client, calcule le nombre de sessions et la part de sessions avec achat.

<details>
<summary>Solution</summary>

À partir de la CTE `sessions` de la section 9, après l'agrégation par session :

```sql
SELECT
  client_id,
  COUNT(*)       AS nb_sessions,
  AVG(a_achete)  AS taux_sessions_avec_achat
FROM sessions_agregees      -- le résultat de la requête d'agrégation de la section 9
GROUP BY client_id;
```

Résultat : `client 1 → 2 sessions, 0.5` ; `client 2 → 2 sessions, 0.5`. L'`AVG` d'un indicateur 0/1 donne directement le taux.
</details>

### Exercice 5 : les 2 jours de plus fort CA de chaque mois
Sur `ca_jour`, avec `DENSE_RANK`.

<details>
<summary>Solution</summary>

```sql
WITH classes AS (
  SELECT
    date_format(jour, 'yyyy-MM') AS mois, jour, ca,
    DENSE_RANK() OVER (PARTITION BY date_format(jour, 'yyyy-MM') ORDER BY ca DESC) AS rang
  FROM ca_jour
)
SELECT mois, jour, ca, rang
FROM classes
WHERE rang <= 2
ORDER BY mois, rang;
```

| mois | jour | ca | rang |
|---|---|---|---|
| 2024-03 | 2024-03-01 | 1225.00 | 1 |
| 2024-03 | 2024-03-10 | 1100.00 | 2 |
| 2024-04 | 2024-04-05 | 150.00 | 1 |
| 2024-04 | 2024-04-02 | 80.00 | 2 |
</details>

---

## 15. Checklist avant de livrer une requête

- [ ] Chaque window function a un `PARTITION BY` (ou une raison écrite de ne pas en avoir)
- [ ] Le cadre est **écrit explicitement** (`ROWS BETWEEN ...`) pour tout cumul ou fenêtre glissante
- [ ] Le `ORDER BY` du `OVER` est **déterministe** (critère de départage unique) et précise `NULLS FIRST` / `NULLS LAST`
- [ ] `LAST_VALUE` est utilisé avec un cadre étendu à `UNBOUNDED FOLLOWING`
- [ ] Pour les fenêtres glissantes en durée, on a vérifié si la table a des **jours manquants** (`ROWS` vs `RANGE` ou calendrier)
- [ ] Les divisions par une valeur issue de `LAG` sont protégées avec `NULLIF`
- [ ] La règle sur les **ex æquo** d'un top N est écrite
- [ ] Les cumuls se **réconcilient** avec un total calculé autrement
- [ ] Les seuils métier (inactivité d'une session, trou d'une série) sont en paramètre, pas en dur sans commentaire
- [ ] Le plan (`EXPLAIN`) a été lu pour repérer les `Exchange` / `Sort` sur les grosses tables
- [ ] Les imputations (forward fill) gardent la valeur d'origine et un indicateur `est_impute`

---

## 16. Prochain blog et blogs restants

**Blog 4 : types complexes et agrégations avancées.** `ARRAY`, `MAP`, `STRUCT`, `LATERAL VIEW EXPLODE` (indispensable avec Hive et Spark), lecture de JSON, `GROUPING SETS`, `ROLLUP` et `CUBE` pour les sous-totaux, pivot et unpivot.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Les bases | SELECT, WHERE, GROUP BY / HAVING, NULL, CASE, JOIN, ordre logique |
| ✅ 2 | SQL intermédiaire | Sous-requêtes, CTE, ensembles, dates, chaînes, intro windows |
| ✅ 3 | Window functions avancées | Cadres, `LAG` / `LEAD`, cumuls, glissants, top N, ABC, gaps & islands, sessions, forward fill |
| 4 | Types complexes et agrégations avancées | `ARRAY` / `MAP` / `STRUCT`, `EXPLODE`, JSON, `ROLLUP` / `CUBE` / `GROUPING SETS`, pivot |
| 5 | Performance I : lire un plan | `EXPLAIN`, Parquet vs ORC, partitioning, bucketing, statistiques, petits fichiers |
| 6 | Performance II : jointures distribuées et skew | Broadcast, sort-merge, shuffle, skew, AQE, salting |
| 7 | Modélisation | Star schema, SCD 1 / 2 / 3, calendrier, clés de substitution, Data Vault |
| 8 | Chargements et idempotence | Incrémental, `MERGE`, ACID, CDC, backfill, rejouabilité |
| 9 | Data quality et tests | Contraintes, réconciliation, quarantaine, observabilité, data contracts |
| 10 | Tech lead | Standards d'équipe, code review, gouvernance, coûts, trade-offs d'architecture |

### Pour t'entraîner avant le Blog 4

Sans regarder la suite : (1) imagine que la table `ventes` stocke les produits d'une commande dans une colonne `ARRAY<STRING>`. Comment obtiens-tu **une ligne par produit** ? (2) Comment calcules-tu, **en une seule requête**, le CA par catégorie, par statut, **et** les sous-totaux par catégorie et le total général ? Si tu bloques, le Blog 4 répond exactement à ces deux questions.

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de l'adapter à ton propre jeu de données et de provoquer toi-même chaque piège.*