---
title: "SQL pour Data Engineers, Blog 10 : tech lead, standards, revue de code, gouvernance, coûts et architecture"
description: "Adopter les pratiques de standards SQL, de revue de code, de gouvernance, de coûts et d'architecture d'un tech lead."
weight: 10
---

# SQL pour Data Engineers, Blog 10 : tech lead, standards, revue de code, gouvernance, coûts et architecture

> **Série** : SQL, de basique à tech lead · **Niveau** : 5/5 (dernier blog) · **Temps de lecture** : ~45 min (avec les travaux pratiques)
> **Moteurs visés** : Hive, Spark SQL, Impala (stack Cloudera). Les écarts de dialecte et de version sont signalés.
> **Prérequis** : [Blogs SQL 1 à 9]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-9-data-quality-tests-contrats" >}}). Ce blog les mobilise tous : chaque standard, chaque question de revue renvoie à un piège déjà rencontré.

> **Ce qui a été vérifié, et ce qui ne l'a pas été.** Les deux travaux pratiques **chiffrés** de ce blog (la revue de code d'une requête fausse, et la pseudonymisation d'identifiants) ont été **exécutés sur SQLite et recoupés en Python** avec les petites tables de la série : **tous les chiffres en proviennent**. Le reste (standards d'équipe, gouvernance, coûts, architecture, gestion d'équipe) relève de **bonnes pratiques et de raisonnement**, pas de mesures : il n'y a **aucun chiffre de performance ni de prix inventé**. Les capacités de **Ranger, Atlas, Iceberg, Impala, Kudu** et des outils de lint ou de CI dépendent de ta version et de ta plateforme : à vérifier chez toi.

---

## 0. Pourquoi ce blog existe

Les neuf blogs précédents t'ont appris à écrire du SQL **juste** (1 à 4), **rapide** (5 et 6), **bien structuré** (7), **rejouable** (8) et **contrôlé** (9). Un tech lead doit en plus faire **écrire du SQL juste à une équipe**, **défendre des choix** devant des gens qui ne sont pas techniques, et **répondre de ce qui se passe en production**.

En entretien « tech lead », on ne te demande presque plus de syntaxe. On te demande :

- *« Tu reprends une équipe de cinq personnes qui écrit du SQL de façon hétérogène. Que fais-tu ? »*
- *« Comment fais-tu une revue de code ? Qu'est-ce que tu regardes en premier ? »*
- *« Hive, Spark ou Impala pour ce besoin ? Justifie. »*
- *« Comment protèges-tu les données personnelles ? »*
- *« Comment réduis-tu les coûts d'une plateforme de données ? »*
- *« Raconte un incident et ce que tu en as tiré. »*

Le scénario :

> **Lundi, 9h00.** Tu viens d'être nommé tech lead d'une équipe de cinq data engineers. Dans la semaine, trois choses t'attendent :
> 1. **Une revue de code** : un collègue te soumet une requête « CA validé de mars par segment ». Elle renvoie **5 100**. La Finance attend **2 950**.
> 2. **Une demande de la direction** : *« Nous voulons ouvrir ces données à 50 analystes, en respectant le RGPD, sans faire exploser la facture. »*
> 3. **Un comité d'architecture** : *« Quelle technologie pour la nouvelle plateforme de reporting ? »*

Trois situations, une seule démarche : **rendre les décisions explicites, vérifiables et réversibles**.

### Plan

1. [Ce qui change quand on devient tech lead](#1-ce-qui-change-quand-on-devient-tech-lead)
2. [Les standards d'équipe](#2-les-standards-déquipe)
3. [La revue de code, en vrai](#3-la-revue-de-code-en-vrai)
4. [La gouvernance et la protection des données](#4-la-gouvernance-et-la-protection-des-données)
5. [Les coûts](#5-les-coûts)
6. [Choisir une architecture et assumer les compromis](#6-choisir-une-architecture-et-assumer-les-compromis)
7. [Exploiter : SLO, incidents et post-mortems](#7-exploiter--slo-incidents-et-post-mortems)
8. [Diriger l'équipe](#8-diriger-léquipe)
9. [Les pièges du tech lead](#9-les-pièges-du-tech-lead)
10. [Résolution du scénario](#10-résolution-du-scénario)
11. [10 questions d'entretien](#11-10-questions-dentretien)
12. [5 exercices](#12-5-exercices)
13. [Checklist du tech lead](#13-checklist-du-tech-lead)
14. [Bilan de la série et suite](#14-bilan-de-la-série-et-suite)

---

## 1. Ce qui change quand on devient tech lead

### 1.1 Trois métiers en un

| Dimension | Ce que tu fais | Question de fond |
|---|---|---|
| **Technique** | Choisir les architectures, garantir la qualité, résoudre les problèmes difficiles | *« Est-ce juste, robuste, maintenable ? »* |
| **Équipe** | Faire progresser les autres, répartir le travail, protéger la concentration | *« L'équipe produit-elle du travail de qualité sans moi ? »* |
| **Parties prenantes** | Traduire le besoin, négocier, dire non, rendre compte | *« Les bonnes choses sont-elles livrées, et le métier le comprend-il ? »* |

Ton **levier** n'est plus ce que *tu* produis, mais ce que *l'équipe* produit grâce à toi. Un tech lead qui écrit toutes les requêtes difficiles lui-même devient un **goulot d'étranglement** ; un tech lead qui ne code plus du tout perd la crédibilité technique. L'équilibre se trouve dans la **revue**, le **cadrage** et les **décisions structurantes**.

### 1.2 Le tech lead, c'est surtout de la clarté

Sur les neuf blogs précédents, **chaque incident** avait la même racine : **une définition implicite**.

| Incident | Définition implicite |
|---|---|
| 3 chiffres de CA (Blog 1) | « Qu'est-ce que le CA ? » |
| 5 100 au lieu de 2 775 (Blog 2) | « Qu'est-ce qu'un client unique ? » |
| Gold 1 305 ou Gold 80 (Blog 7) | « Segment actuel ou à la date de la vente ? » |
| 120 273 de CA (Blog 9) | « Qu'est-ce qu'une donnée valide ? » |

Ton rôle premier est de **faire écrire ces définitions** : dans un standard, un contrat, une revue de code, une décision d'architecture. Le reste du blog en est la déclinaison.

---

## 2. Les standards d'équipe

### 2.1 Pourquoi standardiser

Cinq personnes qui écrivent du SQL cinq façons différentes produisent un code que **personne ne peut relire vite**, et où **les erreurs se cachent**. Un standard n'a pas pour but d'imposer un goût, mais de :

1. **Accélérer la lecture** (on sait où chercher quoi) ;
2. **Éviter les pièges connus** (ceux des neuf blogs précédents) ;
3. **Rendre la revue de code objective** (on discute de la logique, pas de la mise en forme).

### 2.2 Le guide de style SQL

**Mise en forme**

| Règle | Exemple |
|---|---|
| Mots-clés en majuscules, un clause par ligne | `SELECT`, `FROM`, `WHERE`, `GROUP BY` chacun sur sa ligne |
| Une colonne par ligne dans le `SELECT` | Diff lisible lors d'une revue |
| Jointures **explicites** avec alias parlants | `JOIN clients c ON v.client_id = c.client_id`, jamais `FROM a, b WHERE ...` |
| **Qualifier** toutes les colonnes dès qu'il y a une jointure | `v.statut`, `c.segment` |
| Pas de `SELECT *` dans un pipeline (permis pour explorer) | Colonnes listées (Blog 1 et 5) |

**Nommage**

| Objet | Convention | Exemple |
|---|---|---|
| Tables, colonnes | `snake_case`, noms complets, pas d'abréviations obscures | `date_vente`, pas `dt_vte` |
| Couche | Préfixe ou schéma de la couche (Blog 7) | `silver.ventes`, `gold.fait_ventes` |
| Clés | `_id` (clé métier), `_sk` (clé de substitution) | `client_id`, `client_sk` |
| Booléens | `est_...`, `a_...` | `est_courant`, `est_supprime` |
| CTE | Un nom qui dit **ce que contient l'étape** | `ventes_validees`, jamais `t1` |

**Structure d'une requête**

1. Les **CTE** d'abord, une responsabilité par CTE (filtrer, dédupliquer, enrichir, agréger, Blog 2).
2. Un **commentaire** par règle métier non évidente (« `date_vente < fin` : intervalle semi-ouvert »).
3. Les **paramètres** (dates, seuils) en tête, jamais enfouis (« date de traitement » en paramètre, Blog 8).

### 2.3 Les interdits (et d'où ils viennent)

Un bon standard cite **la raison** de chaque interdit : sans elle, c'est de l'arbitraire que personne ne respecte.

| Interdit | Remplacé par | Source |
|---|---|---|
| `FROM a, b WHERE a.id = b.id` (jointure implicite) | `JOIN ... ON` explicite | Blog 1 |
| `NOT IN (sous-requête)` sur une colonne nullable | `NOT EXISTS` ou `LEFT JOIN ... IS NULL` | Blog 1 |
| `statut <> 'X'` sans décision sur le NULL | `IS DISTINCT FROM`, ou `OR statut IS NULL` | Blog 1 |
| `BETWEEN` sur des timestamps | `>= début AND < fin` | Blog 1, 2 |
| `SELECT DISTINCT` pour « réparer » une jointure | Dédupliquer la source, vérifier l'unicité des clés | Blog 1, 2 |
| `SELECT *` dans un pipeline | Colonnes listées | Blog 1, 5 |
| `current_date()` / `current_timestamp()` dans la logique métier | Date en **paramètre** | Blog 7, 8 |
| `rand()` / `uuid()` dans une clé ou un sel | Valeur **déterministe** | Blog 6, 8 |
| `ROW_NUMBER` sans départage unique | `ORDER BY` avec critère unique | Blog 3 |
| `ORDER BY` sans `NULLS FIRST/LAST` quand les NULL comptent | Position des NULL explicite | Blog 1, 3 |
| `INSERT INTO` sans protection contre le rejeu | `INSERT OVERWRITE` de partition ou `MERGE` | Blog 8 |
| `DOUBLE` pour de l'argent | `DECIMAL` | Blog 1 |
| Hint de jointure sans mesure | Statistiques à jour d'abord | Blog 5, 6 |

### 2.4 Le standard ne vaut que s'il est appliqué automatiquement

Un document de 20 pages que personne n'ouvre ne change rien. La règle d'or : **tout ce qui peut être vérifié par une machine ne doit pas dépendre d'un humain.**

| Niveau | Outil / mécanisme | Ce qu'il fait |
|---|---|---|
| **Mise en forme** | Un formateur ou *linter* SQL (par exemple SQLFluff, à évaluer sur ton dialecte) | Applique le style, signale les `SELECT *` et les jointures implicites |
| **Intégration continue** | Exécuter les requêtes sur des **fixtures** et vérifier les résultats attendus (Blog 9, section 8) | Un changement qui casse un cas de test est refusé avant la fusion |
| **Revue de code** | Une **checklist** commune (section 3) | Ce qui exige du jugement |
| **Production** | Les **tests de qualité** à chaque exécution (Blog 9) | Ce qui exige les vraies données |

> **Comment introduire des standards sans braquer l'équipe** : commence par **trois règles** (celles qui ont déjà causé des incidents chez vous), explique la **raison** de chacune avec **l'incident réel**, **automatise** leur vérification, puis ajoute progressivement. Un standard qui naît d'un incident est accepté ; un standard qui naît d'une préférence est contesté.

### 2.5 L'organisation du dépôt et des environnements

```
repo-donnees/
├── bronze/            # ingestion brute (DDL, jobs d'extraction)
├── silver/            # nettoyage, déduplication, dimensions SCD2, tests de qualité
├── gold/              # étoiles, agrégats de reporting
├── tests/             # fixtures + résultats attendus (EXCEPT dans les deux sens)
├── contrats/          # contrats de données (YAML, Blog 9)
└── docs/              # décisions d'architecture (ADR), runbooks
```

Trois principes : **tout est versionné** (DDL, requêtes, contrats, décisions) ; **trois environnements** au minimum (développement, recette, production) avec les **mêmes** données de test en développement (les fixtures) ; **les migrations de schéma sont du code** (scripts versionnés, rejouables), jamais des commandes tapées à la main en production.

### 2.6 La définition de « terminé »

Une livraison n'est « terminée » que si :

- [ ] la **logique** est relue par une autre personne (section 3) ;
- [ ] elle a des **tests** : fixtures avec résultat attendu, cas limites, tests de qualité (Blog 9) ;
- [ ] elle est **rejouable** : un rejeu donne le même résultat (Blog 8) ;
- [ ] elle a des **contrôles de réconciliation** avec une source indépendante (Blog 7, 9) ;
- [ ] les **définitions métier** sont écrites (contrat, commentaire, catalogue) ;
- [ ] la **performance** a été regardée (`EXPLAIN`, Blog 5) pour les grosses tables ;
- [ ] les **droits d'accès** et les **données sensibles** sont traités (section 4) ;
- [ ] un **runbook** existe (que faire si ça casse).

---

## 3. La revue de code, en vrai

### 3.1 Pourquoi on relit

La revue de code n'est pas un contrôle de la personne, c'est un **filet de sécurité de l'équipe** : un deuxième regard attrape ce que l'auteur ne voit plus. Dans une équipe de données, une revue sérieuse coûte **vingt minutes**, et un chiffre faux publié coûte **des jours** (et de la confiance).

### 3.2 Travaux pratiques : la requête de ton collègue

Ton collègue te soumet cette requête, avec le message : *« CA validé de mars par segment client, avec le nombre de clients. Ça tourne, ça donne des résultats. »* Les tables sont celles de la série (`ventes` de mars, `clients_raw` du Blog 2).

```sql
SELECT DISTINCT c.segment, SUM(v.montant) AS ca, COUNT(*) AS nb_clients
FROM ventes v, clients_raw c
WHERE v.client_id = c.client_id
  AND statut <> 'ANNULEE'
  AND date_vente BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY c.segment;
```

**Avant de lire la suite, relis-la et note tous les défauts que tu vois.** (C'est l'exercice « prédis avant de voir » : l'objectif est d'avoir une opinion *avant* ma correction.)

#### Ce que la requête renvoie

| segment | ca | nb_clients |
|---|---|---|
| NULL | 1100.00 | 1 |
| Gold | 1225.00 | 2 |
| Premium | 1375.00 | 4 |
| Standard | 1400.00 | 2 |

Le total fait **5 100**, alors que la vérité (CA validé de mars) est **2 950**. Et la colonne `nb_clients` affiche des valeurs qui **n'ont rien à voir** avec des clients.

#### Les défauts

| # | Défaut | Conséquence | Standard violé (section 2.3) |
|---|---|---|---|
| 1 | **Jointure implicite** (`FROM ventes v, clients_raw c WHERE ...`) | Le type de jointure est caché : c'est un `INNER`, les ventes sans client sont **perdues** | Jointure explicite |
| 2 | **`clients_raw` au lieu d'un référentiel dédupliqué** : les clients 1 et 4 ont deux lignes | Leurs ventes sont comptées **deux fois** | Unicité de la clé (Blog 2) |
| 3 | **`COUNT(*) AS nb_clients`** | Compte des **lignes de jointure**, pas des clients : le nom est faux | `COUNT(DISTINCT client_id)` |
| 4 | **`statut <> 'ANNULEE'`** au lieu de `= 'VALIDEE'` | Le NULL est écarté **par hasard** ; un futur statut `EXPEDIEE` serait inclus à tort | NULL (Blog 1) |
| 5 | **`BETWEEN ... AND '2024-03-31'`** | Correct sur un `DATE`, mais casse sur un `TIMESTAMP` | Intervalle semi-ouvert |
| 6 | **Segment NULL non traité** | Un groupe `NULL` apparaît dans le rapport | `COALESCE(segment, 'INCONNU')` |
| 7 | **Colonnes non qualifiées** (`statut`, `date_vente`) | Fragile : ça casse si une table gagne une colonne du même nom | Qualification |
| 8 | **`SELECT DISTINCT` + `GROUP BY`** | `DISTINCT` est inutile ici et **signale** qu'on a des doublons qu'on n'a pas compris | `DISTINCT` en rustine |
| 9 | **Dates codées en dur** | Non réutilisable, pas de paramètre | Paramètres en tête |
| 10 | **Aucun test, aucune réconciliation** | Personne ne voit le 5 100 avant la Finance | Définition de « terminé » |

#### Où passent les 2 150 d'écart ?

On peut **décomposer** l'écart pas à pas, sans rien deviner (chaque ligne est une variante exécutée) :

| Variante | CA validé de mars | Écart |
|---|---|---|
| **Vérité** (aucune jointure) | **2 950** | |
| Jointure **interne** sur le référentiel **dédupliqué** | 2 775 | **− 175** (ventes 107 et 108 perdues) |
| Jointure interne sur **`clients_raw`** (doublons) | **5 100** | **+ 2 325** (ventes des clients 1 et 4 comptées deux fois) |
| `LEFT JOIN` sur `clients_raw` (doublons seuls) | 5 275 | + 2 325 par rapport à la vérité |

Donc : `2 950 − 175 + 2 325 = 5 100`. **Deux erreurs indépendantes** se compensent partiellement (l'une retire 175, l'autre ajoute 2 325). C'est exactement le genre de situation où un chiffre « presque raisonnable » trompe : **le total ne ressemble pas à une catastrophe**, il est simplement faux.

#### La version corrigée

```sql
-- CA validé d'un mois, par segment actuel du client.
-- Définition : ventes de statut VALIDEE, la date de la vente dans [debut, fin[,
-- rattachées au segment actuel du client ; ventes sans client connu rapportées sous 'INCONNU'.
WITH ventes_mois AS (
  SELECT v.vente_id, v.client_id, v.montant
  FROM ventes v
  WHERE v.statut = 'VALIDEE'
    AND v.date_vente >= DATE '2024-03-01'          -- paramètre : début (inclus)
    AND v.date_vente <  DATE '2024-04-01'          -- paramètre : fin (exclue)
)
SELECT
  COALESCE(c.segment, 'INCONNU')       AS segment,
  COUNT(*)                             AS nb_ventes,
  COUNT(DISTINCT v.client_id)          AS nb_clients,        -- les ventes sans client ne comptent pas ici
  COALESCE(SUM(v.montant), 0)          AS ca
FROM ventes_mois v
LEFT JOIN clients_dedup c ON v.client_id = c.client_id        -- LEFT : ne perd aucune vente
GROUP BY COALESCE(c.segment, 'INCONNU')
ORDER BY segment;
```

| segment | nb_ventes | nb_clients | ca |
|---|---|---|---|
| Gold | 2 | 1 | 1225.00 |
| INCONNU | 2 | 1 | 175.00 |
| Premium | 2 | 1 | 150.00 |
| Standard | 2 | 2 | 1400.00 |

**Total : 8 ventes, 2 950** : la vérité, **ligne par ligne** (les ventes 107 et 108 sont sous `INCONNU`, la 105 a un montant NULL qui explique que Premium n'ait que 150 de CA pour 2 ventes). Pour `INCONNU`, `nb_clients = 1` : la vente 107 a un `client_id` NULL, qui **n'entre pas** dans `COUNT(DISTINCT)` (Blog 1) : c'est une définition à écrire (« clients distincts **identifiés** »).

### 3.3 Comment formuler la revue

Le **fond** compte autant que la **forme**. Une revue qui vexe n'est pas lue ; une revue qui laisse passer l'erreur est inutile.

| À éviter | À préférer |
|---|---|
| « Ton SQL est faux. » | « Ce total vaut 5 100 chez moi, et 2 950 en comptant les ventes validées sans jointure. Je pense que `clients_raw` a des doublons de clé : voici la requête qui le montre. » |
| « Utilise un `LEFT JOIN`. » | « Ce `INNER JOIN` perd 2 ventes (107 et 108, soit 175). Veux-tu les rapporter sous `INCONNU` ? C'est la règle du Blog 7. » |
| « Mauvais nommage. » | « `nb_clients` compte des lignes de jointure. Peux-tu le renommer ou utiliser `COUNT(DISTINCT client_id)` ? » |

Trois principes : **un fait vérifiable** (un chiffre, une requête, un plan) plutôt qu'un jugement ; **une action** claire (quoi faire) ; **une question** quand tu n'es pas sûr (tu peux avoir tort). Distingue aussi ce qui **bloque** (une erreur de logique) de ce qui est une **suggestion** (une préférence de style).

### 3.4 La checklist de revue

Dans l'ordre : on commence par ce qui peut rendre le **résultat faux**, on finit par le style.

| # | Catégorie | Questions à poser | Blog |
|---|---|---|---|
| 1 | **Définition** | La métrique est-elle définie ? Qu'est-ce qu'un client, une vente validée ? | 1, 7 |
| 2 | **Grain et jointures** | Quel est le grain ? La clé de jointure est-elle unique ? Le type de jointure est-il voulu ? Compte-t-on avant / après ? | 1, 2 |
| 3 | **NULL** | Que devient une valeur NULL à chaque filtre, comparaison, jointure, agrégat ? | 1 |
| 4 | **Dates** | Intervalles semi-ouverts ? Dates en paramètre ? Fuseau horaire ? | 1, 2, 7 |
| 5 | **Idempotence** | Un rejeu duplique-t-il ? Ordre des événements, suppressions, filigrane ? | 8 |
| 6 | **Déterminisme** | `rand()`, `current_timestamp()`, `ORDER BY` non unique, `LIMIT` sans tri ? | 3, 6, 8 |
| 7 | **Qualité** | Quels tests ? Quarantaine ? Réconciliation avec une source indépendante ? | 9 |
| 8 | **Performance** | Pruning, colonnes, format, `EXPLAIN` lu ? Skew possible sur la clé de jointure ? | 5, 6 |
| 9 | **Sécurité** | Données sensibles ? Droits d'accès ? Masquage ? | 10 |
| 10 | **Lisibilité** | CTE nommées, commentaires sur les règles, style du guide ? | 2, 10 |

> **Règle d'efficacité** : si tu ne peux relire que **trois** choses, relis **la définition (1), le grain et les jointures (2), et les NULL (3)**. Ce sont les trois sources de la grande majorité des chiffres faux de cette série.

### 3.5 Le modèle de demande de fusion (*pull request*)

Un bon modèle impose à **l'auteur** de faire une partie de la revue :

```
## Ce que fait ce changement
(une phrase : quelle métrique, quelle table)

## Définition métier
(ex. « CA validé = ventes de statut VALIDEE ; segment actuel du client »)

## Comment j'ai vérifié
- [ ] Résultat comparé à une source indépendante : (chiffre attendu, chiffre obtenu)
- [ ] Fixtures et cas limites : (lesquels)
- [ ] Comptes avant / après chaque jointure
- [ ] Rejeu : même résultat

## Risques / ce qui change pour les consommateurs
(colonnes ajoutées, supprimées, unités, définitions)
```

Une demande qui ne contient pas de **chiffre de vérification** n'est pas prête à être relue.

---

## 4. La gouvernance et la protection des données

### 4.1 Les briques de la gouvernance

| Brique | Question | Exemple |
|---|---|---|
| **Catalogue** | Quelles données existent, que signifient-elles, qui en est responsable ? | Descriptions, propriétaires, définitions métier |
| **Lignage** | D'où vient cette colonne, quels rapports en dépendent ? | « Si `montant` est faux, quels dashboards sont touchés ? » |
| **Classification** | Quelles données sont sensibles ? | Données personnelles, financières |
| **Contrôle d'accès** | Qui peut voir quoi (table, colonne, ligne) ? | Un analyste voit les ventes, pas les noms |
| **Cycle de vie** | Combien de temps garde-t-on ? Comment supprime-t-on ? | Durées de conservation, purge |
| **Audit** | Qui a accédé à quoi ? | Journaux d'accès |

Sur une plateforme Cloudera, ces fonctions sont typiquement assurées par des composants comme **Apache Atlas** (catalogue, lignage, classification) et **Apache Ranger** (politiques d'accès, masquage de colonnes, filtres de lignes). Leurs fonctionnalités exactes dépendent de **ta** version et de ta configuration : vérifie-les avant de les promettre.

### 4.2 Classer les colonnes

Commence par **inventorier**. Sur nos tables :

| Colonne | Table | Classification | Traitement |
|---|---|---|---|
| `client_id` | `clients`, `ventes` | **Identifiant** (indirectement identifiant) | Pseudonymiser pour l'analyse |
| `nom` | `clients` | **Donnée personnelle directe** | Restreindre, masquer |
| `pays` | `clients` | Quasi-identifiant (faible) | Libre pour les analystes |
| `segment` | `clients` | Donnée commerciale | Libre |
| `montant`, `statut`, `date_vente` | `ventes` | Donnée commerciale | Libre ; **sensible** si croisée avec un identifiant |

**Le principe de minimisation** : ne donne à chaque population que ce dont elle a **besoin**. Un analyste qui calcule un CA par pays n'a **pas besoin** du nom du client.

### 4.3 Pseudonymisation, anonymisation, masquage

| Technique | Principe | Réversible ? | Les jointures marchent ? | Statut |
|---|---|---|---|---|
| **Masquage** | On cache une partie (`A***`) | Non (la valeur est perdue à l'affichage) | Non | Affichage |
| **Pseudonymisation** | On remplace l'identifiant par un **jeton** (hachage avec secret) | **Oui, par qui détient le secret** | **Oui** | Reste une donnée personnelle |
| **Anonymisation** | On rend la ré-identification **impossible** (agrégation, généralisation) | Non | Non | Hors du champ des données personnelles, **si c'est vraiment irréversible** |

La **pseudonymisation** est l'outil du quotidien : elle protège sans empêcher les analyses (on peut toujours compter des clients, joindre des tables). Mais **la donnée pseudonymisée reste une donnée personnelle** (elle peut être ré-identifiée avec le secret ou par recoupement) : elle réduit le risque, elle ne l'élimine pas.

### 4.4 Travaux pratiques : pseudonymiser et vérifier que les analyses restent justes

On remplace `client_id` par un **jeton** : un hachage **salé** (avec un secret conservé hors des tables).

```sql
-- Le secret (« sel ») est géré par un coffre-fort de secrets, jamais écrit dans le code ou une table.
CREATE TABLE clients_pseudo AS
SELECT
  sha2(concat('sel_secret', '|', CAST(client_id AS STRING)), 256)   AS client_pid,
  pays,
  segment
FROM clients_dedup;                                            -- pas de nom, pas de client_id

CREATE TABLE ventes_pseudo AS
SELECT
  vente_id,
  sha2(concat('sel_secret', '|', CAST(client_id AS STRING)), 256)   AS client_pid,
  montant, statut, date_vente
FROM ventes;
```

Pour les clients 1 à 6, les jetons commencent par (12 premiers caractères, avec le secret de cet exemple) :

| client_id | client_pid (début) |
|---|---|
| 1 | `6b119c48c65e…` |
| 2 | `893b216f7b9e…` |
| 3 | `f5164a556aa5…` |
| 4 | `4cbd1ef853a8…` |
| 5 | `a780d9eb755c…` |
| 6 | `dbb99cb359b0…` |

**Les analyses restent identiques.** Le CA validé par pays, calculé avec les jetons, redonne **exactement** le résultat obtenu avec les identifiants d'origine :

```sql
SELECT c.pays, COUNT(*) AS nb, SUM(v.montant) AS ca
FROM ventes_pseudo v
JOIN clients_pseudo c ON v.client_pid = c.client_pid
WHERE v.statut = 'VALIDEE'
GROUP BY c.pays
ORDER BY c.pays;
```

| pays | nb | ca |
|---|---|---|
| FR | 3 | 2325.00 |
| MA | 2 | 150.00 |
| SN | 1 | 300.00 |

Le résultat est **identique** à celui de la jointure sur les `client_id` (vérifié) : la pseudonymisation **préserve les jointures**, donc les analyses. Et le client `NULL` de la vente 107 reste `NULL` après hachage : il ne joint toujours rien (cohérent avec le Blog 1).

#### Pourquoi le sel est indispensable

Un hachage **sans sel** d'un identifiant à faible entropie (un entier) se **retrouve** par dictionnaire : on hache tous les entiers possibles et on compare.

| Méthode | Identifiants retrouvés (sur les 6 clients) |
|---|---|
| Hachage **sans** sel, dictionnaire de 1 à 999 | **6 sur 6** |
| Hachage **avec** sel secret, même dictionnaire | **0 sur 6** |

Les règles pour la pseudonymisation :

1. **Un sel secret**, stocké **hors** des tables (coffre-fort), avec contrôle d'accès strict.
2. **Un algorithme robuste** (SHA-256 ou mieux, un HMAC), pas de MD5 : `sha2(..., 256)` en Hive ou Spark.
3. **Le même jeton partout** pour pouvoir joindre : le sel doit être **constant** dans le temps (le changer casse la correspondance entre tables).
4. **La table de correspondance** `client_id ↔ jeton`, si elle existe, est **le point sensible** : accès restreint et journalisé.
5. **Rotation du secret** : prévois-la (elle impose de recalculer tous les jetons).

### 4.5 Les accès : colonnes, lignes, vues

| Besoin | Mécanisme | Exemple |
|---|---|---|
| Cacher une colonne | Politique de **masquage** (Ranger) ou vue sans la colonne | L'analyste voit `pays` et `segment`, pas `nom` |
| Limiter les lignes | **Filtre de lignes** (Ranger) ou vue filtrée | Un responsable France ne voit que `pays = 'FR'` |
| Exposer un sous-ensemble | **Vue** sur la couche or | `gold.fait_ventes_analystes` |

```sql
-- Vue d'exposition : pas de nom, identifiant pseudonymisé, seulement les colonnes utiles
CREATE VIEW gold.ventes_analystes AS
SELECT v.vente_id, v.client_pid, c.pays, c.segment, v.montant, v.statut, v.date_vente
FROM ventes_pseudo v
LEFT JOIN clients_pseudo c ON v.client_pid = c.client_pid;
```

Les droits s'accordent **sur la vue**, pas sur les tables sources. Ainsi, on peut **changer** la table source sans changer ce que voient les analystes.

### 4.6 Le droit à l'effacement et le cycle de vie

Le RGPD donne aux personnes un **droit à l'effacement**. Dans une plateforme à plusieurs couches, c'est **plus difficile qu'un `DELETE`** :

| Difficulté | Pourquoi |
|---|---|
| **Les copies** | La donnée existe en bronze, silver, gold, dans des agrégats, des sauvegardes, des exports |
| **Les formats immuables** | Parquet et ORC ne se modifient pas ligne à ligne : il faut réécrire des fichiers (Blog 5, 8) |
| **L'historique** | Une dimension SCD2 (Blog 7) garde les anciennes versions : il faut les purger ou les anonymiser |
| **Le journal brut** | Garder tout le journal CDC (Blog 8) entre en tension avec l'effacement |

Une démarche réaliste : **savoir où se trouve chaque donnée personnelle** (lignage), un **événement de suppression** qui se propage dans toutes les couches (comme le `D` du CDC, Blog 8), une **durée de conservation** par couche, et un **processus testé** (on vérifie réellement qu'une personne n'apparaît plus nulle part). **Consulte le juriste ou le délégué à la protection des données** : ce blog explique la mécanique technique, pas ce que la loi impose dans ton cas.

---

## 5. Les coûts

### 5.1 D'où viennent les coûts

| Poste | Ce qui le fait grimper | Levier (blog) |
|---|---|---|
| **Stockage** | Données dupliquées, formats non compressés, rétention illimitée, petits fichiers | Format colonnaire compressé (5), rétention, compactage (5) |
| **Calcul** | Volume **lu**, shuffles, jointures mal planifiées, skew, requêtes répétées | Pruning, colonnes, broadcast (5, 6) |
| **Réseau** | Shuffles massifs, transferts entre zones | Moins de redistribution (6) |
| **Humain** | Pannes, rejeux manuels, corrections de chiffres, dette technique | Idempotence, qualité, tests (8, 9) |

Le poste **humain** est le plus oublié et souvent le plus lourd : une équipe qui passe deux jours par mois à corriger des chiffres coûte plus qu'un cluster.

### 5.2 La règle : on mesure avant d'optimiser

Comme au Blog 5 : **on ne devine pas, on mesure.** Pour réduire un coût, on doit **savoir quoi**, **qui** le provoque et **à quelle fréquence**.

| Mesure | Question | Source |
|---|---|---|
| **Volume lu** par exécution | Combien d'octets cette requête lit-elle ? | Interface du moteur (*Input Size*) |
| **Fréquence** | Combien de fois par jour / mois ? | Orchestrateur, journaux |
| **Coût par propriétaire** | Quelle équipe, quel rapport ? | Étiquetage des jobs |
| **Part de l'inutile** | Combien est relu inutilement ? | Comparaison volume lu / volume utile |

On parle de **showback** (on montre à chaque équipe ce qu'elle consomme) puis éventuellement de **chargeback** (on le lui facture). Rien que rendre le coût visible change les comportements.

### 5.3 Travaux pratiques : calculer le gain d'un levier

On n'a pas besoin d'un prix pour comparer : le **volume lu par mois** suffit. Reprenons le Blog 5 (table partitionnée par mois, 36 partitions, job qui n'a besoin que de **3 colonnes sur 6** du dernier mois) :

```
volume lu par mois  =  fréquence  ×  volume lu par exécution

Avant (table non partitionnée, toutes colonnes) :     30  ×  V
Après (1 partition sur 36, 3 colonnes sur 6) :        30  ×  V × (1/36) × (3/6)  =  30 × V / 72
```

Le rapport est **1 / 72**, soit environ **1,4 %** du volume lu avant correction, avant même de compter la compression du format colonnaire. Ce calcul est **arithmétique** (il découle de 36 partitions et de 3 colonnes sur 6) : le **vrai** gain se **mesure** ensuite dans l'interface (c'est le tableau « avant / après » du Blog 5).

À remplir pour **tes** jobs les plus gros :

| Job | Fréquence / mois | Volume lu / exécution | Volume lu / mois | Utile ? | Levier envisagé |
|---|---|---|---|---|---|
| | | | | | |

**Le job le plus coûteux n'est pas forcément le plus lourd** : un job léger lancé toutes les cinq minutes peut dépasser un job lourd mensuel. C'est le **produit** fréquence × volume qui compte.

### 5.4 Les leviers, classés par rapport effort / gain

| Levier | Gain typique | Effort | Risque |
|---|---|---|---|
| **Supprimer un job que personne n'utilise** | Total pour ce job | Très faible | Faible (vérifier avec le lignage) |
| **Filtrer et projeter tôt** (partitions, colonnes, `SELECT *` banni) | Très élevé | Faible | Faible |
| **Format colonnaire + compression** | Élevé | Moyen (migration) | Faible |
| **Pré-agréger** les rapports très consultés | Élevé | Moyen | Complexité de maintenance (une couche de plus) |
| **Compacter** les petits fichiers | Moyen | Faible | Faible |
| **Réduire la fréquence** (J+1 plutôt que toutes les heures) | Élevé | Faible | Impact métier : à valider |
| **Rétention** (supprimer ou archiver l'ancien) | Moyen | Faible | Conformité, besoin de l'historique |
| **Dimensionner le cluster** | Variable | Moyen | À faire **après** les leviers ci-dessus |

> **Ordre de bataille** : d'abord **ne plus lire ni calculer ce qui est inutile**, ensuite **lire moins**, ensuite **calculer mieux**, et **en dernier** seulement discuter de la taille du cluster. Ajouter des machines pour compenser un job mal conçu est la solution la plus chère et la moins durable (Blog 5, section 12).

### 5.5 Le coût de la qualité et de la non-qualité

Les tests (Blog 9) consomment du calcul : c'est un coût réel. Mais il se compare au **coût de la non-qualité** : un chiffre faux publié (120 273 au lieu de 278), des heures de diagnostic, un rejeu massif, une perte de confiance. Ne coupe pas les tests pour économiser : **place-les au bon niveau** (tests de ligne simples sur le lot, tests de réconciliation sur des agrégats, tests lourds en échantillon).

---

## 6. Choisir une architecture et assumer les compromis

### 6.1 Il n'y a pas de « meilleure technologie »

La question *« Hive, Spark ou Impala ? »* n'a pas de réponse sans **contexte**. Un tech lead ne répond pas par une préférence, il répond par une **démarche** :

1. **Clarifier le besoin** : qui consomme, quelle latence, quel volume, quelles garanties ?
2. **Lister les contraintes** : budget, compétences de l'équipe, conformité, existant.
3. **Proposer une option**, avec au moins **une alternative** sérieuse.
4. **Expliciter les compromis** : ce qu'on gagne, ce qu'on perd, ce qu'on **accepte**.
5. **Identifier les risques** et un moyen de **revenir en arrière**.
6. **Écrire la décision** (section 6.5).

En entretien, c'est **cette structure** qu'on évalue autant que la technologie citée.

### 6.2 Les questions de cadrage

| Question | Pourquoi elle change l'architecture |
|---|---|
| **Qui** consomme les données (analystes, applications, modèles) ? | Interactif ou batch, droits, volume de requêtes |
| **Quelle fraîcheur** est nécessaire (minutes, heures, J+1) ? | Batch, micro-batch ou flux (6.4) |
| **Quel volume** aujourd'hui et dans 2 ans ? | Partitionnement, formats, dimensionnement |
| **Les données changent-elles** (mises à jour, suppressions) ? | Format transactionnel ou réécriture de partitions (Blog 8) |
| **Quelles contraintes de conformité** ? | Pseudonymisation, accès, rétention (section 4) |
| **Quelle équipe** va maintenir ça ? | Compétences, astreinte, complexité acceptable |
| **Quel budget** et quelle tolérance à la panne ? | Redondance, niveau de service (section 7) |

### 6.3 Les moteurs et formats : à quoi servent-ils ?

Les capacités exactes dépendent de **ta version** et de ta plateforme ; ce tableau donne les rôles **typiques**.

| Composant | Rôle typique | Points forts | Points d'attention |
|---|---|---|---|
| **Hive** (sur Tez) | Transformations SQL **par lot**, tables transactionnelles (ACID, ORC) | SQL large, tables ACID, écosystème mature | Latence de démarrage, moins adapté à l'interactif |
| **Spark SQL** | Transformations **complexes**, code (Scala, Python) mêlé à du SQL, traitement de flux | Souplesse, grande communauté, intégration | Réglages mémoire, skew, petits fichiers (Blog 5, 6) |
| **Impala** | Requêtes **interactives** pour la BI sur Parquet ou Kudu | Faible latence, concurrence | Pas pensé pour les grosses transformations d'ETL ; fonctions de dates et de types spécifiques |
| **Kudu** | Stockage orienté lignes, mises à jour rapides, analyses quasi temps réel | `UPSERT`, lectures aléatoires | Autre système à exploiter, capacité |
| **Parquet / ORC** | Formats de fichiers colonnaires | Lecture analytique efficace (Blog 5) | Pas de mise à jour ligne à ligne |
| **Iceberg** (format de table) | Table transactionnelle ouverte (mises à jour, évolution de schéma, voyage dans le temps) | Partagé entre moteurs, évolution de partitions | Prise en charge **variable selon la version et le moteur** : à valider |

Une architecture courante sur Cloudera : **Spark ou Hive** pour fabriquer les couches silver et gold, **Impala** pour servir la BI sur la couche or en Parquet. Mais la bonne combinaison est celle que **ton équipe sait exploiter** et que **ta version prend en charge** : valide par un **prototype** sur tes données avant de t'engager.

### 6.4 Batch, micro-batch ou flux ?

| | **Batch** (J+1, horaire) | **Micro-batch** (minutes) | **Flux** (secondes) |
|---|---|---|---|
| Fraîcheur | Heures à un jour | Minutes | Secondes |
| Complexité | **Faible** | Moyenne | Élevée |
| Idempotence et rejeu (Blog 8) | Simples | Plus délicats | Difficiles (état, ordre, doublons) |
| Qualité et réconciliation (Blog 9) | Faciles | Possibles | Plus complexes |
| Coût d'exploitation | Faible | Moyen | **Élevé** (processus permanents, astreinte) |

> **Règle** : **commence par le batch**, passe à plus frais **seulement** quand un besoin métier **chiffré** l'exige (« décision commerciale prise dans l'heure »). La fraîcheur n'est pas gratuite : chaque cran de latence en moins multiplie la complexité et le coût.

### 6.5 Écrire la décision : l'ADR

Un **ADR** (*Architecture Decision Record*) est une page qui consigne **une décision** : pourquoi on a choisi ceci, ce qu'on a écarté, ce qu'on accepte. Dans six mois, quand quelqu'un demandera « pourquoi a-t-on fait ça ? », tu auras la réponse.

```
# ADR-001 : <titre court de la décision>

Statut        : proposé / accepté / remplacé par ADR-XXX
Date, auteur  : ...

## Contexte
Le besoin, les contraintes, les hypothèses (chiffrées).

## Décision
Ce qu'on fait, en une ou deux phrases.

## Alternatives considérées
A) ... (pourquoi écartée)   B) ... (pourquoi écartée)

## Conséquences
+ Ce qu'on gagne.
- Ce qu'on perd ou accepte (coût, complexité, limites).
Risques et signaux d'alerte :  ...
Réversibilité : comment revenir en arrière, à quel coût.

## Comment on vérifiera
Mesures / critères de réussite, date de réévaluation.
```

### 6.6 Exemple traité : la plateforme de reporting des ventes

**Les hypothèses de l'exercice** (à remplacer par les vraies) : environ **2 milliards de lignes** d'historique (3 ans), soit environ **1,8 million de lignes par jour** (2 × 10⁹ ÷ 1 096 jours) ; fraîcheur **J+1** ; **50 analystes** en BI interactive ; données personnelles à protéger ; équipe de **5** personnes.

| Besoin | Décision proposée | Alternative écartée | Compromis accepté |
|---|---|---|---|
| Fraîcheur J+1 | **Batch** quotidien | Flux (Kafka + traitement continu) | On renonce à l'actualité « à la minute » pour un pipeline simple à rejouer et à tester |
| Stockage | **Parquet** partitionné **par mois** (~55 M de lignes par partition, Blog 5) | Table non partitionnée | Réécriture de partition à chaque correction |
| Changements à la source | Journal **CDC** brut + application **idempotente** (Blog 8) | Instantané complet quotidien | Plus de logique, mais volume de traitement proportionnel aux changements |
| Historique des clients | Dimension **SCD2** avec clés de substitution (Blog 7) | Type 1 (écrasement) | Dimension plus grosse, jointures avec plage de dates |
| Transformations (silver, gold) | **Spark** ou **Hive** selon les compétences de l'équipe | Tout en Impala | Spark : réglages mémoire et skew à maîtriser |
| BI interactive | **Impala** sur la couche or | Requêter directement Hive | Statistiques à maintenir (`COMPUTE STATS`) |
| Qualité | Tests de ligne, disjoncteur, réconciliation, volume (Blog 9) | Contrôles manuels | Coût de calcul des tests, accepté |
| Conformité | Vue d'exposition **pseudonymisée**, politiques d'accès, durée de rétention (section 4) | Ouverture des tables sources | Maintenance du secret et de la table de correspondance |
| Mises à jour | Réécriture de partitions ; **évaluer** Iceberg ou ACID si les corrections deviennent fréquentes | Tout en ACID dès le départ | Dépend du support dans **ta** version de plateforme |

**Les risques et leurs signaux** : un skew sur une valeur sentinelle (surveiller le ratio Max / Médiane, Blog 6) ; une source qui change d'unité (contrat + test de plage, Blog 9) ; un volume qui grossit plus vite que prévu (suivre le volume lu, section 5).

**Ce qu'on vérifie avant de décider** : un **prototype** sur un mois de données réelles (partitionnement, plans, durées), mesuré dans le tableau « avant / après » du Blog 5. **Une architecture qu'on n'a pas essayée est une hypothèse.**

### 6.7 Les décisions réversibles et irréversibles

| Type | Exemple | Comment décider |
|---|---|---|
| **Réversible** (« porte à double sens ») | Un format de fichier, un nom de colonne dans une couche interne, un seuil d'alerte | **Vite**, sans longue étude : on corrigera |
| **Coûteuse à inverser** | Le schéma de partitionnement, le choix du format de table, la clé de substitution | **Prototype**, ADR, revue par un pair |
| **Quasi irréversible** | Un contrat exposé à des dizaines de consommateurs, des identifiants publiés | **Prudence**, préavis, versionnement |

Dépenser la même énergie sur toutes les décisions est une erreur : **investis l'analyse là où le retour en arrière est coûteux.**

### 6.8 Les anti-patterns d'architecture

- **L'architecture « CV »** : choisir une technologie parce qu'elle est à la mode, pas parce qu'elle répond au besoin.
- **La sur-ingénierie** : un flux temps réel pour un rapport lu une fois par jour.
- **L'optimisation prématurée** : le salting avant d'avoir mesuré un skew (Blog 6).
- **Le choix par habitude** : « on a toujours fait comme ça ».
- **L'absence de décision écrite** : on ne sait plus pourquoi, on n'ose plus rien changer.

---

## 7. Exploiter : SLO, incidents et post-mortems

### 7.1 SLI, SLO, SLA : en termes de données

| Terme | Signification | Exemple pour notre plateforme |
|---|---|---|
| **SLI** (indicateur) | Une **mesure** | Heure de disponibilité du rapport ; taux de lots bloqués |
| **SLO** (objectif) | Un **niveau visé** pour le SLI | « Rapport disponible avant 07h00 dans 99 % des jours » |
| **SLA** (accord) | Un **engagement** envers un client, avec conséquences | « Si le SLO n'est pas tenu, … » |

Les SLO se **négocient** avec le métier et se **mesurent** (les résultats de `dq_resultats`, Blog 9, et les heures de fin de job sont tes SLI). **Un objectif de 100 % n'est pas réaliste** : il impose un coût disproportionné ; on se donne un **budget d'erreur**.

Quelques SLI pour une plateforme de données :

| Dimension | SLI | Source |
|---|---|---|
| **Fraîcheur** | Heure à laquelle la couche or est à jour | Journaux du pipeline, filigrane (Blog 8) |
| **Qualité** | Part des tests de qualité qui passent ; taux de rejet ; taille de la quarantaine | `dq_resultats` (Blog 9) |
| **Fiabilité** | Part des exécutions réussies du premier coup | Orchestrateur |
| **Exactitude** | Écart de réconciliation avec la source | Contrôles C1 à C3 (Blog 8) |

### 7.2 La gestion d'un incident

```
1. DÉTECTER   : alerte automatique (tests, fraîcheur, volume), pas un coup de téléphone de la Finance
2. TRIER      : quel impact ? qui est touché ? quelle gravité ?
3. LIMITER    : stopper l'hémorragie (bloquer le chargement, retirer le rapport, communiquer)
4. CORRIGER   : rejouer avec la logique corrigée (c'est possible parce que c'est idempotent, Blog 8)
5. COMMUNIQUER: dire ce qu'on sait, ce qu'on ignore, quand on aura des nouvelles
6. APPRENDRE  : post-mortem sans recherche de coupable
```

**Communiquer pendant l'incident** compte autant que le résoudre : un message régulier (« voici ce que nous savons, voici la prochaine mise à jour à 14h ») évite que la direction n'imagine pire.

### 7.3 Le post-mortem sans recherche de coupable

Le but n'est pas de **désigner un fautif**, mais de comprendre **pourquoi le système a permis l'erreur**. Si une personne peut casser la production avec une commande, le défaut est dans le système, pas dans la personne.

```
# Post-mortem : <titre>

Résumé (3 phrases)  · Impact (qui, combien de temps, quels chiffres)
Chronologie         · détection, réaction, résolution (avec les heures)
Causes              · immédiate, et surtout contributives (pourquoi n'a-t-on rien vu ?)
Ce qui a bien marché · ce qui a limité les dégâts
Ce qui a mal marché  · ce qui a aggravé ou retardé
Actions             · quoi, qui, pour quand, comment on vérifie (chaque action a un responsable)
```

#### Exemple : l'incident du Blog 9

| Rubrique | Contenu |
|---|---|
| **Résumé** | Le dashboard a affiché un CA validé de **120 273** pour le 9 avril, au lieu d'environ **278**. Le chargement avait « réussi ». |
| **Impact** | Un chiffre faux visible par la direction ; heures de diagnostic ; perte de confiance. |
| **Cause immédiate** | Un lot contenant un montant 100 fois trop grand, un doublon, un montant négatif, une date de 2025 a été chargé sans contrôle. |
| **Causes contributives** | Aucun test de contenu ; aucun test de volume ; aucun disjoncteur ; aucun contrat de données avec la source sur l'unité des montants. |
| **Détection** | Par la Finance, **après** publication : c'est la défaillance principale. |
| **Ce qui a bien marché** | Le chargement est idempotent (Blog 8) : on a pu rejouer sans doublon une fois la logique corrigée. |
| **Actions** | Tests de ligne et disjoncteur (Blog 9) ; test de volume ; contrat avec la source ; fixtures de non-régression. Chaque action : responsable, échéance, critère de vérification. |

Ce qui rend un post-mortem utile : **les actions** sont concrètes, **datées**, **assignées**, et on **vérifie** plus tard qu'elles ont été faites.

### 7.4 Livrer sans casser : changement et retour arrière

| Pratique | Pourquoi |
|---|---|
| **Déployer par la revue + l'intégration continue** | Ce qui n'est pas relu ni testé ne part pas |
| **Tester sur une partition** avant tout l'historique | Limite le rayon d'action |
| **Écriture idempotente + source brute conservée** | Un retour arrière = **rejouer l'ancienne version** (Blog 8) |
| **Annoncer** les changements cassants (contrat versionné, Blog 9) | Les consommateurs s'adaptent |
| **Pas de modification manuelle en production** | Tout passe par du code versionné |

### 7.5 Mesurer l'équipe, pas les personnes

Quelques indicateurs d'une équipe de données saine :

| Indicateur | Question |
|---|---|
| **Délai de livraison** | Entre l'idée et la production, combien de temps ? |
| **Taux d'échec des changements** | Quelle part des livraisons provoque un incident ? |
| **Délai de rétablissement** | Combien de temps pour revenir à la normale ? |
| **Part des incidents détectés par nos tests** (et non par les utilisateurs) | Voit-on les problèmes avant le métier ? |
| **Dette de qualité** | Tests qui échouent en permanence, quarantaine qui grossit |

Ces indicateurs servent à **améliorer le système**, jamais à classer les individus : dès qu'on les utilise pour juger des personnes, elles apprennent à les contourner.

---

## 8. Diriger l'équipe

### 8.1 Les 30 premiers jours

| Semaine | Objectif | Actions |
|---|---|---|
| **1** | **Écouter et cartographier** | Rencontrer chaque personne (que font-ils, qu'est-ce qui les frustre ?), lister les pipelines, les incidents des six derniers mois, les consommateurs |
| **2** | **Trouver les trois sources de douleur** | Où sont les incidents, les rejeux manuels, les chiffres contestés ? Les relier aux pièges de cette série |
| **3** | **Choisir trois règles et un contrôle** | Les trois standards qui auraient évité les incidents (section 2.4), et un premier test automatisé (réconciliation ou unicité) |
| **4** | **Montrer un résultat** | Une première victoire visible, par exemple le rapport qui se réconcilie enfin ; partager avec l'équipe et le métier |

**Ne change pas tout** : une réorganisation massive dès la première semaine détruit la confiance. Les gains rapides **et** visibles construisent la crédibilité.

### 8.2 Faire monter l'équipe en compétence

- **Pair programming et revue croisée** : plus efficaces que des formations théoriques.
- **Des exercices pratiques** comme ceux de cette série : un incident réel, des données minuscules dont on **recalcule les résultats à la main**, un piège **provoqué volontairement**. On retient ce qu'on a vu casser.
- **Des rôles tournants** pour la revue, l'astreinte, l'animation des post-mortems.
- **Une matrice de compétences** (SQL, performance, modélisation, qualité...) pour répartir les sujets et voir les trous.
- **Déléguer des sujets**, pas des tâches : confie la responsabilité complète d'un domaine (par exemple la qualité d'une source) avec le droit de décider.

### 8.3 Déléguer sans abandonner

| Niveau | Tu dis | Quand |
|---|---|---|
| **Fais, et dis-moi avant** | La décision t'est soumise | Personne débutante, enjeu élevé |
| **Fais, et dis-moi après** | Information a posteriori | Compétence confirmée, enjeu moyen |
| **Décide, c'est ton domaine** | Autonomie complète | Personne experte, domaine confié |

Ajuste le niveau selon la **personne** et le **risque** : on peut déléguer pleinement un sujet à faible risque à un junior, et garder un œil sur un sujet critique même confié à un senior.

### 8.4 La dette technique

La dette technique est un **emprunt** : on a livré vite en acceptant un défaut (un `INSERT INTO` non idempotent, un test manquant) avec l'intention de le corriger. Comme une dette financière, elle porte des **intérêts** (incidents, lenteur, peur de toucher au code).

| Pratique | Pourquoi |
|---|---|
| **La rendre visible** : un registre (quoi, où, risque, coût de correction) | Ce qui est invisible n'est jamais priorisé |
| **La prioriser par le risque**, pas par l'ancienneté | Un job critique non rejouable passe avant un style de code |
| **Réserver une part régulière** du temps de l'équipe | Sinon elle ne diminue jamais (la proportion se décide avec ton management) |
| **Ne pas en prendre de nouvelle sans le décider** | « On le fera plus tard » doit être une décision écrite |

### 8.5 Prioriser et dire non

Face à dix demandes, trois questions : **quelle valeur** pour le métier ? **quel effort** (en incluant les tests, le rejeu, le backfill et la qualité, que l'on oublie toujours dans une estimation) ? **quel risque** (pour la production, pour la conformité) ?

Dire non, c'est rarement refuser : c'est **proposer un compromis** :

- *« Pas pour vendredi avec les tests de qualité. Je peux livrer une version limitée vendredi, marquée comme provisoire, et la version complète le jeudi suivant. »*
- *« Le temps réel coûte dix fois plus que le J+1 ; quelle décision exige cette fraîcheur ? »*

### 8.6 Expliquer à un non-technicien

Un directeur ne veut pas entendre « SCD de type 2 » : il veut savoir **pourquoi deux chiffres divergent**. Traduis :

> *« Il y a deux façons légitimes de compter : par le segment qu'Alice a **aujourd'hui** (Gold : 1 305 de CA) ou par le segment qu'elle avait **le jour de la vente** (Gold : 80, car ses 1 225 de mars ont été réalisés quand elle était Premium). Les deux totalisent 3 030. Je vous propose d'afficher les deux colonnes, côte à côte, avec leur définition. »*

Trois règles : **un exemple concret** (une personne, un chiffre) plutôt qu'un concept ; **les deux options** et **ta recommandation** ; **la conséquence pour la décision** du directeur.

### 8.7 Documenter ce qui compte

Pas une documentation exhaustive que personne ne lit. **Ce qui doit être écrit** : les **définitions métier**, les **contrats**, les **décisions** (ADR), les **runbooks** (que faire si ça casse), les **post-mortems**. Le reste vit dans le code, les tests et les commentaires utiles.

---

## 9. Les pièges du tech lead

| # | Piège | Conséquence | Remède |
|---|---|---|---|
| 1 | **Le héros** : tout passer par toi | Goulot d'étranglement, équipe dépendante, épuisement | Déléguer des domaines, documenter, former |
| 2 | **Imposer des standards sans raison** | Contournement, ressentiment | Chaque règle justifiée par un **incident réel**, vérifiée automatiquement |
| 3 | **Revue de code vexante ou laxiste** | Personne ne propose, ou les erreurs passent | Faits vérifiables, actions claires, ordre : logique avant style |
| 4 | **Chercher un coupable** après un incident | On cache les erreurs | Post-mortem sans recherche de fautif |
| 5 | **Choisir une technologie par mode** | Complexité inutile | Besoin, contraintes, prototype, ADR |
| 6 | **Optimiser sans mesurer** | Effort perdu, complexité ajoutée | Mesurer d'abord (Blog 5, 6, section 5) |
| 7 | **Promettre ce que la plateforme ne garantit pas** (par exemple l'atomicité d'une écriture) | Incident en production | Tester le comportement réel sur ta version |
| 8 | **Négliger la conformité** jusqu'à l'audit | Reprise coûteuse | Classification et accès dès la conception (section 4) |
| 9 | **Estimer sans les tests, le rejeu, le backfill** | Retards, qualité sacrifiée | Inclure la qualité dans chaque estimation |
| 10 | **Laisser la dette invisible** | Elle explose en incident | Registre, priorisation par le risque, temps réservé |
| 11 | **Mesurer les personnes** au lieu du système | Les indicateurs sont truqués | Indicateurs d'équipe, utilisés pour améliorer |
| 12 | **Ne plus toucher au code** | Perte de crédibilité technique | Rester proche de la revue, du cadrage et des cas difficiles |
| 13 | **Ne pas dire non** | Surcharge, qualité en chute | Proposer un compromis explicite |

---

## 10. Résolution du scénario

### 10.1 Mission 1 : la revue de code

**Verdict** : la demande de fusion est **refusée en l'état**, avec des commentaires précis. La requête renvoie **5 100** au lieu de **2 950** : deux erreurs indépendantes (− 175 par la jointure interne, + 2 325 par les doublons de `clients_raw`). Les dix défauts de la section 3.2 sont listés ; la version corrigée est fournie et se **réconcilie** avec la vérité (8 ventes, 2 950). Le suivi :

- **un test automatisé** (fixtures, résultat attendu) qui aurait détecté l'écart ;
- **un standard** ajouté au guide (« une jointure sur un référentiel exige une clé unique, vérifiée ») ;
- **un échange** avec l'auteur, centré sur les faits (le chiffre, la requête qui le montre), pas sur la personne.

### 10.2 Mission 2 : ouvrir les données à 50 analystes

| Étape | Action | Section |
|---|---|---|
| 1 | **Classer** les colonnes (identifiant, donnée personnelle, commerciale) | 4.2 |
| 2 | **Exposer une vue pseudonymisée** : pas de nom, `client_pid` salé, seulement les colonnes utiles | 4.4, 4.5 |
| 3 | **Vérifier** que les analyses restent justes (CA par pays identique : 2 325 / 150 / 300) | 4.4 |
| 4 | **Accorder les droits sur la vue** (politiques d'accès de la plateforme), journaliser les accès | 4.5 |
| 5 | **Définir la rétention** par couche et un **processus d'effacement** testé, avec le juriste | 4.6 |
| 6 | **Rendre le coût visible** : volume lu par équipe et par rapport (*showback*) | 5.2 |
| 7 | **Traiter d'abord le superflu** : supprimer les jobs inutilisés, filtrer et projeter tôt, avant de parler de dimensionnement | 5.4 |

### 10.3 Mission 3 : le comité d'architecture

La réponse n'est pas « Spark » ni « Impala » : c'est **l'ADR** de la section 6.6, avec ses hypothèses, ses alternatives, ses compromis et son **prototype de vérification**. En une phrase pour le comité : *« Batch quotidien, Parquet partitionné par mois, CDC rejouable, transformations en Spark ou Hive selon les compétences, BI interactive sur Impala, pseudonymisation à l'exposition : chaque choix a une alternative écartée et un moyen de revenir en arrière. »*

### 10.4 Récapitulatif

| Question | Réponse |
|---|---|
| Que fait-on d'une requête qui donne 5 100 ? | On la décompose (− 175, + 2 325), on la corrige, on la teste, on explique à l'auteur |
| Comment ouvrir les données en sécurité ? | Classer, pseudonymiser (avec un sel secret), exposer par vue, droits sur la vue, effacement testé |
| Comment réduire les coûts ? | Mesurer, supprimer l'inutile, lire moins, calculer mieux, ensuite dimensionner |
| Comment choisir une architecture ? | Besoin, contraintes, option et alternative, compromis, prototype, ADR |
| Comment réagir à un incident ? | Détecter par les tests, limiter, corriger par rejeu, communiquer, post-mortem sans coupable |

---

## 11. 10 questions d'entretien

**1. Tu reprends une équipe de cinq personnes qui écrit du SQL de façon hétérogène. Que fais-tu les trente premiers jours ?**
J'écoute chaque personne, je cartographie les pipelines et les incidents récents, j'identifie les trois principales sources de douleur, puis j'introduis **trois règles** (celles qui auraient évité des incidents réels) avec une vérification **automatique**, et je livre une première victoire visible. Je ne change pas tout d'un coup.

**2. Comment fais-tu une revue de code SQL ? Que regardes-tu en premier ?**
D'abord ce qui peut rendre le **résultat faux** : la définition de la métrique, le grain et les jointures (clés uniques, type de jointure, comptes avant / après), le traitement des NULL. Ensuite les dates, l'idempotence, le déterminisme, la qualité, la performance, la sécurité, et seulement à la fin le style. J'appuie chaque remarque sur un fait vérifiable.

**3. Comment introduis-tu des standards sans braquer l'équipe ?**
En partant d'**incidents réels**, en expliquant la **raison** de chaque règle, en **automatisant** sa vérification (formateur, intégration continue, tests) et en commençant petit. Un standard issu d'un incident est accepté ; un standard issu d'une préférence est contesté.

**4. Hive, Spark ou Impala ?**
Ça dépend du besoin : Hive pour des transformations SQL par lot et des tables transactionnelles, Spark pour des transformations complexes ou du code, Impala pour la BI interactive. Je clarifie la latence, le volume, la concurrence et les compétences, je propose une option avec une alternative, je fais un **prototype**, et je consigne la décision dans un **ADR**.

**5. Batch ou streaming ?**
Je commence par le batch : plus simple à rendre idempotent, à tester et à exploiter. Je ne passe à plus frais que si un besoin métier **chiffré** l'exige, car chaque cran de fraîcheur multiplie complexité et coût.

**6. Comment protèges-tu les données personnelles ?**
Je classe les colonnes, j'applique la **minimisation**, je **pseudonymise** les identifiants avec un hachage salé (le sel est un secret, hors des tables), j'expose des **vues** sans les colonnes sensibles avec les droits accordés sur la vue, je prévois un **effacement** qui se propage dans toutes les couches et une **rétention**. La pseudonymisation reste une donnée personnelle ; l'anonymisation doit être réellement irréversible.

**7. Comment réduis-tu les coûts d'une plateforme de données ?**
Je **mesure** (volume lu × fréquence, par propriétaire), je supprime ce qui est inutilisé, je lis moins (partitions, colonnes, format colonnaire), je calcule mieux (jointures, skew, petits fichiers), et seulement ensuite je discute du dimensionnement. Je n'oublie pas le coût humain (incidents, rejeux).

**8. Raconte un incident et ce que tu en as tiré.**
Un exemple structuré : un CA affiché à 120 273 au lieu d'environ 278 après un chargement « réussi » ; détecté par la Finance, pas par nos tests ; cause : un lot non contrôlé (montant aberrant, doublon, date future) ; ce qui a limité les dégâts : l'idempotence (rejeu sans doublon) ; actions : tests de ligne, disjoncteur, test de volume, contrat avec la source, fixtures de non-régression. Post-mortem **sans recherche de coupable**, actions assignées et vérifiées.

**9. Comment gères-tu la dette technique et les priorités ?**
J'ai un **registre** de dette priorisé par le **risque** (un job critique non rejouable passe avant un problème de style), je réserve une part régulière de temps, et je ne prends pas de nouvelle dette sans décision écrite. Pour prioriser les demandes : valeur, effort (tests, rejeu et backfill inclus), risque ; et je dis non en proposant un compromis.

**10. Comment expliques-tu un écart de chiffres à un directeur ?**
Avec un **exemple concret** et sans jargon : « Alice était Premium en mars et elle est Gold aujourd'hui : ses 1 225 de mars comptent en Premium si on regarde le jour de la vente, en Gold si on regarde son segment actuel ; les deux totalisent 3 030. » Je propose **les deux vues** avec leur définition, et ma recommandation.

---

## 12. 5 exercices

### Exercice 1 : revois ce code
Voici une requête de chargement soumise en revue. Liste tous les problèmes que tu vois.

```sql
INSERT INTO gold.ventes_resume
SELECT client_id, SUM(montant), current_date()
FROM silver.ventes v
WHERE statut NOT IN (SELECT statut FROM exclusions)
GROUP BY client_id;
```

<details>
<summary>Solution</summary>

1. **`INSERT INTO` non idempotent** : un rejeu duplique (Blog 8) → `INSERT OVERWRITE` de partition ou `MERGE`.
2. **`current_date()`** dans les données : un rejeu ne redonne pas le même résultat → date de traitement en **paramètre**.
3. **`NOT IN (sous-requête)`** : si `exclusions.statut` contient un NULL, le résultat est vide (Blog 1) → `NOT EXISTS`.
4. **Pas de liste de colonnes à l'insertion** et colonne `SUM(montant)` **sans alias** : fragile et illisible.
5. **`statut` non qualifié** (aucun alias de table sur `exclusions`).
6. **`SUM(montant)` peut être NULL** (groupe sans montant) → `COALESCE`.
7. **Aucun test, aucune réconciliation** avec la source (Blog 9).
8. **Pas de définition** de la métrique (quels statuts exclus, pourquoi ?).
9. **Aucun contrôle de grain** : une ligne par `client_id`, mais la table de sortie est-elle unique par `(client_id, date)` ?
</details>

### Exercice 2 : le coût d'un job
Le job A s'exécute toutes les 5 minutes et lit 2 Go à chaque fois ; le job B s'exécute une fois par mois et lit 800 Go. Compare le volume lu par mois (30 jours). Quel est l'effet d'une exécution horaire, puis quotidienne, pour A ?

<details>
<summary>Solution</summary>

Job A : 30 × 24 × 12 = **8 640** exécutions × 2 Go = **17 280 Go** (≈ 17,3 To) par mois. Job B : **800 Go** par mois. Le job A lit **21,6 fois** plus que B, alors qu'il est « léger » : c'est le **produit** fréquence × volume qui compte. En exécution **horaire** : 720 × 2 Go = **1 440 Go** (12 fois moins). En exécution **quotidienne** : 30 × 2 Go = **60 Go** (288 fois moins). À valider avec le métier : la fraîcheur réellement nécessaire.
</details>

### Exercice 3 : écris un ADR
Un service demande un tableau de bord actualisé « le plus vite possible » à partir d'un flux de ventes. Écris l'ADR en précisant les questions de cadrage que tu poses.

<details>
<summary>Solution (exemple, pas LA bonne réponse)</summary>

**Questions de cadrage** : « le plus vite possible » = combien (une minute, une heure, J+1) ? Quelle **décision** dépend de cette fraîcheur ? Quel volume ? Quelle tolérance à une donnée provisoire ? **Contexte** : si la réponse est « une décision par jour » : **batch** quotidien (ou horaire). **Décision** : batch horaire avec application idempotente (Blog 8). **Alternatives** : flux continu (écarté : coût et complexité sans besoin métier chiffré). **Conséquences** : + simplicité, rejeu, tests ; − fraîcheur d'une heure. **Réversibilité** : on pourra passer en micro-batch si un besoin chiffré apparaît. **Vérification** : mesurer l'usage réel du tableau de bord à 3 mois. L'important est la **structure**, pas la technologie choisie.
</details>

### Exercice 4 : post-mortem
Rédige les rubriques essentielles du post-mortem de l'incident du Blog 8 (lot rejoué : 26 lignes et un CA de 6 410 au lieu de 13 lignes et 3 205).

<details>
<summary>Solution</summary>

**Résumé** : un `INSERT INTO` rejoué après un plantage à 80 % a dupliqué les 13 clés. **Impact** : CA validé doublé (6 410 au lieu de 3 205). **Cause immédiate** : écriture non idempotente. **Causes contributives** : aucun test d'unicité de clé après chargement ; relance manuelle sans vérification. **Détection** : par la Finance, **pas** par un test : défaillance principale. **Ce qui a bien marché** : le journal brut était conservé, donc recalcul des partitions possible. **Actions** : remplacer l'ajout par `INSERT OVERWRITE` de partition ou `MERGE` ; test d'unicité C1 à chaque exécution ; réconciliation avec la source ; chacune avec un **responsable**, une **échéance** et un critère de vérification.
</details>

### Exercice 5 : refais sans regarder
Revois cette requête : *« clients qui n'ont jamais eu de vente validée »*, sur `clients_dedup` (6 clients) et `ventes_dedup` (13 ventes, dont la 107 avec un `client_id` NULL).

```sql
SELECT nom, pays
FROM clients_dedup
WHERE client_id NOT IN (SELECT client_id FROM ventes_dedup WHERE statut = 'VALIDEE');
```

Que renvoie-t-elle, que devrait-elle renvoyer, et comment la corriges-tu ?

<details>
<summary>Solution</summary>

Elle renvoie **0 ligne** : la sous-requête contient un `client_id` NULL (vente 107), donc `client_id NOT IN (...)` n'est jamais vrai (Blog 1). Elle devrait renvoyer **un client : Farid Haddad (MA)**, le seul sans vente validée (les clients 1 à 5 en ont tous). Corrections équivalentes (même résultat) :

```sql
-- NOT EXISTS
SELECT c.nom, c.pays FROM clients_dedup c
WHERE NOT EXISTS (SELECT 1 FROM ventes_dedup v WHERE v.client_id = c.client_id AND v.statut = 'VALIDEE');

-- ou LEFT JOIN ... IS NULL, ou NOT IN avec « AND client_id IS NOT NULL » dans la sous-requête
```

Dans une revue, signale-le avec le **fait** : « cette requête renvoie 0 ligne chez moi ; la sous-requête contient un NULL ; voici la version corrigée, qui renvoie Farid Haddad ». Ajoute le cas en **fixture** pour qu'il ne revienne pas.
</details>

---

## 13. Checklist du tech lead

- [ ] Les **définitions métier** (CA, client, vente validée, segment) sont écrites et partagées
- [ ] Un **guide de style** existe, chaque règle citant l'**incident** qui la justifie, et il est **vérifié automatiquement**
- [ ] La **revue de code** a une checklist commune, ordonnée du plus grave au plus cosmétique, et un modèle de demande de fusion exigeant un **chiffre de vérification**
- [ ] Les **tests** (fixtures, cas limites, qualité, réconciliation) font partie de la définition de « terminé »
- [ ] Les **colonnes sensibles** sont classées, les identifiants pseudonymisés avec un **sel secret**, les droits accordés sur des **vues**
- [ ] Un **processus d'effacement** et des **durées de rétention** existent, validés avec le juriste
- [ ] Les **coûts** sont mesurés (volume lu × fréquence, par propriétaire) avant d'être optimisés
- [ ] Les choix d'architecture sont **écrits** (ADR) avec alternatives, compromis, réversibilité, et **prototypés**
- [ ] Des **SLO** (fraîcheur, qualité) sont négociés avec le métier et mesurés
- [ ] Les incidents sont détectés par des **tests**, traités avec une communication régulière, puis suivis d'un **post-mortem sans recherche de coupable**
- [ ] La **dette technique** est visible, priorisée par le risque, avec du temps réservé
- [ ] L'**équipe** monte en compétence (revue croisée, exercices pratiques, rôles tournants) et **tu n'es pas le goulot d'étranglement**

---

## 14. Bilan de la série et suite

### 14.1 Ce que la série t'a donné

| # | Blog | Ce que tu sais faire maintenant |
|---|---|---|
| 1 | Les bases | Lire et écrire du SQL sans te faire piéger : NULL, jointures, `WHERE` contre `HAVING` |
| 2 | SQL intermédiaire | Structurer une requête, dédupliquer, combiner des sources |
| 3 | Window functions | Cumuls, glissants, top N, séries, sessions |
| 4 | Types complexes | Aplatir tableaux et JSON, sous-totaux, pivot |
| 5 | Performance I | Lire un plan, choisir format et partitionnement, statistiques |
| 6 | Performance II | Diagnostiquer et corriger un skew de jointure |
| 7 | Modélisation | Étoile, clés de substitution, historisation SCD2 |
| 8 | Chargements | Idempotence, CDC, `MERGE`, backfill |
| 9 | Data quality | Tests, quarantaine, disjoncteur, contrats |
| 10 | Tech lead | Standards, revue, gouvernance, coûts, architecture, équipe |

### 14.2 Un fil rouge : la définition explicite

Chaque incident de la série a eu la **même racine** : **une définition implicite** (le CA, le client unique, le segment, la donnée valide, la clé, l'ordre des événements). Le travail d'un tech lead, c'est de **rendre explicite ce qui est implicite**, de le **vérifier automatiquement**, et de **laisser une trace** (contrat, test, ADR) pour que l'équipe n'ait plus à le redécouvrir par un incident.

### 14.3 Comment consolider (la partie qui compte)

Lire ne suffit pas : **tu retiens ce que tu fais casser.** Plan concret pour les prochaines semaines :

1. **Rejoue les dix blogs sur ton propre cluster**, en reproduisant chaque piège et en notant tes propres chiffres (surtout les tableaux « avant / après » des Blogs 5 et 6).
2. **Reprends un vrai pipeline de ton projet** et passe-le dans la checklist de revue (section 3.4) : tu trouveras des défauts.
3. **Écris un ADR** pour une décision récente de ton équipe, même passée.
4. **Prépare tes histoires d'entretien** (méthode : situation, action, résultat) à partir des incidents de la série : tu en as maintenant neuf, chacun avec sa cause, sa correction et ses chiffres.
5. **Refais les exercices « sans regarder »** une semaine plus tard : c'est la répétition espacée qui fixe.

### 14.4 La suite : les autres technologies

La même méthode (un scénario d'incident, des données minuscules recalculables à la main, des pièges provoqués, des contrôles, l'honnêteté sur ce qui est vérifié) s'applique à **Spark, Scala, Kafka, NiFi, Airflow, HBase** et au reste du stack. Pour chacune : un incident réaliste, un jeu de données sale, un laboratoire local reproductible, et un tableau « avant / après » à remplir avec **tes** mesures.

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
| ✅ 10 | Tech lead | Standards d'équipe, revue de code, gouvernance, coûts, architecture, équipe |

**La série SQL est terminée.**

---

*La meilleure façon de consolider ce blog est de relire, avec la checklist de la section 3.4, une vraie requête de ton projet, de trouver ses défauts, et de rédiger l'ADR d'une décision dont tu n'as jamais écrit la raison.*