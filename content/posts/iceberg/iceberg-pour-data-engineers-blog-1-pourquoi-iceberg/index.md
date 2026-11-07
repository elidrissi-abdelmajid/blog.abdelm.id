---
title: "Iceberg pour Data Engineers, Blog 1 : pourquoi Iceberg, et audit de ta stack Cloudera"
description: "Comprendre ce qu'Iceberg change par rapport aux tables Hive/Parquet, observer une vraie table Iceberg sur disque, et auditer ce que ta plateforme Cloudera sait faire."
weight: 1
---

# Iceberg pour Data Engineers, Blog 1 : pourquoi Iceberg, et audit de ta stack Cloudera

> **Série** : Iceberg · **Niveau** : 1/4 · **Temps de lecture** : ~40 min (avec les expériences proposées)
> **Moteurs visés** : Hive, Impala, Spark 3 (stack Cloudera CDP). Les écarts de version sont signalés.
> **Prérequis** : [Blogs SQL 1 à 5]({{< relref "/posts/advanced-sql/sql-pour-data-engineers-blog-5-performance-lire-un-plan" >}}), surtout le Blog 5 (formats, partitionnement, petits fichiers, statistiques). Ce blog en reprend les limites et montre ce qu'Iceberg change.

> **À lire avant d'aller plus loin : ce qui a été exécuté, et ce qui ne l'a pas été.**
> **Exécuté** (en local, sans cluster) : la démonstration Python de la section 4, avec **PyIceberg 0.12.0** et **PySpark 3.5.3**. Les arborescences, les extraits de `metadata.json` et les résultats de cette section sont **copiés de ces exécutions**.
> **Non exécuté** : toutes les commandes Hive, Impala et Spark sur CDP (section 6), faute de cluster Cloudera ici. Les syntaxes viennent de la documentation des moteurs : **vérifie-les sur ta version** avant de les lancer.
> **Affirmations propres à Cloudera** (versions, moteurs supportés) : elles viennent de la documentation et de la communauté Cloudera consultées (liens en fin d'article). Elles évoluent avec les versions : **confirme-les sur ton cluster**.

---

## 0. Pourquoi ce blog existe

Au Blog 5 de la série SQL, on a corrigé un job lent en jouant sur le format, le partitionnement, les statistiques et les petits fichiers. Tout cela repose sur une idée simple : **une table Hive/Parquet, c'est un dossier de fichiers plus une entrée dans le métastore**. Ça marche, mais cette simplicité a un prix. Voici un scénario qui illustre où ça casse (illustratif) :

> **Mercredi, 14h02.** Le job d'ingestion remplace les données de plusieurs partitions de `ventes_hist`. À 14h03, une analyste relance son tableau de bord et obtient un total **trop bas**. À 14h05, le total redevient normal. Personne n'a rien cassé : elle a simplement lu la table **pendant** l'écriture.
>
> **Jeudi.** L'équipe source renomme la colonne `montant` en `montant_ht`. Les nouvelles partitions suivent le nouveau nom, les anciennes gardent l'ancien : le rapport affiche des **NULL** sur trois ans d'historique.
>
> **Vendredi.** Le métier veut un partitionnement **par jour** au lieu de **par mois**. Il faut réécrire toute l'historique.

Trois problèmes, une même racine : **la table n'a pas de véritable définition indépendante de ses fichiers**. Apache Iceberg ajoute cette définition : une couche de métadonnées qui dit, de façon atomique, **quels fichiers composent la table à un instant donné**, avec quel schéma et quel partitionnement.

À la fin de ce blog, tu sauras :

1. **Pourquoi** les tables Hive/Parquet ont ces limites, et ce qu'Iceberg change pour chacune ;
2. **Ce qu'est Iceberg** (et ce qu'il n'est pas) : la structure en couches, de façon concrète ;
3. **Observer** une vraie table Iceberg sur disque, avec ses snapshots, ses manifests et son `metadata.json` ;
4. **Ce que Cloudera supporte** d'après sa documentation, et comment **auditer** ta propre plateforme ;
5. **Quand ne pas** utiliser Iceberg.

### Plan

1. [Le contexte : une table Hive est une convention](#1-le-contexte--une-table-hive-est-une-convention)
2. [Qu'est-ce qu'Iceberg ?](#2-quest-ce-quiceberg-)
3. [Les cinq idées à retenir](#3-les-cinq-idées-à-retenir)
4. [Démonstration locale : voir une table Iceberg sur disque](#4-démonstration-locale--voir-une-table-iceberg-sur-disque)
5. [Iceberg sur Cloudera : ce que disent les sources](#5-iceberg-sur-cloudera--ce-que-disent-les-sources)
6. [Audit de ta stack Cloudera (laboratoire)](#6-audit-de-ta-stack-cloudera-laboratoire)
7. [Quand ne pas utiliser Iceberg](#7-quand-ne-pas-utiliser-iceberg)
8. [Les pièges d'adoption](#8-les-pièges-dadoption)
9. [10 questions d'entretien](#9-10-questions-dentretien)
10. [5 exercices](#10-5-exercices)
11. [Checklist de départ](#11-checklist-de-départ)
12. [Prochain blog et blogs restants](#12-prochain-blog-et-blogs-restants)
13. [Sources à vérifier](#13-sources-à-vérifier)

---

## 1. Le contexte : une table Hive est une convention

Reprends la table du Blog 5 SQL :

```
/warehouse/ventes_hist_part/
   mois=2024-02/   part-0000.parquet  part-0001.parquet ...
   mois=2024-03/   part-0000.parquet  part-0001.parquet ...
```

Ce que le moteur **sait** de cette table vient de deux sources : le **métastore** (nom, colonnes, liste des partitions) et le **contenu des dossiers** (qu'il découvre en les **listant**). Il n'existe pas de document qui dise « voici exactement les fichiers de cette table, à cet instant ». C'est une convention : « les fichiers qui sont dans ce dossier font la table ».

### 1.1 Les sept limites de cette convention

| # | Limite | Symptôme | Ce qu'Iceberg change |
|---|---|---|---|
| 1 | **Lecture = listing de dossiers** | Planification lente avec des milliers de partitions et de fichiers (appels au NameNode ou au stockage objet, au métastore) | Les fichiers sont listés dans des **manifests** : le planificateur lit des métadonnées, pas des dossiers |
| 2 | **Pas de commit atomique sur plusieurs fichiers** | Un lecteur voit un état **intermédiaire** pendant une écriture multi-partitions ; sur stockage objet, un renommage n'est pas atomique | Un **commit** remplace atomiquement un pointeur dans le catalogue : on voit l'ancien état **ou** le nouveau, jamais un mélange |
| 3 | **Schéma lié aux noms (ou aux positions)** | Un renommage de colonne en amont donne des `NULL` sur les anciens fichiers (voir 4.5) | Chaque colonne a un **identifiant** : renommer ou réordonner est sûr |
| 4 | **Partitionnement figé et exposé** | L'utilisateur doit filtrer sur la colonne de partition (Blog 5 SQL, 7.5) ; changer le partitionnement = tout réécrire | **Partitionnement caché** et **évolutif** (Blog 5 de cette série) |
| 5 | **Pas d'historique** | Pas de retour en arrière simple après une mauvaise écriture | **Snapshots** immuables : *time travel*, *rollback* (Blog 6) |
| 6 | **Modifications ligne à ligne limitées** | `UPDATE` / `DELETE` / `MERGE` : transactionnel Hive (ORC) ou réécriture de partitions | Tables **V2** : fichiers de suppression, `MERGE` (Blog 7) |
| 7 | **Un dossier, plusieurs moteurs, plusieurs interprétations** | Chaque moteur applique ses propres règles de découverte de partitions, de types, de statistiques | Une **spécification commune** que les moteurs implémentent |

> **Ce qu'Iceberg ne règle pas tout seul** : les **petits fichiers**, les **statistiques** et le **mauvais choix de partitionnement** restent ton travail (Blogs 8 et 9). Iceberg donne les outils pour les traiter proprement ; il ne les fait pas disparaître.

### 1.2 Nuance sur la limite n°2

Sur HDFS, remplacer **un** dossier se fait généralement par un renommage atomique, ce qui limite les dégâts au niveau d'une partition. Le problème apparaît quand une écriture touche **plusieurs** partitions ou **plusieurs** étapes, ou quand le stockage est un **stockage objet** où le renommage est en réalité une copie suivie d'une suppression. Dans ces cas, le lecteur peut voir un état mélangé. C'est exactement le scénario de 14h02.

---

## 2. Qu'est-ce qu'Iceberg ?

**Apache Iceberg** est une **spécification de format de table** ouverte. Retiens la distinction :

| Notion | Rôle | Exemples |
|---|---|---|
| **Format de fichier** | Comment les lignes sont stockées dans un fichier | Parquet, ORC, Avro |
| **Format de table** | Comment un ensemble de fichiers forme **une table** : schéma, partitionnement, historique, commits | **Iceberg** (et, dans le même espace, Delta Lake et Hudi) |
| **Moteur** | Qui exécute les requêtes | Hive, Impala, Spark, Flink |
| **Catalogue** | Où l'on retrouve une table par son nom | Hive Metastore, d'autres catalogues |

Iceberg est un **format de table** : les **données** restent dans des fichiers Parquet (ou ORC, Avro). Il n'est ni un moteur, ni un format de fichier, ni un catalogue.

### 2.1 La structure en couches

```
Catalogue (ex. Hive Metastore)
   │   garde un POINTEUR : « le metadata.json courant de la table X »
   ▼
metadata.json              schémas (avec identifiants de colonnes), partitionnements,
   │                       liste des snapshots, propriétés
   │   (pointe vers le snapshot courant)
   ▼
manifest list  (snap-*.avro)    la liste des manifests d'UN snapshot
   ▼
manifests      (*-m0.avro)      la liste des fichiers de données + statistiques par fichier
   ▼
fichiers de données             Parquet / ORC / Avro   (+ fichiers de suppression en V2)
```

Un **commit** crée de nouveaux fichiers de métadonnées, puis **remplace atomiquement le pointeur** du catalogue. Les anciens fichiers de métadonnées restent : c'est ce qui permet de revenir en arrière.

### 2.2 Les versions du format

La spécification a des **versions de format** (V1, V2, et d'autres plus récentes en amont). V1 couvre les tables à fichiers de données immuables ; V2 ajoute les **suppressions ligne à ligne**. **La version de format supportée par ta plateforme est une information à auditer** (section 6), pas à supposer.

### 2.3 Ce qu'Iceberg n'est pas

- **Pas un moteur** : il faut Hive, Impala, Spark, Flink ou autre pour l'utiliser.
- **Pas un nouveau format de fichier** : tes données restent en Parquet ou ORC.
- **Pas magique sur la performance** : il rend la planification et les commits plus fiables et plus rapides dans beaucoup de cas, mais un mauvais partitionnement ou des milliers de petits fichiers restent un problème.
- **Pas un catalogue** : il en a besoin. Sur Cloudera, d'après la documentation, c'est par défaut le **HiveCatalog** (le métastore Hive).

---

## 3. Les cinq idées à retenir

1. **Une table est une liste explicite de fichiers**, pas un dossier. Un fichier non référencé n'existe pas pour la table.
2. **Un snapshot est un état complet et immuable** de cette liste à un instant donné.
3. **Un commit est atomique** : il remplace un pointeur. Pas d'état intermédiaire visible, et un conflit entre deux écrivains se détecte au moment du commit (concurrence optimiste, Blog 7).
4. **Les colonnes ont des identifiants** : le nom est un simple libellé, ce qui rend le renommage et le réordonnancement sûrs.
5. **Le partitionnement est une propriété de la table**, pas une convention de dossiers : l'utilisateur n'a pas à connaître la colonne de partition pour en profiter.

---

## 4. Démonstration locale : voir une table Iceberg sur disque

Cette section **a été exécutée** (PyIceberg 0.12.0, PySpark 3.5.3, Python 3.12). Elle ne demande aucun cluster : tu peux la refaire sur ton poste.

> **Différences avec Cloudera.** Ici le catalogue est une base **SQLite** (un « catalogue SQL » de PyIceberg), alors que sur CDP ce sera le **Hive Metastore**. Les concepts et l'organisation des fichiers sont les mêmes. L'API de PyIceberg peut changer d'une version à l'autre.

### 4.1 Installation

```bash
pip install "pyiceberg[sql-sqlite,pyarrow]" pandas
```

### 4.2 Création, deux ajouts, observation

```python
import os, json, datetime
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

wh = os.path.abspath("wh"); os.makedirs(wh, exist_ok=True)
cat = SqlCatalog("local", uri=f"sqlite:///{wh}/catalog.db", warehouse=f"file://{wh}")
cat.create_namespace("blog")

schema = pa.schema([("vente_id", pa.int64()), ("client_id", pa.int32()),
                    ("montant", pa.float64()), ("date_vente", pa.date32())])
t = cat.create_table("blog.ventes", schema=schema)

b1 = pa.table({"vente_id": [1, 2, 3], "client_id": [10, 11, 10], "montant": [120.0, 80.5, 200.0],
               "date_vente": [datetime.date(2024, 3, 1), datetime.date(2024, 3, 2), datetime.date(2024, 4, 1)]}, schema=schema)
b2 = pa.table({"vente_id": [4, 5], "client_id": [12, 10], "montant": [50.0, 310.0],
               "date_vente": [datetime.date(2024, 4, 2), datetime.date(2024, 4, 3)]}, schema=schema)

t.append(b1)      # commit 1
t.append(b2)      # commit 2
```

### 4.3 Ce que le disque contient (mesuré)

**Juste après `create_table`**, avant tout ajout :

```text
blog/ventes/metadata/00000-8853db8c-….metadata.json
```

Une table **vide** est déjà une table : un seul fichier, le `metadata.json`, sans snapshot.

**Après le premier `append`** :

```text
blog/ventes/metadata/00000-8853db8c-….metadata.json
blog/ventes/metadata/00001-aa4c0e9b-….metadata.json                      ← nouveau metadata.json
blog/ventes/metadata/537161ed-…-m0.avro                                   ← manifest
blog/ventes/metadata/snap-6882943637601159146-0-537161ed-….avro           ← manifest list
blog/ventes/data/00000-0-537161ed-….parquet                               ← données
```

**Après le deuxième `append`** : 3 `metadata.json` (`00000`, `00001`, `00002`), 2 manifests (`…-m0.avro`), 2 manifest lists (`snap-*.avro`), 2 fichiers Parquet.

| Élément | Après 2 ajouts | Rôle |
|---|---|---|
| `metadata.json` | **3** | Un nouveau à **chaque** commit, y compris la création |
| manifest list (`snap-*.avro`) | **2** | Un par snapshot |
| manifest (`*-m0.avro`) | **2** | Un par commit d'ajout ici |
| fichier Parquet (`data/`) | **2** | Les données : un fichier par ajout |

Un **ajout n'a réécrit aucun fichier de données existant** : il a seulement ajouté un fichier de données et de nouveaux fichiers de métadonnées.

### 4.4 Lire le `metadata.json` (mesuré)

```python
md = json.load(open(t.metadata_location.replace("file://", "")))
print(md["format-version"], md["last-column-id"])
print([(f["id"], f["name"], f["type"]) for f in md["schemas"][0]["fields"]])
for s in md["snapshots"]:
    print(s["snapshot-id"], s.get("parent-snapshot-id"), s["summary"])
```

Résultat observé :

```text
format-version : 2
last-column-id : 4
schemas[0].fields : [(1, 'vente_id', 'long'), (2, 'client_id', 'int'), (3, 'montant', 'double'), (4, 'date_vente', 'date')]
partition-specs   : [{'spec-id': 0, 'fields': []}]
nb snapshots      : 2 | current-snapshot-id = 942748304206150894

snapshot 6882943637601159146 | parent None               | op append | added-data-files 1 | added-records 3 | total-records 3
snapshot 942748304206150894  | parent 6882943637601159146 | op append | added-data-files 1 | added-records 2 | total-records 5
```

Ce qu'il faut remarquer :

- **Chaque colonne a un identifiant** (`1`, `2`, `3`, `4`) : c'est la base du renommage sûr.
- **Les snapshots forment une chaîne** : le second a pour parent le premier.
- Le **résumé** de chaque snapshot dit ce qu'il a fait (`append`, fichiers et lignes ajoutés, total courant).
- **Le format de cette table est la version 2** : c'est le défaut de PyIceberg 0.12.0. Sur ton cluster, la version créée par défaut peut être différente (section 6).
- Clés présentes dans le fichier : `schemas`, `partition-specs`, `snapshots`, `snapshot-log`, `metadata-log`, `refs`, `properties`, `sort-orders`, `current-snapshot-id`, `location`, `table-uuid`… (on les détaille au Blog 2).

### 4.5 Les tables de métadonnées

PyIceberg expose les mêmes informations sans lire les JSON à la main :

```python
t.inspect.snapshots()     # snapshot_id, operation, ...
t.inspect.manifests()     # un manifest par ligne : fichiers ajoutés, existants...
t.inspect.files()         # les fichiers de données : chemin, format, nombre de lignes, taille
```

Mesuré : deux fichiers Parquet de **2** et **3** lignes (`1755` et `1774` octets), deux manifests avec chacun `added_data_files_count = 1`.

Les moteurs SQL exposent des tables équivalentes (`.snapshots`, `.files`, `.manifests`…, Blog 6).

### 4.6 *Time travel* (mesuré)

```python
premier = md["snapshots"][0]["snapshot-id"]
t.scan(snapshot_id=premier).to_arrow().num_rows      # 3
t.scan().to_arrow().num_rows                         # 5
```

Lire au premier snapshot donne **3 lignes**, l'état courant en donne **5**. Aucune copie des données n'a été faite : on lit simplement un autre ensemble de fichiers.

### 4.7 Renommer une colonne : Parquet brut contre Iceberg (mesuré)

**Côté Parquet brut** (Spark 3.5.3, deux lots écrits dans le même dossier, le second avec la colonne renommée) :

```python
d1 = spark.createDataFrame([(1, 120.0), (2, 80.5), (3, 200.0)], ["vente_id", "montant"])
d2 = spark.createDataFrame([(4, 50.0), (5, 310.0)],             ["vente_id", "montant_ht"])
d1.write.mode("append").parquet("pq")
d2.write.mode("append").parquet("pq")
spark.read.parquet("pq").orderBy("vente_id").show()
```

```text
+--------+----------+
|vente_id|montant_ht|
+--------+----------+
|       1|      NULL|      ← les montants de l'ancien lot ont « disparu »
|       2|      NULL|
|       3|      NULL|
|       4|      50.0|
|       5|     310.0|
+--------+----------+
```

Avec `mergeSchema=true`, on obtient **deux colonnes** (`montant_ht` et `montant`), chacune à moitié vide : les données sont là, mais mal alignées. Dans les deux cas, **c'est à toi de corriger** (vues, `COALESCE`, réécriture).

**Côté Iceberg** (la table de 4.2) :

```python
with t.update_schema() as u:
    u.rename_column("montant", "montant_ht")

t = cat.load_table("blog.ventes")
t.scan().to_pandas().sort_values("vente_id")[["vente_id", "montant_ht"]]
```

```text
 vente_id  montant_ht
        1       120.0
        2        80.5
        3       200.0
        4        50.0
        5       310.0
```

Les **cinq** montants sont lus, y compris ceux écrits **avant** le renommage. Dans le `metadata.json`, on voit pourquoi : il y a maintenant **deux schémas** (`schema-id` 0 et 1) et le champ d'identifiant **3** s'appelle `montant` dans l'un, `montant_ht` dans l'autre. Le renommage a ajouté un **nouveau `metadata.json`** (`00003`, **4** au total) et les lignes déjà écrites sont relues telles quelles.

> **C'est la réponse au scénario de jeudi.** (Blog 5 de cette série : le détail de l'évolution de schéma, avec ses limites.)

---

## 5. Iceberg sur Cloudera : ce que disent les sources

Je n'ai pas accès à ton cluster. Voici ce que disent la **documentation Cloudera** et la **communauté Cloudera** consultées (liens en section 13). Chaque ligne est à **confirmer** sur ta version.

| Sujet | Ce que disent les sources | À vérifier chez toi |
|---|---|---|
| Version minimale | Support de l'accès et du traitement **Iceberg 1.3** à partir de **CDP Private Cloud Base 7.1.9** | Ta version de CDP et de Cloudera Manager |
| Moteurs sur 7.1.9 | **Impala, Spark 3, Flink, NiFi** sont cités comme intégrés | Les moteurs installés et activés |
| Hive | Un expert Cloudera indique, sur le forum communautaire, qu'Iceberg avec Hive est supporté **à partir de CDP on-prem 7.3.1** | Si tu es en 7.1.9 : Hive peut fonctionner avec des JAR ajoutés, mais ce n'est pas supporté |
| Catalogue | Le **HiveCatalog** (métastore Hive) est celui par défaut | `DESCRIBE FORMATTED` sur une table Iceberg |
| Versions de format | La documentation CDW indique que Hive et Impala lisent et écrivent des tables **V1 ou V2** | La version créée **par défaut** par chaque moteur |
| Sécurité | Intégration avec **Apache Ranger** | Tes politiques Ranger et le droit sur l'emplacement des données |
| Stockage | **HDFS** et **Ozone** certifiés | Ton stockage |

### 5.1 Une règle d'or sur la documentation

La version d'Iceberg **embarquée** dans une distribution est souvent **en retard** sur la version la plus récente du projet Apache. Pour savoir ce que **ta plateforme** sait faire, **lis la documentation Cloudera de ta version**, pas seulement la documentation du projet. Une fonction décrite en amont n'est pas forcément disponible chez toi.

---

## 6. Audit de ta stack Cloudera (laboratoire)

> **Toutes les commandes de cette section sont à exécuter par toi : je n'ai pas de cluster CDP.** Les syntaxes viennent des moteurs ; vérifie-les sur ta version.

### 6.1 Version de la plateforme et JAR Iceberg

```bash
# Parcels installés : version de CDH et de Spark 3
ls /opt/cloudera/parcels/

# Fichiers Iceberg présents (la version du JAR révèle la version d'Iceberg embarquée)
find /opt/cloudera/parcels -name "*iceberg*" 2>/dev/null | head -20
```

Un nom de JAR comme `iceberg-spark-runtime-3.5_2.12-1.3.0...` indique à la fois la version de Spark visée et la version d'Iceberg. **Note ces versions** : elles bornent ce que tu peux faire.

Dans **Cloudera Manager**, relève aussi : la version de CDP, la liste des services (Hive, Hive on Tez, Impala, Spark 3, Flink, NiFi, Ranger, Atlas).

### 6.2 Créer la même table dans chaque moteur

```sql
-- Impala
CREATE DATABASE IF NOT EXISTS blog_iceberg;
CREATE TABLE blog_iceberg.ventes_ice_impala (
  vente_id BIGINT, client_id INT, montant DECIMAL(10,2), date_vente DATE
)
PARTITIONED BY SPEC (month(date_vente))
STORED AS ICEBERG;

-- Hive
CREATE TABLE blog_iceberg.ventes_ice_hive (
  vente_id BIGINT, client_id INT, montant DECIMAL(10,2), date_vente DATE
)
PARTITIONED BY SPEC (month(date_vente))
STORED BY ICEBERG;
```

Pour **Spark 3**, le catalogue Iceberg doit être configuré (une partie peut déjà l'être sur CDP : vérifie) :

```bash
spark3-shell \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive
```

```sql
-- Spark SQL
CREATE TABLE blog_iceberg.ventes_ice_spark (
  vente_id BIGINT, client_id INT, montant DECIMAL(10,2), date_vente DATE
)
USING iceberg
PARTITIONED BY (months(date_vente));
```

Si une de ces commandes **échoue**, c'est déjà un résultat d'audit : note le message exact (JAR absent, syntaxe inconnue, droit refusé).

### 6.3 Regarder ce qui a été créé

```sql
DESCRIBE FORMATTED blog_iceberg.ventes_ice_impala;     -- type de table, emplacement, propriétés (cherche la version de format)
SHOW CREATE TABLE  blog_iceberg.ventes_ice_impala;
```

```bash
# remplace par l'emplacement donné par DESCRIBE FORMATTED
hdfs dfs -ls -R <emplacement_de_la_table>
```

Après quelques `INSERT`, compare avec la section 4.3 : tu dois retrouver un dossier `metadata/` (fichiers `*.metadata.json`, manifests, manifest lists) et un dossier `data/` (fichiers de données, éventuellement rangés par partition).

### 6.4 Tester l'interopérabilité

```sql
-- Dans Impala
INSERT INTO blog_iceberg.ventes_ice_impala VALUES (1, 10, 120.00, DATE '2024-03-01');

-- Dans Spark (ou Hive si supporté) : lire ce que Impala a écrit
SELECT COUNT(*) FROM blog_iceberg.ventes_ice_impala;

-- Puis écrire depuis Spark et relire dans Impala
INSERT INTO blog_iceberg.ventes_ice_impala VALUES (2, 11, 80.50, DATE '2024-03-02');
```

Si Impala ne voit pas les lignes écrites par un autre moteur, essaie `REFRESH blog_iceberg.ventes_ice_impala;` et **note** si c'était nécessaire (comportement selon la version).

### 6.5 Droits

Si une création, une lecture ou une écriture échoue avec une erreur de droit, vérifie **les deux couches** : la politique **Ranger** sur la base et la table, et le droit sur l'**emplacement de stockage** des fichiers.

### 6.6 Squelette : le tableau d'audit

Remplis-le sur ton cluster (**✅** fonctionne, **❌** échoue, **⚠️** partiel, avec le message d'erreur) :

| Moteur | Version | Créer | `INSERT` | Lire | V1 / V2 créée par défaut | `UPDATE` / `DELETE` / `MERGE` | *Time travel* | Compactage / maintenance |
|---|---|---|---|---|---|---|---|---|
| Hive | ___ | ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| Impala | ___ | ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| Spark 3 | ___ | ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| Flink / NiFi | ___ | ___ | ___ | ___ | ___ | ___ | ___ | ___ |

<details>
<summary>Comment remplir les colonnes que ce blog n'a pas encore couvertes</summary>

Les colonnes `UPDATE` / `DELETE` / `MERGE`, *time travel* et compactage sont étudiées aux Blogs 6, 7 et 9. Pour l'instant, tente simplement chaque opération et note le **résultat brut** (succès ou message d'erreur) ; on reviendra dessus avec les bonnes commandes par moteur.

Ce tableau est ton **document de référence** pour toute la série : il te dira quels blogs s'appliquent tels quels et lesquels demandent une adaptation à ta version.
</details>

---

## 7. Quand ne pas utiliser Iceberg

Iceberg n'est pas un réflexe. Pose-toi ces questions :

| Situation | Pourquoi y réfléchir à deux fois |
|---|---|
| **Petite table de référence** (quelques milliers de lignes), stable | Le gain est faible, la maintenance d'une table de plus est un coût |
| **Moteur ou version** qui ne supporte pas bien Iceberg | Un moteur qui lit mal ou n'écrit pas V2 casse la chaîne ; la fonction doit exister **là où tu l'utilises** |
| **Dépendance à des fonctions propres à Hive ACID** | Les tables transactionnelles Hive ont leurs propres mécanismes ; changer de format demande une migration (Blog 13) |
| **Équipe sans processus de maintenance** | Snapshots et petits fichiers s'accumulent : il faut planifier le compactage et l'expiration (Blog 9) |
| **Écritures ligne à ligne très fréquentes** | Possible en V2, mais coûteux sans maintenance ; à mesurer (Blog 7) |
| **Documentation upstream qui promet une fonction absente de ta plateforme** | Voir 5.1 |

**Quand c'est un bon choix** : grosses tables de faits, partitionnement qui doit évoluer, plusieurs moteurs sur les mêmes données, besoin d'écritures fiables et d'historique, charges incrémentales et CDC.

---

## 8. Les pièges d'adoption

| # | Piège | Pourquoi | Que faire |
|---|---|---|---|
| 1 | Croire qu'un renommage de colonne en amont est sans risque **sur du Parquet brut** | Mesuré en 4.7 : les anciennes lignes deviennent `NULL` | Voilà précisément le problème qu'Iceberg résout |
| 2 | Croire qu'Iceberg supprime la maintenance | Mesuré : **un nouveau `metadata.json` à chaque commit** (4 après deux ajouts et un renommage) ; les fichiers de métadonnées et de données s'accumulent | Prévoir expiration, compactage, nettoyage (Blog 9) |
| 3 | Copier un fichier Parquet **à la main** dans le dossier `data/` | Un fichier absent d'un manifest **n'existe pas** pour la table | Toujours passer par un `INSERT` ou une procédure du moteur |
| 4 | **Supprimer des fichiers à la main** dans `data/` ou `metadata/` | Les manifests référencent encore ces fichiers : la table devient illisible | Jamais de suppression manuelle : expiration de snapshots et nettoyage par procédure (Blog 9) |
| 5 | Ne pas vérifier la **version de format** | Un moteur ne supportant pas V2 ne lira pas une table V2 | Auditer (section 6) et fixer la version à la création |
| 6 | Suivre la **doc upstream** au lieu de la doc de la plateforme | La version embarquée peut être en retard | Lire la documentation Cloudera de ta version |
| 7 | Ne vérifier que les droits Ranger | Les fichiers sont aussi protégés au niveau du stockage | Contrôler les deux couches |
| 8 | Changer de moteur d'écriture sans test | Types, partitionnement, version de format peuvent différer | Test d'interopérabilité (6.4) avant de s'engager |
| 9 | Choisir un catalogue non adapté au stockage | Certains catalogues reposent sur des opérations de renommage qui ne sont pas atomiques sur du stockage objet | Sur Cloudera, la documentation recommande le HiveCatalog par défaut |
| 10 | Migrer sans plan de retour | Une migration touche des consommateurs (BI, jobs) | Blog 13 |

---

## 9. 10 questions d'entretien

**1. Qu'est-ce qu'Apache Iceberg ?**
Une spécification de **format de table** ouverte : elle décrit, dans des fichiers de métadonnées, quels fichiers de données composent une table à un instant donné, avec son schéma et son partitionnement. Les données restent en Parquet, ORC ou Avro.

**2. Format de fichier ou format de table : quelle différence ?**
Le format de fichier dit comment stocker des lignes dans un fichier (Parquet). Le format de table dit comment un **ensemble** de fichiers forme une table : schéma, partitions, historique, commits atomiques.

**3. Pourquoi un commit Iceberg est-il atomique ?**
Parce qu'il se termine par le **remplacement d'un pointeur** dans le catalogue, du `metadata.json` courant vers le nouveau. Les lecteurs voient l'ancien état ou le nouveau, jamais un mélange.

**4. Que contient un `metadata.json` ?**
Le schéma (avec identifiants de colonnes), les partitionnements, la liste des snapshots et leur résumé, le snapshot courant, l'historique des métadonnées et les propriétés de la table.

**5. Quelle différence entre une manifest list et un manifest ?**
La manifest list est la liste des **manifests** d'un snapshot. Un manifest est la liste des **fichiers de données** (avec statistiques par fichier). La hiérarchie permet de planifier une lecture sans lister de dossiers.

**6. Pourquoi renommer une colonne est-il sûr dans Iceberg ?**
Chaque colonne a un **identifiant** stable. Le nom n'est qu'un libellé dans un schéma : renommer ajoute un nouveau schéma qui associe le même identifiant à un autre nom, sans toucher aux anciens fichiers.

**7. Comment fonctionne le *time travel* ?**
Chaque commit produit un **snapshot immuable**. Lire « à un instant passé » consiste à lire les fichiers référencés par un snapshot ancien. Il n'y a pas de copie des données.

**8. Qu'est-ce qu'Iceberg ne résout pas ?**
Les petits fichiers, les mauvais choix de partitionnement et les statistiques ne disparaissent pas : il faut une maintenance planifiée. Et il dépend du support de chaque moteur.

**9. Quand déconseillerais-tu Iceberg ?**
Pour de petites tables stables, quand un moteur de la chaîne ne le supporte pas correctement, sans processus de maintenance, ou quand la fonction recherchée n'existe pas dans la version embarquée de la plateforme.

**10. Comment vérifies-tu le support d'Iceberg sur une plateforme Cloudera ?**
Je relève la version de CDP et des parcels, je cherche les JAR Iceberg pour connaître la version embarquée, je lis la documentation Cloudera de cette version, puis je **teste** : création, `INSERT`, lecture croisée entre moteurs, droits Ranger, et je consigne tout dans un tableau d'audit.

---

## 10. 5 exercices

### Exercice 1 : reproduire la démonstration (squelette)
Complète le script sans regarder la section 4.

```python
cat = SqlCatalog("local", uri=f"sqlite:///{wh}/catalog.db", warehouse=f"file://{wh}")
cat.______("blog")                                   # créer le namespace
t = cat.______("blog.ventes", schema=schema)         # créer la table
t.______(b1)                                         # ajouter des lignes
t.______(b2)

print(t.inspect.______().to_pandas())                # liste des snapshots
premier = t.inspect.snapshots().to_pandas().iloc[0]["snapshot_id"]
print(t.scan(______=premier).to_arrow().num_rows)    # lire au premier snapshot
```

<details>
<summary>Solution</summary>

```python
cat.create_namespace("blog")
t = cat.create_table("blog.ventes", schema=schema)
t.append(b1)
t.append(b2)
print(t.inspect.snapshots().to_pandas())
premier = t.inspect.snapshots().to_pandas().iloc[0]["snapshot_id"]
print(t.scan(snapshot_id=premier).to_arrow().num_rows)     # 3
```

(Les noms de méthodes peuvent varier selon la version de PyIceberg.)
</details>

### Exercice 2 : lire une arborescence
Après **trois** ajouts successifs sur une table créée vide, combien y a-t-il de `metadata.json`, de snapshots, de manifest lists et de fichiers de données (un fichier par ajout) ?

<details>
<summary>Solution</summary>

**4** `metadata.json` (un à la création + un par commit), **3** snapshots, **3** manifest lists (un par snapshot), **3** fichiers de données. Par analogie avec la section 4.3 (deux ajouts : 3, 2, 2, 2).
</details>

### Exercice 3 : prédire le résultat
Un lot de données utilise `montant`, le suivant `montant_ht` (même colonne renommée). Qu'obtient-on en lisant (a) un dossier Parquet brut avec Spark, (b) une table Iceberg dont le schéma a été renommé via `rename_column` ?

<details>
<summary>Solution</summary>

(a) Dans la mesure de 4.7, la lecture simple du dossier donne la colonne `montant_ht` avec des `NULL` pour les lignes de l'ancien lot ; avec `mergeSchema=true`, deux colonnes à moitié vides. (b) Les cinq valeurs sont lues sous le nom `montant_ht`, car l'identifiant de colonne est le même dans les deux schémas.
</details>

### Exercice 4 : ton tableau d'audit
Remplis le tableau de la section 6.6 pour Hive, Impala et Spark 3 sur ton cluster, et note la **version de CDP**, la **version d'Iceberg embarquée** (via le nom des JAR) et la version de format créée par défaut.

<details>
<summary>Solution</summary>

Il n'y a pas de solution unique : c'est **ton** état des lieux. Ce qui compte, c'est de noter les **messages d'erreur exacts** de chaque échec, car ils servent aux Blogs suivants. Pour la version de format, cherche-la dans la sortie de `DESCRIBE FORMATTED` (ou dans les propriétés de la table) ; si tu ne la vois pas, note-le, on voit au Blog 2 comment lire le `metadata.json` directement sur HDFS.
</details>

### Exercice 5 : le scénario de 14h02
Une analyste lit `ventes_hist` pendant qu'un job réécrit plusieurs partitions et obtient un total trop bas. Explique ce qui se passe avec une table Hive/Parquet, puis avec une table Iceberg.

<details>
<summary>Solution</summary>

**Hive/Parquet** : le job remplace les partitions les unes après les autres (ou écrit dans le dossier en plusieurs étapes) ; la lecture de l'analyste liste les dossiers à un instant où certaines partitions sont déjà remplacées et d'autres non, ou vides : l'état lu n'a **jamais existé** comme état cohérent.

**Iceberg** : le job écrit de nouveaux fichiers puis effectue **un commit** qui remplace le pointeur du catalogue. Tant que le commit n'est pas fait, l'analyste lit l'**ancien snapshot** complet ; après, le **nouveau** snapshot complet. Pas d'état mixte.
</details>

---

## 11. Checklist de départ

- [ ] Je connais la **version de CDP**, de Cloudera Manager et les **moteurs** installés
- [ ] J'ai relevé la **version d'Iceberg embarquée** via les JAR (`find /opt/cloudera/parcels -name "*iceberg*"`)
- [ ] J'ai lu la **documentation Cloudera de ma version**, pas seulement la doc upstream
- [ ] J'ai créé une table Iceberg dans **chaque moteur** et noté les échecs avec leur message
- [ ] J'ai comparé la structure sur HDFS avec la section 4.3 (`metadata/` et `data/`)
- [ ] J'ai testé l'**interopérabilité** (écrire dans un moteur, lire dans un autre)
- [ ] J'ai vérifié les droits **Ranger** et ceux de l'emplacement de stockage
- [ ] J'ai rempli mon **tableau d'audit** (6.6)
- [ ] J'ai refait la **démonstration locale** (section 4) et compris pourquoi le renommage est sûr
- [ ] Je sais citer trois cas où **je n'utiliserais pas** Iceberg

---

## 12. Prochain blog et blogs restants

**Blog 2 : anatomie d'une table Iceberg.** On ouvre les fichiers : le contenu complet d'un `metadata.json`, la lecture d'un manifest (statistiques par fichier, comptages), le rôle des numéros de séquence, la différence entre V1 et V2, et ce que fait réellement un commit, étape par étape, y compris en cas de conflit.

| # | Titre | Contenu principal |
|---|---|---|
| ✅ 1 | Pourquoi Iceberg ? Audit de ta stack Cloudera | Limites des tables Hive/Parquet, structure en couches, démonstration locale, audit de la plateforme |
| 2 | Anatomie d'une table Iceberg | `metadata.json`, snapshots, manifest list, manifests, delete files, V1/V2, mécanique d'un commit |
| 3 | Créer, charger, migrer | `CREATE TABLE` depuis Hive, Impala, Spark ; CTAS ; migration in-place ou par copie |
| 4 | Multi-moteurs sur Cloudera | Matrice des fonctions par moteur et version, HiveCatalog, interopérabilité |
| 5 | Schéma et partitionnement évolutifs | Identifiants de colonnes, *hidden partitioning*, transformations, évolution du partitionnement |
| 6 | Snapshots, *time travel* et tables de métadonnées | Lire dans le passé, *rollback*, `.snapshots`, `.files`, `.manifests` |
| 7 | Écritures ligne à ligne | `INSERT`, `UPDATE`, `DELETE`, `MERGE`, *copy-on-write* contre *merge-on-read*, conflits |
| 8 | Performance et planification de lecture | Élagage par manifests, taille de fichiers, tri, lecture des plans |
| 9 | Maintenance de tables | Compactage, expiration des snapshots, fichiers orphelins, calendrier |
| 10 | Pipelines : CDC, incrémental, streaming | `MERGE` pour le CDC, lectures incrémentales, Flink, NiFi, Spark, idempotence |
| 11 | Sécurité et gouvernance | Ranger, SDX, lignage, chiffrement, audit |
| 12 | Architecture lakehouse | Bronze / silver / gold, SCD2, modélisation, choix de moteur par charge |
| 13 | Migration à l'échelle | Plan par étapes, coexistence, réconciliation, retour arrière |
| 14 | Opérations, coûts et décisions | Indicateurs de santé, runbooks, reprise après sinistre, ADR, standards d'équipe |

### Pour t'entraîner avant le Blog 2

Sans regarder la suite : (1) après **cinq** ajouts successifs, combien y a-t-il de `metadata.json` dans le dossier `metadata/` ? (2) Un manifest contient la liste de quoi, et quelles informations utiles à la **planification** y trouve-t-on ? (3) Si tu supprimes **une ligne** dans une table dont les fichiers de données sont immuables, que doit-il se passer physiquement ? Si tu hésites, le Blog 2 est fait pour toi.

---

## 13. Sources à vérifier

Documentation et communauté Cloudera consultées pour la section 5 (à relire sur **ta** version) :

- Open Data Lakehouse, CDP Private Cloud Base 7.1.9 : `https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/lakehouse-overview/topics/private-cloud-open-data-lakehouse.html`
- Open Data Lakehouse, CDP Private Cloud Base 7.3.2 : `https://docs.cloudera.com/cdp-private-cloud-base/7.3.2/lakehouse-overview/topics/private-cloud-open-data-lakehouse.html`
- Présentation d'Iceberg dans CDW Runtime : `https://docs.cloudera.com/cdw-runtime/1.5.4/iceberg-overview/iceberg-overview.pdf`
- Discussion communautaire sur Iceberg avec Hive en CDP 7.1.9 (réponse d'un expert Cloudera : support de Hive avec Iceberg à partir de 7.3.1 en on-prem) : `https://community.cloudera.com/t5/Support-Questions/Assistance-Required-for-Deploying-Iceberg-SerDe-in-CDP-7-1-9/m-p/413361`

---

*Si cet article t'a aidé, la meilleure façon de le consolider est de refaire la démonstration locale, puis de remplir ton tableau d'audit sur ton propre cluster.*