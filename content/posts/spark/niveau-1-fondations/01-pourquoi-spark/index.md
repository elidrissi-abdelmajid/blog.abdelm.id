---
title: "Pourquoi Spark ?"
description: "Big Data, Hadoop MapReduce vs Spark, calcul en mémoire, écosystème."
weight: 1
---

## 1- Pourquoi Spark ? Du Big Data au calcul distribué en mémoire


---

## Sommaire

1. Le problème : quand une machine ne suffit plus
2. Hadoop MapReduce : la première réponse
3. Spark : l'idée centrale
4. Pourquoi Spark est plus rapide
5. L'écosystème Spark
6. Où tourne Spark ?
7. Quand utiliser (ou non) Spark
8. Squelette à compléter
9. Prochain blog

---

## 1. Le problème : quand une machine ne suffit plus

Une machine a des limites : CPU, RAM, disque. Quand les données dépassent ces limites, deux options :

| Approche | Principe | Limite |
|---|---|---|
| **Scale up** (vertical) | Une machine plus puissante | Coût exponentiel, plafond physique, point de défaillance unique |
| **Scale out** (horizontal) | Plusieurs machines ordinaires | Complexité : il faut coordonner le travail |

Le **calcul distribué** choisit le scale out. Un **cluster** est un ensemble de machines interconnectées qui travaillent comme un seul système.

Exemple : 10 workers × (16 CPU, 64 Go RAM) = **160 CPU et 640 Go RAM**.

Le vrai défi n'est pas d'avoir des machines, c'est de :
- découper les données et le calcul,
- répartir le travail,
- gérer les pannes,
- rassembler les résultats.

C'est exactement ce que fait un framework comme Spark.

---

## 2. Hadoop MapReduce : la première réponse

Hadoop a popularisé le Big Data avec deux briques :
- **HDFS** : stockage distribué
- **MapReduce** : modèle de calcul en deux phases (Map puis Reduce)

```
Lecture HDFS → Map → écriture disque → Shuffle → Reduce → écriture HDFS
```

**Le problème :** chaque étape lit et écrit sur **disque**. Un traitement à plusieurs étapes (ML itératif, pipelines complexes) enchaîne donc des I/O disque très coûteuses. De plus, le modèle Map/Reduce est verbeux et peu adapté aux traitements interactifs.

---

## 3. Spark : l'idée centrale

**Apache Spark** est un moteur de calcul distribué, **en mémoire**, pour le traitement de données à grande échelle. Créé à Berkeley (AMPLab), il est aujourd'hui un projet Apache.

Spark est souvent présenté comme le **successeur de Hadoop MapReduce** pour le calcul. Il ne remplace pas le stockage : Spark lit et écrit sur HDFS, S3, ADLS, Kafka, bases de données, etc.

```
        Votre code (Python / Scala / SQL / Java / R)
                         │
                    Driver Spark
                         │
        ┌────────────┬───┴────────┬────────────┐
     Executor     Executor     Executor     Executor
     (données en mémoire, tasks en parallèle)
```

Trois idées à retenir :
1. **Un driver** coordonne.
2. **Des executors** exécutent les tasks en parallèle.
3. **Les données restent en mémoire** entre les étapes quand c'est possible.

---

## 4. Pourquoi Spark est plus rapide

| Facteur | MapReduce | Spark |
|---|---|---|
| Données intermédiaires | Écrites sur disque | **Gardées en mémoire** |
| Modèle d'exécution | Map → Reduce rigide | **DAG** (graphe d'opérations) |
| Optimisation | Aucune globale | **Catalyst** optimise le plan complet |
| Génération de code | Non | Bytecode Java généré (Tungsten) |
| API | Bas niveau, verbeuse | DataFrame / SQL, haut niveau |
| Cas itératifs (ML) | Très lent | Natif |

> ⚠️ **Piège classique (certification)** : la vraie raison de la performance de Spark est le **calcul et le stockage en mémoire**, pas les RDD, ni Kubernetes, ni les DataFrames en tant que tels.

### Deux concepts qui expliquent tout

**Lazy evaluation** : Spark ne calcule rien tant qu'on n'appelle pas une **action**. Les transformations construisent juste un plan, ce qui permet à Spark de l'optimiser avant d'exécuter.

**Tolérance aux pannes** : Spark mémorise la **lineage** (l'historique des transformations). Si un executor tombe, les partitions perdues sont **recalculées** ailleurs, sans tout relancer.

---

## 5. L'écosystème Spark

Un seul moteur, plusieurs modules :

| Module | Usage |
|---|---|
| **Spark Core** | Moteur de base (RDD, scheduling, mémoire) |
| **Spark SQL / DataFrame** | Données structurées, requêtes SQL, optimiseur Catalyst |
| **Structured Streaming** | Traitement temps réel (Kafka, etc.) |
| **MLlib** | Machine learning distribué |
| **GraphX** | Calcul sur graphes (Scala/Java) |

Langages : **Scala** (natif), **Python (PySpark)**, SQL, Java, R.

> Spark est écrit en Scala et tourne sur la JVM. PySpark passe par **Py4J** pour appeler le code JVM : une application PySpark démarre donc toujours une JVM. On creusera ça au Blog 8.

---

## 6. Où tourne Spark ?

Spark a besoin d'un **cluster manager** pour obtenir des ressources :

| Cluster manager | Remarque |
|---|---|
| **Hadoop YARN** | Très répandu en entreprise |
| **Kubernetes** | Standard cloud-native |
| Standalone / Mesos | Moins courants |
| `local[*]` | Pour développer et tester sur une machine |

YARN et Kubernetes couvrent plus de **90 %** des cas d'usage.

---

## 7. Quand utiliser (ou non) Spark

| Utiliser Spark |  Éviter Spark |
|---|---|
| Données de plusieurs Go à To+ | Petit jeu de données (pandas suffit) |
| ETL / pipelines batch lourds | Requêtes transactionnelles (OLTP) |
| Jointures et agrégations massives | Latence en millisecondes sur une ligne |
| Streaming à grande échelle | Logique purement séquentielle |
| ML distribué | Quand une seule machine suffit |

Règle simple : **si ça tient confortablement dans pandas, n'utilisez pas Spark.**

---

## 8. Squelette à compléter

Objectif : écrire de mémoire la première session Spark (qu'on installera pour de bon au Blog 2).

```python
from pyspark.sql import ______

spark = (
    ______.builder
    .appName("______")
    .master("______")        # local[*] pour tester
    .getOrCreate()
)

df = spark.range(______)      # 1 million de lignes
result = df.______("id % 2 = 0").______()   # filtre puis action

print(result)
spark.______()
```

<details>
<summary>Solution</summary>

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("blog1")
    .master("local[*]")
    .getOrCreate()
)

df = spark.range(1_000_000)
result = df.filter("id % 2 = 0").count()

print(result)  # 500000
spark.stop()
```

</details>

**Question de réflexion :** dans ce code, quelle ligne déclenche réellement le calcul ? (Indice : *lazy evaluation*.)

---

## Points clés

1. Une machine ne suffit plus : on passe au **scale out** avec un cluster.
2. MapReduce écrit tout sur disque ; Spark garde les données **en mémoire**.
3. Spark = **driver** (coordonne) + **executors** (exécutent en parallèle).
4. **Lazy evaluation** + **DAG** + **Catalyst** = optimisation globale.
5. **Lineage** = tolérance aux pannes par recalcul.
6. Un moteur, plusieurs modules : SQL, Streaming, MLlib, GraphX.
7. Spark tourne surtout sur **YARN** ou **Kubernetes**.

---


---


#Spark #ApacheSpark #BigData #DataEngineering #PySpark #Hadoop #LearnSpark #DataEngineer #CloudComputing #SparkDeZeroAExpert
