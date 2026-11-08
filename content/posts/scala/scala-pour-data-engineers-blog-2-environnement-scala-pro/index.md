---
title: "Scala pour Data Engineers, Blog 2 : environnement Scala professionnel"
description: "Configurer un environnement Scala 2.12 reproductible avec JDK, Maven, IntelliJ et des tests rapides."
weight: 2
---

# Blog 02 — Environnement Scala pro : JDK, Scala 2.12, Maven, IntelliJ, tests rapides (sans Spark)

> Durée : 35 à 45 min · Niveau : débutant · Aucune notion Spark dans ce blog : Scala pur. Spark viendra plus tard, par conversion pas à pas, avec le **même** projet.

## Ce qui a été vérifié, et ce qui ne l'a pas été

| Élément | Statut |
|---|---|
| Le code Scala du projet (`Modele`, `Parseur`, `Pipeline`, `Main`) et sa sortie (section 6) | **Exécuté** en Scala 2.12.20 sur JDK 21 (environnement de rédaction), compilé avec `scalac` |
| Version de bytecode produite par Scala 2.12.20 (major 52) | **Vérifié** en lisant l'en-tête du `.class` |
| `scala.util.Using` absent en 2.12 | **Vérifié** (erreur de compilation reproduite) |
| Le `pom.xml`, les plugins Maven, ScalaTest, les commandes `mvn` | **Non exécutés** : mon environnement n'a pas accès à Maven Central. Versions des plugins **indicatives, à vérifier chez toi** |
| Menus et options IntelliJ | **À vérifier chez toi** (les libellés changent selon la version) |
| JDK et patch Scala exacts de ton cluster | **À vérifier chez toi** (commandes section 3) |

---

## 1. Le scénario

**Lundi 9h.** Un collègue clone ton projet : « Ça ne compile pas chez moi », puis « les tests passent chez toi mais pas chez moi ». Même code, deux machines, deux résultats. Plus tard ce projet devra tourner sur le cluster (Scala 2.12, Spark 3.3.2) : si le poste n'est pas aligné, tu découvriras l'erreur le jour du déploiement.

Tout le blog sert à construire **un projet reproductible**, aligné sur le cluster, qui compile et teste vite dans IntelliJ.

## 2. Le jeu de données fil rouge

Les mêmes 15 lignes sales du blog 01, cette fois dans un fichier `src/main/resources/transactions.csv` (une ligne par transaction, sans en-tête, format `id,client,montant,devise,date`). Résultats de référence (blog 01) : **15 lignes, 7 valides, 6 uniques, 8 rejets, total 5370.75 MAD**.

```
T001,C01,1500.00,MAD,2026-10-01
T002,C01,200.50,MAD,2026-10-02
T003,C02,-300.00,MAD,2026-10-02
T004,C02,,MAD,2026-10-03
T005,C03,abc,MAD,2026-10-03
T006,C03,1000.00,EUR,2026-10-04
T007,C04,750.25,MAD,03/10/2026
T001,C01,1500.00,MAD,2026-10-01
T009,C99,400.00,MAD,2026-10-05
T010,C04,0.00,MAD,2026-10-05
T011,C02,2500.00,MAD,2026-10-06
T012,C05,300.00,mad,2026-10-06
T013,C01,90.00,MAD
T014,C03,120.00,MAD,2026-10-07
T015,C02,50.00,MAD,
```

---

## 3. Notion 1 — L'alignement des versions (la source de 80 % des incidents de build)

**(a) Concept.** Quatre versions doivent être cohérentes : le **JDK** (qui exécute), la **version de Scala** (2.12 : le « binaire » 2.12 est le même pour tout 2.12.x, mais pas compatible avec 2.13), la **cible de bytecode** (ce que ton jar demande comme JVM minimale) et, plus tard, la version de Spark (3.3.2 chez toi). Une règle : **ton poste doit se plier au cluster, jamais l'inverse.**

**(b) Relevé à faire (squelette à remplir).** Dans `spark3-shell` **sur le cluster** (seulement pour lire les versions, rien d'autre) :

```scala
scala.util.Properties.versionString      // ???
System.getProperty("java.version")       // ???
```

Sur ton poste :

```
java -version      // ???
mvn -v             // ???  (donne aussi le JDK utilisé par Maven)
```

Reporte-les dans l'onglet « Alignement environnement » du fichier `roadmap-scala-data-engineer.xlsx`.

**(c) Résultat attendu (le correct).**

| Mesure | Valeur attendue |
|---|---|
| Scala du cluster | `2.12.x` (d'après toi ; patch à relever) |
| Scala de ton `pom.xml` | **la même** `2.12.x` |
| Bytecode produit par Scala 2.12.20 (vérifié ici) | **major 52 = Java 8**, même compilé sous JDK 21 |

Un bytecode major 52 s'exécute sur Java 8, 11 et 17 : c'est ce qui rend l'alignement du JDK peu risqué pour du Scala 2.12 pur, à condition de ne **pas** monter la cible.

**(d) Pièges provoqués.**

| Piège | Résultat faux |
|---|---|
| Écrire du code d'après une doc Scala **2.13** : `import scala.util.Using` | **Vérifié** : `error: object Using is not a member of package util` (absent en 2.12) |
| Compiler avec une cible plus récente que le JDK du cluster (ex. Java 17 pour un cluster en Java 11) | Typiquement `UnsupportedClassVersionError` au lancement (**non exécuté, à reproduire chez toi**) : major 61 = Java 17, 55 = Java 11, 52 = Java 8 |
| Mettre `scala.version` en 2.13 alors que le cluster est en 2.12 | Typiquement `NoSuchMethodError` (par exemple sur `scala.Predef$.refArrayOps`) au déploiement (**non exécuté**) |

**(e) Correction.** `scala.version` = celle du cluster, cible bytecode Java 8 (ou le JDK du cluster, pas plus), et une **garde** automatique dans les tests (section 7) qui échoue si quelqu'un change de version de Scala.

### Prédis avant de voir 1
Quelle version de bytecode (major) produit `scalac` 2.12.20 par défaut, même sous JDK 21 : 52, 55 ou 65 ?

<details><summary>Réponse</summary>

**52** (Java 8). Vérifié en lisant l'en-tête du `.class` (octets 6 et 7) : 52, avec ou sans `-target:jvm-1.8`.
</details>

---

## 4. Notion 2 — Structure d'un projet Maven Scala

**(a) Concept.** Maven impose une disposition standard ; le `pom.xml` décrit les versions, les dépendances et les plugins. Pour Scala il faut en plus `scala-maven-plugin` (compile Scala) et, pour les tests, `scalatest-maven-plugin` (lance ScalaTest ; on désactive le plugin de test par défaut, surefire, qui ne connaît pas ScalaTest).

**(b) Arborescence (à créer toi-même).**

```
scala101/
├── pom.xml
└── src/
    ├── main/
    │   ├── resources/transactions.csv
    │   └── scala/dev/scala101/
    │       ├── Modele.scala
    │       ├── Parseur.scala
    │       ├── Pipeline.scala
    │       └── Main.scala
    └── test/scala/dev/scala101/
        └── ParseurSpec.scala
```

**`pom.xml` : squelette à compléter** (tout ce qui est `???` vient de ton relevé de la notion 1) :

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.scala101</groupId>
  <artifactId>scala101</artifactId>
  <version>0.1.0-SNAPSHOT</version>

  <properties>
    <scala.version>???</scala.version>               <!-- ex. 2.12.x, celle du cluster -->
    <scala.binary.version>???</scala.binary.version>  <!-- 2.12 -->
    <scalatest.version>???</scalatest.version>        <!-- une 3.2.x -->
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>${scala.version}</version>
    </dependency>
    <dependency>
      <groupId>org.scalatest</groupId>
      <artifactId>scalatest_${scala.binary.version}</artifactId>
      <version>${scalatest.version}</version>
      <scope>???</scope>                              <!-- test -->
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>???</version>
        <executions>
          <execution><goals><goal>compile</goal><goal>testCompile</goal></goals></execution>
        </executions>
        <configuration>
          <scalaVersion>${scala.version}</scalaVersion>
          <args><arg>-target:jvm-1.8</arg><arg>-deprecation</arg><arg>-feature</arg></args>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration><skipTests>???</skipTests></configuration>   <!-- true -->
      </plugin>
      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
        <version>???</version>
        <executions>
          <execution><id>test</id><goals><goal>test</goal></goals></execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

<details><summary>Solution (versions indicatives, non résolues par moi : à vérifier chez toi)</summary>

- `scala.version` : la version complète relevée sur ton cluster (ex. `2.12.17`) ; `scala.binary.version` : `2.12`.
- `scalatest.version` : une `3.2.x` publiée pour Scala 2.12 (ex. `3.2.17`).
- `scala-maven-plugin` : une `4.8.x` (ex. `4.8.1`) ; `scalatest-maven-plugin` : `2.2.0`.
- `<scope>` ScalaTest : `test` ; `<skipTests>` surefire : `true`.

Si une version n'existe pas dans ton dépôt interne, `mvn` te le dit au premier build : choisis la plus proche disponible.
</details>

**(c) Résultat attendu.** `mvn -q compile` finit sans erreur ; un dossier `target/classes` apparaît avec les `.class`.

**(d) Piège provoqué.** Mets `scalatest_2.13` (au lieu de `${scala.binary.version}`) avec Scala 2.12. Résultat typique : erreur de compilation ou d'exécution des tests liée aux classes Scala incompatibles (**non exécuté, à observer chez toi**). Le suffixe `_2.12` d'un artefact **doit** égaler `scala.binary.version`.

**(e) Correction.** Toujours `_${scala.binary.version}` dans les `artifactId` Scala. Une seule propriété à changer, pas dix.

### Prédis avant de voir 2
Pourquoi met-on `<skipTests>true</skipTests>` sur surefire alors qu'on veut lancer les tests ?

<details><summary>Réponse</summary>

Surefire cherche des tests JUnit/TestNG et ne trouve pas les tests ScalaTest ; on le désactive pour que **seul** `scalatest-maven-plugin` exécute les tests, sans doublon ni faux « 0 test ».
</details>

---

## 5. Notion 3 — IntelliJ : SDK alignés, build et tests rapides

**(a) Concept.** IntelliJ a **trois** réglages qui doivent dire la même chose que le `pom.xml` : le **JDK du projet**, le **SDK Scala** et le JDK utilisé par **Maven**. Pour aller vite, on lance les tests **depuis IntelliJ** (compilation incrémentale du plugin Scala), et on garde `mvn` pour la vérification complète. (Menus à **vérifier chez toi**.)

**(b) Checklist à exécuter (squelette).**

1. Installer le plugin **Scala** (Settings, Plugins).
2. `File > Open` sur le **`pom.xml`** (pas sur le dossier) : « Open as Project ».
3. `File > Project Structure` : JDK du projet = `???` (aligné sur ton relevé).
4. Même écran : le SDK Scala détecté doit être `???` (= `scala.version` du pom). S'il propose une 2.13, **corrige** avant d'écrire une ligne.
5. Settings, Build Tools, Maven, Runner : JRE = le même JDK que l'étape 3.
6. Après toute modification du pom : « Reload All Maven Projects ».

**(c) Résultat attendu.** Aucune erreur rouge dans `Modele.scala` ; `Main` se lance via la flèche verte et affiche la sortie de la section 6 ; un clic droit sur `ParseurSpec` > Run lance les tests en quelques secondes.

**(d) Piège provoqué : le SDK Scala d'IntelliJ ne suit pas le pom.** Le code compile dans l'IDE mais pas avec `mvn`, ou l'inverse (« ça marche chez moi »). Symptôme à observer : `mvn -q test-compile` échoue alors que l'IDE est vert.

**(e) Correction.** Considère `mvn` comme **référence** : si `mvn clean test` passe, le projet est bon ; l'IDE sert à aller vite. Recharge le projet Maven et recale les SDK.

### Prédis avant de voir 3
Quelle commande Maven compile le code **et** les tests sans les exécuter ?

<details><summary>Réponse</summary>

`mvn test-compile`.
</details>

---

## 6. Notion 4 — Builder et lancer le projet (Scala pur)

**(a) Concept.** Séparer la **logique** (`Parseur`, `Pipeline`, testables) du **point d'entrée** (`Main`, fin, qui lit le fichier et affiche). C'est ce qui rend les tests possibles et, plus tard, la conversion vers Spark propre.

**(b) Code (les trois fichiers principaux, à retaper).**

`Modele.scala` :

```scala
package dev.scala101

case class Tx(id: String, clientId: String, montant: BigDecimal, devise: String, date: String)

sealed trait Resultat
case class Valide(tx: Tx) extends Resultat
case class Rejet(ligne: String, raison: String) extends Resultat
```

`Parseur.scala` : identique au blog 01 (`parse(ligne: String): Resultat`), dans `package dev.scala101`.

`Pipeline.scala` :

```scala
package dev.scala101

case class Rapport(lignes: Int, valides: Int, uniques: Int,
                   rejetsParRaison: Map[String, Int], total: BigDecimal)

object Pipeline {
  def run(lignes: Seq[String]): Rapport = {
    val resultats = lignes.map(Parseur.parse)
    val valides   = resultats.collect { case Valide(t) => t }
    val rejets    = resultats.collect { case r: Rejet => r }
    val uniques   = valides.groupBy(_.id).values.map(_.head).toSeq
    Rapport(
      lignes = ???, valides = ???, uniques = ???,
      rejetsParRaison = rejets.groupBy(_.raison).map { case (k, v) => k -> ??? },
      total = uniques.map(_.montant).sum
    )
  }
}
```

`Main.scala` (squelette) :

```scala
package dev.scala101

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val source = Source.fromResource("transactions.csv")
    val lignes = try source.getLines().toList finally ???
    val r = Pipeline.run(lignes)
    println(s"lignes=${r.lignes} valides=${r.valides} uniques=${r.uniques} total=${r.total}")
    r.rejetsParRaison.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"  rejet $k=$v") }
  }
}
```

<details><summary>Solution</summary>

```scala
Rapport(
  lignes = lignes.size, valides = valides.size, uniques = uniques.size,
  rejetsParRaison = rejets.groupBy(_.raison).map { case (k, v) => k -> v.size },
  total = uniques.map(_.montant).sum)
// Main : try source.getLines().toList finally source.close()
```

(On utilise `try ... finally` car `scala.util.Using` n'existe pas en 2.12.)
</details>

**(c) Résultat attendu (exécuté ici).**

```
lignes=15 valides=7 uniques=6 total=5370.75
  rejet client_inconnu=1
  rejet date_vide=1
  rejet devise=1
  rejet format=1
  rejet montant_invalide=2
  rejet montant_non_positif=2
```

| Contrôle | Valeur |
|---|---|
| valides + rejets | 7 + 8 = 15 = lignes |
| uniques | 6 (le doublon T001 est écarté) |
| total | 5370.75 (identique au blog 01) |

Pour lancer (à vérifier chez toi) : dans IntelliJ, flèche verte sur `Main` ; en ligne de commande, `mvn -q compile scala:run -DmainClass=dev.scala101.Main` (goal du `scala-maven-plugin`).

**(d) Piège provoqué : ressource introuvable.** Écris `Source.fromResource("transaction.csv")` (une faute de frappe). Résultat faux : exception à l'ouverture (fichier introuvable dans le classpath) au lieu d'un rapport. (**Non exécuté** ; l'erreur est classique, à reproduire.)

**(e) Correction.** Un fichier dans `src/main/resources` est copié dans `target/classes` ; le nom doit être exact. Mieux : tester que la ressource existe (section 7).

### Prédis avant de voir 4
Si on supprime le doublon T001 du fichier CSV, quels compteurs changent ?

<details><summary>Réponse</summary>

`lignes` passe à **14**, `valides` à **6**, `uniques` reste **6**, `total` reste **5370.75**. Les rejets ne changent pas.
</details>

---

## 7. Notion 5 — Tests ScalaTest et garde d'alignement

**(a) Concept.** Un test = une fonction pure + des entrées + une sortie attendue. On ajoute un test **« garde »** qui échoue si l'environnement n'est plus aligné : il protège contre le piège de la notion 1.

**(b) Squelette `ParseurSpec.scala` à compléter** (ScalaTest 3.2.x : **non exécuté**, à vérifier chez toi) :

```scala
package dev.scala101

import org.scalatest.funsuite.AnyFunSuite

class ParseurSpec extends AnyFunSuite {

  private val cas: Seq[(String, String)] = Seq(
    "T001,C01,1500.00,MAD,2026-10-01" -> "valide",
    "T003,C02,-300.00,MAD,2026-10-02" -> "montant_non_positif",
    "T004,C02,,MAD,2026-10-03"        -> "montant_invalide",
    "T005,C03,abc,MAD,2026-10-03"     -> "montant_invalide",
    "T006,C03,1000.00,EUR,2026-10-04" -> "devise",
    "T009,C99,400.00,MAD,2026-10-05"  -> "client_inconnu",
    "T010,C04,0.00,MAD,2026-10-05"    -> "montant_non_positif",
    "T012,C05,300.00,mad,2026-10-06"  -> "valide",
    "T013,C01,90.00,MAD"              -> "format",
    "T015,C02,50.00,MAD,"             -> "date_vide"
  )

  private def raison(r: Resultat): String = r match {
    case Valide(_)   => "valide"
    case Rejet(_, x) => ???
  }

  cas.foreach { case (ligne, attendu) =>
    test(s"$ligne => $attendu") {
      assert(raison(Parseur.parse(ligne)) == ???)
    }
  }

  test("pipeline : totaux de reference") {
    val r = Pipeline.run(scala.io.Source.fromResource("transactions.csv").getLines().toList)
    assert(r.lignes == ???)
    assert(r.total == BigDecimal("???"))
  }

  test("garde : Scala binaire 2.12") {
    assert(scala.util.Properties.versionNumberString.startsWith("???"))
  }
}
```

<details><summary>Solution</summary>

```scala
case Rejet(_, x) => x
assert(raison(Parseur.parse(ligne)) == attendu)
assert(r.lignes == 15)
assert(r.total == BigDecimal("5370.75"))
assert(scala.util.Properties.versionNumberString.startsWith("2.12"))
```
</details>

**(c) Résultat attendu.**

| Test | Résultat |
|---|---|
| 10 tests de règles | 10 passent |
| Pipeline (15 lignes, total 5370.75) | passe (valeurs **exécutées** par moi en pur Scala) |
| Garde 2.12 | passe ; la condition évaluée ici vaut `true` (Scala 2.12.20) |
| **Total** | **12 tests** (non exécutés sous ScalaTest : à lancer chez toi avec `mvn test`) |

**(d) Piège provoqué : on désaligne.** Mets `scala.version` en 2.13.x et `scala.binary.version` en 2.13. Résultat attendu : le test « garde » **échoue** (`startsWith("2.12")` est faux), alors que le reste peut passer. C'est justement son rôle : détecter le désalignement **avant** le cluster. (Logique simple, mais **non exécutée** sous Maven.)

**(e) Correction.** Remettre 2.12. Garde-le dans le projet pour toujours.

### Prédis avant de voir 5
Combien de tests ScalaTest vas-tu voir passer : 10, 11 ou 12 ?

<details><summary>Réponse</summary>

**12** : 10 (règles) + 1 (pipeline) + 1 (garde).
</details>

---

## 8. Diagnostic de l'incident

**Cause racine du « chez moi ça marche »** : environnements non décrits dans le projet (JDK, Scala, versions des plugins, IDE) et aucune vérification automatique.

**Correction** : un `pom.xml` qui fixe `scala.version`, `scala.binary.version`, la cible bytecode et les versions de plugins ; une garde de version dans les tests ; une procédure unique : `mvn clean test` doit passer partout.

**Contrôles qui l'auraient détecté :**

| Contrôle | Attendu |
|---|---|
| `mvn -v` : JDK utilisé par Maven = JDK du projet IntelliJ | identiques |
| Scala du pom = Scala du cluster | identiques |
| Suffixe des artefacts Scala = `scala.binary.version` | `_2.12` partout |
| Test garde de version | vert |
| `mvn clean test` sur une machine vierge | vert |

---

## 9. Tableau Avant / Après (à remplir avec **tes** mesures)

| Mesure | Avant (1er build à froid) | Après (2e build, incrémental) |
|---|---|---|
| `mvn clean test` : durée | | |
| `mvn -o -q test` (hors-ligne) : durée | | |
| Test lancé depuis IntelliJ (clic droit, Run) : durée | | |
| Nombre de tests exécutés | | |

Je ne donne volontairement aucun chiffre : il dépend de ta machine et de ton dépôt Maven.

---

## 10. Dix questions d'entretien

1. **Pourquoi la version de Scala (2.12 vs 2.13) est-elle critique ?** Les versions *binaires* ne sont pas compatibles entre elles : un jar compilé en 2.13 n'est pas utilisable avec une librairie 2.12.
2. **Que signifie le suffixe `_2.12` d'un artefact ?** La version binaire de Scala pour laquelle il est compilé.
3. **Pourquoi surefire est-il désactivé avec ScalaTest ?** Il ne détecte pas les tests ScalaTest ; le plugin ScalaTest les lance.
4. **Qu'est-ce que la cible de bytecode ?** La version JVM minimale requise par le jar (major 52 = Java 8, 55 = Java 11, 61 = Java 17).
5. **Pourquoi aligner sur le cluster et pas sur le poste ?** Le code s'exécutera sur le cluster : c'est lui qui impose ses versions.
6. **Différence entre `mvn compile`, `test-compile`, `test`, `package` ?** Compile le code ; compile code et tests ; exécute les tests ; produit le jar.
7. **À quoi sert `src/main/resources` ?** Aux fichiers copiés dans le classpath (`target/classes`) et lus avec `Source.fromResource`.
8. **Pourquoi séparer `Main` de la logique ?** Pour tester la logique sans I/O et, plus tard, la réutiliser telle quelle.
9. **Pourquoi un test « garde » de version ?** Il échoue dès qu'un développeur désaligne l'environnement.
10. **« Ça marche chez moi » : ta méthode ?** Fixer les versions dans le pom, vérifier avec `mvn clean test` sur une machine propre, et documenter les prérequis.

---

## 11. Cinq exercices

**Exercice 1.** Relève et note : version complète de Scala du cluster, JDK du cluster, JDK de ton poste. Attendu : trois valeurs écrites dans l'onglet « Alignement environnement ».

<details><summary>Solution</summary>

`scala.util.Properties.versionString` et `System.getProperty("java.version")` dans le shell du cluster ; `java -version` et `mvn -v` sur le poste.
</details>

**Exercice 2.** Ajoute dans `Pipeline` un champ `rejets: Int` au `Rapport`. Attendu sur le fil rouge : `rejets = 8`, et `valides + rejets == lignes`.

<details><summary>Solution</summary>

```scala
case class Rapport(lignes: Int, valides: Int, uniques: Int, rejets: Int,
                   rejetsParRaison: Map[String, Int], total: BigDecimal)
// dans run : rejets = rejets.size
```
Contrôle : 7 + 8 = 15.
</details>

**Exercice 3.** Écris le test `"valides + rejets == lignes"` sur le fichier de référence. Attendu : il passe (15 = 7 + 8).

<details><summary>Solution</summary>

```scala
test("aucune ligne perdue") {
  val r = Pipeline.run(scala.io.Source.fromResource("transactions.csv").getLines().toList)
  assert(r.valides + r.rejets == r.lignes)
}
```
</details>

**Exercice 4.** Casse volontairement le projet : change `scala.version` en 2.13.x. Note quels tests échouent. Attendu : au minimum le test garde.

<details><summary>Solution</summary>

Le test « garde : Scala binaire 2.12 » échoue. Remets 2.12.x puis `mvn clean test`. (Autres échecs possibles selon tes dépendances : à observer chez toi.)
</details>

**Exercice 5 : refais sans regarder.** Dans un **nouveau** projet vide `compta101`, recrée de mémoire : l'arborescence, un `pom.xml` Scala 2.12 avec ScalaTest, une `case class Paiement`, un `object Validateur.valider(ligne: String): Resultat` et 3 tests (une ligne valide, un montant `abc`, une ligne tronquée). Attendu : `mvn clean test` = 3 tests verts.

<details><summary>Solution</summary>

Mêmes briques que ce blog, avec `Paiement(id, marchand, montant)` ; `valider` suit le modèle de `Parseur.parse` (`split(",", -1)`, `Try(BigDecimal(...))`, `case _ => Rejet(...)`).
</details>

---

## 12. À retaper de mémoire (signatures seules)

```scala
object Pipeline { def run(lignes: Seq[String]): Rapport = ??? }
case class Rapport(???)
class ParseurSpec extends AnyFunSuite { ??? }
```

## 13. Checklist avant de livrer

- [ ] `scala.version` du pom = Scala du cluster, `scala.binary.version` = 2.12.
- [ ] Tous les artefacts Scala utilisent `_${scala.binary.version}`.
- [ ] Cible bytecode : pas plus récente que le JDK du cluster.
- [ ] JDK du projet IntelliJ = JDK de Maven (`mvn -v`).
- [ ] `mvn clean test` passe sur une machine propre.
- [ ] Le test garde de version existe.
- [ ] `Main` est fin ; la logique est dans des `object` testables.
- [ ] Aucun usage d'API 2.13 (`Using`, `toDoubleOption`...).

## 14. Blogs restants (nouvelle feuille de route : Scala pur, puis conversion vers Spark)

| # | Sujet | Phase |
|---|---|---|
| 01 | Bases Scala (avec tests et bonnes pratiques) ✅ | Scala pur |
| 02 | Environnement Maven, IntelliJ, tests ✅ (ce blog) | Scala pur |
| 03 | Collections et style fonctionnel (`map`, `flatMap`, `fold`, `groupBy`) | Scala pur |
| 04 | `Option`, `Either`, `Try` et gestion d'erreurs | Scala pur |
| 05 | **POO** : classes, objets, companions, héritage, classes abstraites, traits, polymorphisme | Scala pur |
| 06 | Génériques, variance, type classes, `implicit` (2.12) | Scala pur |
| 07 | Scala avancé utile : `for`, `lazy val`, by-name, récursion terminale, `Iterator` | Scala pur |
| 08 | Tests niveau pro (ScalaTest, ScalaCheck) et bonnes pratiques (structure, lint, logging, config) | Scala pur |
| 09 | Fichiers, I/O et concurrence de base (`Future`) | Scala pur |
| 10 | Niveau tech lead : multi-modules Maven, versioning, packaging, revue de code | Scala pur |
| 11 | **Conversion 1** : la même `Pipeline` sur des collections Scala → premier job Spark (ajout de la dépendance en `provided`) | Vers Spark |
| 12 | **Conversion 2** : de RDD vers DataFrame/Dataset typé avec les mêmes `case class` | Vers Spark |
| 13 | **Conversion 3** : `spark-submit` sur YARN, tests avec `SparkSession` locale, closures et sérialisation | Vers Spark |

## 15. Échauffement pour le blog 03

Sans exécuter : combien de clients **distincts** y a-t-il parmi les lignes à 5 champs du fil rouge (`split(",", -1)`) ? Réponds sur papier, puis vérifie dans le shell.