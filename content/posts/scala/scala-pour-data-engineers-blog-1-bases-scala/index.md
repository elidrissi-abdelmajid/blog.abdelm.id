---
title: "Scala pour Data Engineers, Blog 1 : le total de la Finance est faux"
description: "Découvrir les bases de Scala avec un exemple de réconciliation financière, un parseur robuste et des tests unitaires."
weight: 1
---

# Blog 01 — Scala de zéro : le total de la Finance est faux (version 2, avec tests)

> Durée : 35 à 45 min · Niveau : débutant complet · Cible : Scala 2.12 / Spark 3.3.2 (Cloudera 7.1.9)
> Cette version remplace la première : elle part vraiment de zéro, ajoute les **tests unitaires** et les **bonnes pratiques**.

## Ce qui a été vérifié, et ce qui ne l'a pas été

| Élément | Statut |
|---|---|
| Tout le code Scala et tous les chiffres de ce blog | **Exécutés** en Scala 2.12.20 sur JDK 21 (environnement de rédaction) |
| Les tests « maison » (section 9) et leur échec provoqué (9/10) | **Exécutés** (même environnement) |
| Messages d'erreur (`reassignment to val`, `MatchError`, `NumberFormatException`) | **Exécutés** |
| Le squelette ScalaTest (section 9.3) | **Non exécuté** (pas d'accès à Maven Central). À vérifier chez toi au blog 02 : version de ScalaTest et plugin Maven |
| Comportement sur le JDK/Scala exact de ton cluster | **À vérifier chez toi** : `scala.util.Properties.versionString` et `System.getProperty("java.version")` dans `spark3-shell` |
| Nom de la commande (`spark3-shell` vs `spark-shell`), collage avec `:paste` | **À vérifier chez toi** |

Rappel Scala 2.12 : `toDoubleOption` n'existe pas (2.13 seulement) et `mapValues` est une vue paresseuse. Le code évite ces pièges.

**Comment exécuter** : ouvre `spark3-shell`, tape `:paste`, colle un bloc, Ctrl-D. Au blog 02 on passera à un vrai projet Maven dans IntelliJ.

---

## 1. Le scénario

**Lundi 9h.** La Finance t'appelle : « Le dashboard affiche **7970.75 MAD** de transactions pour la semaine. Notre calcul manuel donne **5370.75 MAD**. Qui a raison ? »

Tout le blog sert à comprendre l'écart de **2600.00 MAD**, à le réconcilier ligne par ligne, puis à écrire un parseur **testé** qui ne se trompe plus.

---

## 2. Le jeu de données fil rouge (réutilisé dans les blogs suivants)

15 lignes, volontairement sales, assez petit pour tout recalculer à la main. Format : `id,client,montant,devise,date`.

```scala
val raw: Seq[String] = Seq(
  "T001,C01,1500.00,MAD,2026-10-01",
  "T002,C01,200.50,MAD,2026-10-02",
  "T003,C02,-300.00,MAD,2026-10-02",
  "T004,C02,,MAD,2026-10-03",
  "T005,C03,abc,MAD,2026-10-03",
  "T006,C03,1000.00,EUR,2026-10-04",
  "T007,C04,750.25,MAD,03/10/2026",
  "T001,C01,1500.00,MAD,2026-10-01",
  "T009,C99,400.00,MAD,2026-10-05",
  "T010,C04,0.00,MAD,2026-10-05",
  "T011,C02,2500.00,MAD,2026-10-06",
  "T012,C05,300.00,mad,2026-10-06",
  "T013,C01,90.00,MAD",
  "T014,C03,120.00,MAD,2026-10-07",
  "T015,C02,50.00,MAD,"
)
```

| Ligne | Anomalie plantée | Pourquoi elle est là |
|---|---|---|
| T003 | montant négatif | remboursement ou erreur ? règle métier à trancher |
| T004 | montant vide | piège du `toDouble`/`BigDecimal` sur `""` |
| T005 | montant `abc` | donnée non numérique |
| T006 | devise EUR | total en MAD : mélange de devises |
| T007 | date `03/10/2026` | format de date différent (exercice 3) |
| 2e T001 | doublon exact | piège `class` vs `case class` |
| T009 | client `C99` inconnu | orpheline (réutilisée en jointure plus tard) |
| T010 | montant 0.00 | zéro : valide ou non ? |
| T012 | devise `mad` | casse |
| T013 | 4 champs au lieu de 5 | ligne tronquée |
| T015 | date vide en fin de ligne | piège du `split` |

**Règles métier retenues** (tu peux les contester en entretien) : montant strictement positif, devise MAD (après mise en majuscules), client connu parmi `C01` à `C05`, date non vide, doublon = même `id`.

---

## 3. Notion 0 — Scala de zéro

**(a) Concept.** En Scala presque tout est une **expression** qui renvoie une valeur (même un `if`). `val` = référence non réassignable, `var` = réassignable. Les types sont **inférés**. On écrit une fonction avec `def nom(param: Type): TypeRetour = corps`. On préfère `val` : un calcul qui ne mute rien est plus simple à raisonner et à paralléliser.

**(b) Code à compléter (squelette).**

```scala
val nom = "Finance"
val n = 3
val phrase = s"Bonjour ???, il y a ${???} lignes"          // doit afficher 15 : n * 5
def carre(x: Int): Int = ???
val signe = if (n > 0) ??? else "negatif"
println(phrase + " | carre(7)=" + carre(7) + " | signe=" + signe)
println(5 / 2); println(5 / 2.0); println(5 % 2)
var compteur = ???
raw.foreach(_ => ???)
println(s"compteur=$compteur lignes=${raw.size}")
```

**(c) Résultat attendu.**

| Expression | Résultat |
|---|---|
| `phrase` | `Bonjour Finance, il y a 15 lignes` |
| `carre(7)` | 49 |
| `signe` | `positif` |
| `5 / 2`, `5 / 2.0`, `5 % 2` | 2, 2.5, 1 |
| `compteur` / `raw.size` | 15 / 15 |

**(d) Pièges provoqués.**

| Code | Résultat faux |
|---|---|
| `val total = 0; total = total + 1` | erreur de compilation : `reassignment to val` |
| `5 / 2` quand on attendait 2.5 | **2** : division **entière** entre deux `Int` |

**(e) Correction.** Ne pas réassigner un `val` : produis une nouvelle valeur (`val total2 = total + 1`). Pour une division décimale, un des deux opérandes doit être décimal (`5 / 2.0`). Plus tard (blog 08) : un `var` modifié dans une closure Spark ne se comporte pas comme tu le crois.

<details><summary>Solution</summary>

```scala
val phrase = s"Bonjour $nom, il y a ${n * 5} lignes"
def carre(x: Int): Int = x * x
val signe = if (n > 0) "positif" else "negatif"
var compteur = 0
raw.foreach(_ => compteur += 1)
```
</details>

### Prédis avant de voir 1
Combien vaut `raw.count(_.nonEmpty)` ?

<details><summary>Réponse</summary>

**15** : aucune ligne n'est vide, même les lignes sales. Une ligne « sale » n'est pas une ligne vide.
</details>

---

## 4. Notion 1 — `split` et le piège des champs vides

**(a) Concept.** `String.split(",")` (comportement Java) **supprime les champs vides en fin de ligne**. Avec `split(",", -1)` on les garde.

**(b) Code à compléter.**

```scala
val l15 = "T015,C02,50.00,MAD,"
println(l15.split(???).length)
println(l15.split(",", ???).length)
println(raw.count(_.split(",").length == 5))
println(raw.count(_.split(",", -1).length == 5))
```

**(c) Résultat attendu (le correct).**

| Expression | Résultat |
|---|---|
| `l15.split(",", -1).length` | 5 |
| `raw.count(_.split(",", -1).length == 5)` | 14 (T013 est la seule ligne vraiment tronquée) |

**(d) Piège provoqué (résultat faux).**

| Expression | Résultat faux |
|---|---|
| `l15.split(",").length` | **4** : le champ date vide a disparu |
| `raw.count(_.split(",").length == 5)` | **13** : T015 est confondue avec une ligne tronquée |

**(e) Correction.** Toujours `split(",", -1)` pour un CSV brut, puis tester la longueur.

### Prédis avant de voir 2
Avec `split(",")` (sans `-1`), que vaut `"T004,C02,,MAD,2026-10-03".split(",").length` : 4 ou 5 ?

<details><summary>Réponse</summary>

**5.** Le champ vide est **au milieu** : seuls les vides **de fin** sont supprimés. C'est pourquoi T004 passe le filtre naïf puis fait échouer la conversion en nombre.
</details>

---

## 5. Notion 2 — `Double` ou `BigDecimal` pour de l'argent

**(a) Concept.** `Double` est binaire (IEEE 754) : 0.1 n'est pas représentable exactement. Pour de l'argent, utilise `BigDecimal` construit depuis une `String`.

**(b) Code à compléter.**

```scala
println(0.1 + 0.2)
println(BigDecimal("0.1") + BigDecimal("0.2"))
println(List.fill(10)(0.1).sum)
println(List.fill(10)(BigDecimal("0.1")).sum)
```

**(c) Résultat attendu avec `BigDecimal`.**

| Expression | Résultat |
|---|---|
| `BigDecimal("0.1") + BigDecimal("0.2")` | 0.3 |
| somme de 10 × `BigDecimal("0.1")` | 1.0 |

**(d) Piège provoqué (résultat faux avec `Double`).**

| Expression | Résultat faux |
|---|---|
| `0.1 + 0.2` | **0.30000000000000004** |
| somme de 10 × `0.1` | **0.9999999999999999** |

**(e) Correction.** `BigDecimal("0.1")`, jamais `BigDecimal(0.1)` depuis un `Double` déjà imprécis.

### Prédis avant de voir 3
`BigDecimal("abc")` renvoie-t-il `null`, `0`, ou lève-t-il une exception ?

<details><summary>Réponse</summary>

Il lève `java.lang.NumberFormatException`. Idem pour `BigDecimal("")`. Il faut donc **attraper** l'erreur : `Try(BigDecimal(m)).toOption` (`Try` et `Option` sont détaillés au blog 04).
</details>

---

## 6. Notion 3 — `case class` vs `class` : le doublon qui survit

**(a) Concept.** Une `case class` génère `equals`, `hashCode`, `toString`, `copy`. Deux instances aux champs identiques sont **égales**. Une `class` simple compare par **référence**.

**(b) Code à compléter.**

```scala
case class Tx(id: String, clientId: String, montant: BigDecimal, devise: String, date: String)
class TxPlain(val id: String, val clientId: String, val montant: BigDecimal,
              val devise: String, val date: String)
// valides : les 7 transactions valides (section 7)
// 1. valides.distinct.size
// 2. valides.map(t => new TxPlain(t.id, t.clientId, t.montant, t.devise, t.date)).distinct.size
```

**(c) Résultat attendu (le correct).**

| Expression | Résultat |
|---|---|
| `valides.size` | 7 |
| `valides.distinct.size` (case class) | **6** |
| `valides(0) == valides(3)` (T001 et son doublon) | `true` |

**(d) Piège provoqué (résultat faux).**

| Expression | Résultat faux |
|---|---|
| `distinct.size` avec `class` simple | **7** : le doublon survit |
| `plain(0) == plain(3)` | `false` |

**(e) Correction.** `case class`. Écart : 7 → 6 lignes, soit **1500.00 MAD** en moins (le doublon T001).

### Prédis avant de voir 4
Si le doublon de T001 avait une date différente, `distinct` sur la `case class` l'éliminerait-il encore ?

<details><summary>Réponse</summary>

**Non** : `distinct` compare **tous** les champs. On dédoublonne donc par **`id`** (clé métier) avec `groupBy(_.id)`.
</details>

---

## 7. Notion 4 — Pattern matching et ADT : le parseur robuste

**(a) Concept.** `match` décompose une valeur selon sa forme. Un `sealed trait` + des `case class` modélisent le résultat : `Valide` ou `Rejet(raison)`. Le compilateur te prévient si un cas manque. (Les `trait` sont approfondis au blog 05.)

**(b) Code à compléter (squelette).** Remarque bonne pratique : la logique est dans un `object` **sans effet de bord** (aucun `println`, aucun `var`) pour pouvoir la tester.

```scala
import scala.util.Try

object Modele {
  case class Tx(id: String, clientId: String, montant: BigDecimal, devise: String, date: String)
  sealed trait Resultat
  case class Valide(tx: Tx) extends Resultat
  case class Rejet(ligne: String, raison: String) extends Resultat
}

object Parseur {
  import Modele._
  val ClientsConnus: Set[String] = Set("C01", "C02", "C03", "C04", "C05")

  def parse(ligne: String): Resultat = ligne.split(",", -1) match {
    case Array(id, client, m, dev, date) =>
      if (???) Rejet(ligne, "date_vide")
      else Try(BigDecimal(m)).toOption match {
        case None                              => ???
        case Some(v) if v <= 0                 => ???
        case Some(_) if ???                    => Rejet(ligne, "devise")
        case Some(_) if ???                    => Rejet(ligne, "client_inconnu")
        case Some(v)                           => Valide(???)
      }
    case _ => ???
  }
}
```

<details><summary>Solution</summary>

```scala
def parse(ligne: String): Resultat = ligne.split(",", -1) match {
  case Array(id, client, m, dev, date) =>
    if (date.isEmpty) Rejet(ligne, "date_vide")
    else Try(BigDecimal(m)).toOption match {
      case None                                => Rejet(ligne, "montant_invalide")
      case Some(v) if v <= 0                   => Rejet(ligne, "montant_non_positif")
      case Some(_) if dev.toUpperCase != "MAD" => Rejet(ligne, "devise")
      case Some(_) if !ClientsConnus(client)   => Rejet(ligne, "client_inconnu")
      case Some(v)                             => Valide(Tx(id, client, v, dev.toUpperCase, date))
    }
  case _ => Rejet(ligne, "format")
}
```
</details>

**(c) Résultat attendu.**

```scala
import Modele._
val resultats = raw.map(Parseur.parse)
val valides   = resultats.collect { case Valide(t) => t }
val rejets    = resultats.collect { case r: Rejet => r }
```

| Raison de rejet | Nb | Lignes |
|---|---|---|
| montant_non_positif | 2 | T003, T010 |
| montant_invalide | 2 | T004, T005 |
| devise | 1 | T006 |
| client_inconnu | 1 | T009 |
| format | 1 | T013 |
| date_vide | 1 | T015 |
| **Total rejets** | **8** | |

**(d) Piège provoqué : le `match` sans cas par défaut.**

```scala
"T013,C01,90.00,MAD".split(",") match { case Array(a, b, c, d, e) => a }
```

Résultat faux : `scala.MatchError` (le job entier plante sur une seule ligne tronquée).

**(e) Correction.** Le `case _ => Rejet(ligne, "format")` final : la ligne tronquée devient un rejet **tracé**, pas un crash.

### Prédis avant de voir 5
`valides.size + rejets.size` doit être égal à combien ?

<details><summary>Réponse</summary>

**15** = `raw.size`. Aucune ligne ne doit disparaître : chaque ligne est valide, ou rejetée avec une raison. Exécution : 7 + 8 = 15.
</details>

---

## 8. Le diagnostic de l'incident

**Calcul correct** : dédoublonner les valides par `id` (premier gardé), puis additionner.

```scala
val uniques = valides.groupBy(_.id).values.map(_.head).toSeq
val totalOK = uniques.map(_.montant).sum
```

| Étape | Lignes |
|---|---|
| Lignes brutes | 15 |
| Valides | 7 |
| Uniques (sans doublon T001) | 6 |
| Rejets | 8 |
| **Total correct** | **5370.75 MAD** |

À la main : 1500.00 (T001) + 200.50 (T002) + 750.25 (T007) + 2500.00 (T011) + 300.00 (T012) + 120.00 (T014) = **5370.75**. La Finance a raison.

**Le job naïf** (celui du dashboard) :

```scala
import scala.util.Try
raw.map(_.split(","))
   .filter(_.length == 5)
   .map(a => Try(a(2).toDouble).getOrElse(0.0))
   .sum
```

Il donne **7970.75**. Réconciliation de l'écart **2600.00** :

| Cause | Effet sur le total |
|---|---|
| Doublon T001 compté deux fois | + 1500.00 |
| T006 en EUR additionné avec des MAD | + 1000.00 |
| T009, client inconnu C99 | + 400.00 |
| T003, montant négatif | − 300.00 |
| T004, T005, T010 (vide, `abc`, zéro) | 0.00 (`getOrElse(0.0)` masque l'erreur) |
| **Total de l'écart** | **+ 2600.00** |

Vérification : 1500 + 1000 + 400 − 300 = 2600 ; 7970.75 − 5370.75 = 2600.00. ✔

**Cause racine :** un `getOrElse(0.0)` qui avale les erreurs de parsing, aucune règle métier, aucune trace des rejets.

**Contrôles qui l'auraient détecté :**

| Contrôle | Attendu |
|---|---|
| `valides.size + rejets.size == raw.size` | vrai (15) |
| Doublons d'`id` parmi les valides après correction | 0 |
| `uniques.map(_.devise).distinct` | `Seq("MAD")` |
| Réconciliation avec un total calculé autrement (main, ou SQL sur la source) | écart = 0 |
| Rejets par raison publiés à chaque run | alerte si une raison explose |

Total par client (uniques) : C01 = 1700.50, C02 = 2500.00, C03 = 120.00, C04 = 750.25, C05 = 300.00 (somme = 5370.75).

---

## 9. Notion 5 — Tests unitaires : le filet de sécurité

### 9.1 Concept
Un test unitaire vérifie **une fonction pure** sur des entrées connues avec des sorties attendues. Un **test piloté par table** (une liste `(entrée, attendu)`) est idéal pour un parseur : une ligne par règle métier.

### 9.2 Version sans framework (exécutable dans `spark3-shell`)

**(b) Squelette à compléter.**

```scala
import Modele._

var echecs = 0
def verifier(nom: String, cond: Boolean): Unit =
  if (cond) println(s"  OK     $nom") else { echecs += 1; println(s"  ECHEC  $nom") }

def raison(r: Resultat): String = r match {
  case Valide(_)   => "valide"
  case Rejet(_, x) => ???
}

val cas: Seq[(String, String)] = Seq(
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
cas.foreach { case (ligne, attendu) => verifier(ligne + " => " + attendu, ???) }
println(s"=> ${cas.size - echecs}/${cas.size} OK")
```

**(c) Résultat attendu** (parseur correct) : `10/10 OK, 0 échec`.

**(d) Piège provoqué : on casse volontairement le parseur.** On remplace `split(",", -1)` par `split(",")` (le bug de la notion 1) :

| Parseur | Résultat des tests |
|---|---|
| Correct (`split(",", -1)`) | **10/10 OK** |
| Bugué (`split(",")`) | **9/10 OK**, 1 échec : `T015,C02,50.00,MAD,` donne `format` au lieu de `date_vide` |

**Point important :** avec le parseur bugué, le **total reste 5370.75** (T015 est rejetée dans les deux cas), mais les rejets deviennent `format = 2` au lieu de `format = 1` + `date_vide = 1`. **La réconciliation du total ne détecte pas ce bug ; le test, si.** Les contrôles de totaux et les tests unitaires sont complémentaires.

**(e) Correction.** Remettre `split(",", -1)` : retour à 10/10.

<details><summary>Solution du squelette</summary>

```scala
def raison(r: Resultat): String = r match {
  case Valide(_)   => "valide"
  case Rejet(_, x) => x
}
cas.foreach { case (ligne, attendu) =>
  verifier(ligne + " => " + attendu, raison(Parseur.parse(ligne)) == attendu)
}
```
</details>

### Prédis avant de voir 6
Si on supprime la règle `devise` du parseur, quels tests échouent, et quel total obtient-on ?

<details><summary>Réponse</summary>

Un seul test échoue : celui de T006 (`devise` attendu, `valide` obtenu). T006 (EUR, 1000.00) devient valide : le total passe de 5370.75 à **6370.75** (+ 1000.00). Raisonnement à la main, non exécuté ; tu peux le vérifier chez toi en supprimant la ligne.
</details>

### 9.3 Aperçu du même test avec ScalaTest (squelette, non exécuté)
Au blog 02 on installera Maven et ScalaTest. À titre d'aperçu (**à vérifier chez toi** : version de ScalaTest compatible Scala 2.12 et plugin Maven) :

```scala
import org.scalatest.funsuite.AnyFunSuite

class ParseurSpec extends AnyFunSuite {
  test("un montant négatif est rejeté") {
    assert(Parseur.parse("T003,C02,-300.00,MAD,2026-10-02") ==
      Modele.Rejet("T003,C02,-300.00,MAD,2026-10-02", "montant_non_positif"))
  }
  // un test par règle : devise, client_inconnu, date_vide, format...
}
```

---

## 10. Bonnes pratiques (à retenir)

| Pratique | Pourquoi (vu dans ce blog) |
|---|---|
| `val` par défaut, `var` seulement si indispensable | moins d'états cachés |
| Logique pure dans un `object` sans `println` ni `var` | testable sans cluster |
| `split(",", -1)` pour un CSV brut | évite la perte de champs vides |
| `BigDecimal` depuis une `String` pour l'argent | pas d'erreur d'arrondi binaire |
| `case class` pour les données, `sealed trait` pour les résultats | égalité structurelle, exhaustivité |
| Jamais `getOrElse(0)` pour masquer une erreur | un chiffre faux sans alerte |
| Un cas par défaut (`case _`) dans les `match` sur données externes | pas de crash sur une ligne sale |
| Chaque rejet porte une raison et `valides + rejets == total` | traçabilité et contrôle |
| Un test par règle métier, avant de corriger un bug | le test reproduit le bug |
| Constantes nommées (`ClientsConnus`) | pas de valeurs magiques dans le code |

---

## 11. Tableau Avant / Après

Pas de mesure de performance ici. Remplis avec **tes** résultats dans `spark3-shell` :

| Mesure | Ma valeur |
|---|---|
| Version Scala du cluster | |
| Version Java du cluster | |
| Total naïf obtenu chez moi | |
| Total correct obtenu chez moi | |
| Tests : résultat parseur correct / parseur bugué | |

---

## 12. Dix questions d'entretien

1. **`val` vs `var` ?** `val` n'est pas réassignable, `var` oui. On préfère `val` (immutabilité, code plus sûr en parallèle).
2. **`class` vs `case class` ?** La `case class` génère `equals`, `hashCode`, `toString`, `copy`, `apply` et `unapply` (pattern matching). Égalité structurelle contre égalité par référence.
3. **Pourquoi `BigDecimal` pour l'argent ?** `Double` est binaire : 0.1 + 0.2 donne 0.30000000000000004.
4. **Que fait `"a,b,,".split(",")` ?** Il renvoie 2 éléments : les vides de fin sont supprimés (comportement Java). `split(",", -1)` les garde.
5. **Que se passe-t-il si un `match` ne couvre pas tous les cas ?** `MatchError` à l'exécution ; avec un `sealed trait`, le compilateur avertit.
6. **À quoi sert `sealed` ?** Restreint les sous-types au même fichier : le compilateur vérifie l'exhaustivité.
7. **Pourquoi `Try(...).getOrElse(0.0)` est dangereux ?** Il transforme une erreur en valeur plausible : un chiffre faux sans alerte.
8. **Comment savoir qu'aucune ligne n'a disparu ?** `valides.size + rejets.size == raw.size`.
9. **Qu'est-ce qu'un bon test unitaire ?** Il teste une fonction pure, est rapide, indépendant, avec des entrées/sorties connues, et échoue pour la bonne raison.
10. **Un total correct prouve-t-il que le code est correct ?** Non : ici le parseur bugué donne le même total 5370.75 mais des raisons de rejet fausses. Totaux et tests se complètent.

---

## 13. Cinq exercices

**Exercice 1.** Compte les lignes à 5 champs par devise (en majuscules). Attendu : `EUR = 1`, `MAD = 13`.

<details><summary>Solution</summary>

```scala
raw.map(_.split(",", -1)).filter(_.length == 5)
   .groupBy(_(3).toUpperCase).mapValues(_.size).toSeq.sortBy(_._1)
```
Résultat : `Vector((EUR,1), (MAD,13))`.
</details>

**Exercice 2.** Total par client sur les `uniques`. Attendu : C01 = 1700.50, C02 = 2500.00, C03 = 120.00, C04 = 750.25, C05 = 300.00.

<details><summary>Solution</summary>

```scala
uniques.groupBy(_.clientId).mapValues(_.map(_.montant).sum).toSeq.sortBy(_._1)
```
</details>

**Exercice 3.** Ajoute la règle « date au format `yyyy-MM-dd` » (nouvelle raison `date_format`). Attendu : 9 rejets, 6 valides, 5 uniques, total **4620.50**, ligne rejetée : T007.

<details><summary>Solution</summary>

```scala
def parse3(ligne: String): Resultat = Parseur.parse(ligne) match {
  case Valide(t) if !t.date.matches("""\d{4}-\d{2}-\d{2}""") => Rejet(ligne, "date_format")
  case r => r
}
```
Contrôle : 5370.75 − 750.25 = 4620.50 ✔.
</details>

**Exercice 4 (test).** Écris les 3 tests de la règle de l'exercice 3 avec ta fonction `verifier`. Attendus : T007 → `date_format`, T001 → `valide`, T015 → `date_vide` (le vide doit rester `date_vide`, pas `date_format`). Résultat : 3/3 OK.

<details><summary>Solution</summary>

```scala
Seq(
  "T007,C04,750.25,MAD,03/10/2026"  -> "date_format",
  "T001,C01,1500.00,MAD,2026-10-01" -> "valide",
  "T015,C02,50.00,MAD,"             -> "date_vide"
).foreach { case (l, att) => verifier(l + " => " + att, raison(parse3(l)) == att) }
```
</details>

**Exercice 5 : refais sans regarder, sur un cas voisin.**
Ferme ce blog. Écris de mémoire un parseur et sa table de tests pour des paiements carte au format `id,marchand,montant,devise,date`, marchands connus `M01` et `M02`, rejets tracés, dédoublonnage par `id`. Données :

```
P1,M01,25.50,MAD,2026-11-01
P2,M01,abc,MAD,2026-11-01
P3,M02,40.00,mad,2026-11-02
P1,M01,25.50,MAD,2026-11-01
P4,M99,10.00,MAD,2026-11-02
P5,M02,15.00,MAD
```

Attendu : 3 valides, 2 uniques, 3 rejets (`montant_invalide`, `marchand_inconnu`, `format`), total **65.50**.

<details><summary>Solution</summary>

```scala
def parseP(connus: Set[String])(ligne: String): Resultat = ligne.split(",", -1) match {
  case Array(id, m, v, dev, date) =>
    if (date.isEmpty) Rejet(ligne, "date_vide")
    else Try(BigDecimal(v)).toOption match {
      case None                  => Rejet(ligne, "montant_invalide")
      case Some(x) if x <= 0     => Rejet(ligne, "montant_non_positif")
      case Some(_) if !connus(m) => Rejet(ligne, "marchand_inconnu")
      case Some(x)               => Valide(Tx(id, m, x, dev.toUpperCase, date))
    }
  case _ => Rejet(ligne, "format")
}
```
</details>

---

## 14. À retaper de mémoire (signatures seules)

```scala
case class Tx(???)
sealed trait Resultat
object Parseur { def parse(ligne: String): Resultat = ??? }
def verifier(nom: String, cond: Boolean): Unit = ???
val uniques: Seq[Tx] = ???
```

## 15. Checklist avant de livrer

- [ ] Tous les `split` utilisent `-1`.
- [ ] Les montants sont des `BigDecimal` construits depuis des `String`.
- [ ] Aucun `getOrElse(0)` ne masque une erreur de parsing.
- [ ] Chaque rejet porte une raison, et `valides + rejets == total`.
- [ ] Le dédoublonnage se fait sur la clé métier (`id`).
- [ ] Un test par règle métier, et le test échoue bien quand on casse la règle.
- [ ] Le total est réconcilié avec un calcul indépendant.

## 16. Blogs restants

| # | Sujet | Statut |
|---|---|---|
| 01 | Bases Scala (avec tests et bonnes pratiques) | ✅ ce blog |
| 02 | **Environnement pro** : JDK, Scala SDK 2.12, IntelliJ, Maven aligné, build du jar, premier test ScalaTest, `spark-submit` | prochain |
| 03 | Collections et style fonctionnel | à venir |
| 04 | `Option`, `Either`, `Try` et gestion d'erreurs | à venir |
| 05 | Traits, génériques, type classes, `implicit` (2.12) | à venir |
| 06 | Scala avancé utile : `for`, `lazy val`, by-name, récursion terminale, `Iterator` | à venir |
| 07 | Tests niveau pro : ScalaTest, ScalaCheck, `SparkSession` locale | à venir |
| 08 | Scala x Spark côté langage : Dataset typé, closures, UDF | à venir |
| 09 | Bonnes pratiques : structure, lint, logging, config | à venir |
| 10 | Tech lead : modules Maven, shading, CI, revue de code | à venir |
| 11 | Capstone optionnel : mini-projet + live coding | optionnel |

## 17. Échauffement pour le blog 02

Avant le blog 02, relève sur ton cluster : version complète de Scala, version de Java, et note aussi la version de Maven de ton poste (`mvn -v`). Remplis l'onglet « Alignement environnement » du fichier `roadmap-scala-data-engineer.xlsx`.