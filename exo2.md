# Notre premier Use Case

Ça y est nous avons validé notre PoC de job Spark ! Félicitations, nous sommes maintenant prêts à répondre à notre première demande métier.

Tout le code devra être réalisé dans le dossier `fr.hymaia.exo2`

## Des ~~souris~~ villes et des hommes

Nous avons 2 bases de données représentant d'un côté une liste de clients et de l'autre un référentiel de code postal avec le nom de la ville associée.

Schéma de la table clients :
* name : type string, nom du client
* age : type int, age du client
* zip : type string, code postal du client

Schéma de la table villes :
* zip : type string, code postal d'une ville
* city : type string, nom de la ville

### Lire les fichiers

Les fichiers sont dans src/resources/exo2. Chargez-les dans une DataFrame en reprenant ce qui avait été fait dans votre PoC.

### Filtrer

Dans notre table de clients, nous souhaitons garder que les clients majeurs (age >= 18). Créez une fonction dédiée à ça.

### Joindre

Nous souhaitons connaitre le nom de la ville où habitent nos clients majeurs.

Exemple :

```
input clients :
name  , age, zip
Cussac, 27 , 75020

input villes :
zip  , city
75020, Paris

output :
name  , age, zip  , city
Cussac, 27 , 75020, Paris
```

Créez une fonction dédiée à ça.

### Écrire

Écrivez le résultat dans un fichier parquet nommé `output` dans le dossier `data/exo2/`.

## Les départements

Nous souhaiterions ajouter une colonne nommée `departement` à notre fichier output. Celui-ci se déduit depuis le code postal en récupérant les 2 premiers digits.

Exemple :
```
input : 75020
output : 75
```

Attention la Corse est un cas particulier :
```
input : <= 20190
output : 2A

input : > 20190
output : 2B
```

Créez une fonction dédiée à ça.

## Second job Spark

Nous allons créer un second job Spark. Comme ce job Spark correspond à la suite du même produit, son code ira dans le même projet. Cependant afin de voir venir la croissance du nombre de job Spark, il va peut-être falloir réorganiser nos fichiers pythons pour continuer de s'y retrouver avec plusieurs jobs Spark.

Mon conseil :

* Mettre à la racine du projet les fichiers qui créent les jobs Spark (`src/fr/hymaia/exo2/spark_clean_job.py`)
* Mettre dans des sous packages correspondant aux jobs spark les autres fichiers de code (si vous en avez) (`src/fr/hymaia/exo2/clean/`, `src/fr/hymaia/exo2/agregate/`)

Ce n'est pas forcément la meilleure solution, mais pour des projets de moyenne taille, d'expérience ça fait le job !

### Lire un fichier

Pour ce job Spark, notre donnée d'entrée est le résultat du premier job Spark. Comme nous allons bientôt écrire un second fichier, modifier le code du premier job Spark pour que notre premier fichier s'appelle désormais `clean`

### Calcul de la population par département

Nous souhaiterions savoir combien de nos clients vivent par département et trier le résultat du département le plus peuplé au moins peuplé. En cas d'égalité, c'est l'ordre alphabétique qui nous intéressera.

Exemple :
```
input :
name  , age, zip  , city    , departement
Cussac, 27 , 75020, Paris   , 75
Titi  , 35 , 20200, Bastia  , 2B
Tata  , 40 , 75008, Paris   , 75
Toto  , 45 , 33120, Arcahcon, 33


output :
departement, nb_people
75         , 2
2B         , 1
33         , 1
```

Créez une fonction dédiée à ça.

### Écrire

Écrivez le résultat dans un fichier csv nommé `aggregate` dans le dossier `data/exo2/`. Faites en sorte qu'il n'y ait qu'un seul fichier d'écrit dans le dossier.


[Précédent](exo1.md) <- -> [Suivant](exo3.md)